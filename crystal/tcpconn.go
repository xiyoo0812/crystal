package crystal

import (
	"net"
	"sync"
	"time"
	"context"
)

// WriteCloser is the interface that groups Write and Close methods.
type WriteCloser interface {
	Write(*Message) error
	Close()
}

// TcpConn represents a connection to a TCP server, it implments Conn.
type TcpConn struct {
	netid   int64
	heart   int64
	name    string
	once    *sync.Once
	wg   	*sync.WaitGroup
	mu		sync.Mutex
	ctrl	*TCPCtrl
	rawConn net.Conn
	sendCh  chan []byte
	handCh 	chan *Message
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewServerConn returns a new server connection which has not started to
// serve requests yet.
func NewTcpConn(id int64, ct *TCPCtrl, c net.Conn) *TcpConn {
	sc := &TcpConn{
		netid:    	id,
		rawConn:   	c,
		ctrl:		ct,
		once:      	&sync.Once{},
		wg:        	&sync.WaitGroup{},
		sendCh:    	make(chan []byte, ct.bufferSize),
		handCh: 	make(chan *Message, MaxMessageQueue),
		heart:     	time.Now().UnixNano(),
	}
	sc.ctx, sc.cancel = context.WithCancel(ct.ctx)
	sc.name = c.RemoteAddr().String()
	return sc
}

// NetID returns net ID of server connection.
func (tc *TcpConn) NetID() int64 {
	return tc.netid
}

// Name returns the name of server connection.
func (tc *TcpConn) Name() string {
	return tc.name
}

// SetHeartBeat sets the heart beats of server connection.
func (tc *TcpConn) SetHeartBeat(heart int64) {
	tc.heart = heart
}

// HeartBeat returns the heart beats of server connection.
func (tc *TcpConn) HeartBeat() int64 {
	return tc.heart
}

// RemoteAddr returns the remote address of server connection.
func (tc *TcpConn) RemoteAddr() net.Addr {
	return tc.rawConn.RemoteAddr()
}

// LocalAddr returns the local address of server connection.
func (tc *TcpConn) LocalAddr() net.Addr {
	return tc.rawConn.LocalAddr()
}

// Start starts the server connection, creating go-routines for reading, writing and handlng.
func (tc *TcpConn) Start() {
	Infof("conn start, <%v -> %v>\n", tc.rawConn.LocalAddr(), tc.rawConn.RemoteAddr())
	if ConnectFuncImpl != nil {
		ConnectFuncImpl(tc)
	}
	loopers := []func(*TcpConn, *sync.WaitGroup){readLoop, writeLoop, handleLoop}
	for _, l := range loopers {
		looper := l
		tc.wg.Add(1)
		go looper(tc, tc.wg)
	}
}

// Close gracefully closes the server connection. It blocked until all sub go-routines are completed and returned.
func (tc *TcpConn) Close() {
	tc.once.Do(func() {
		Infof("conn close gracefully, <%v -> %v>\n", tc.rawConn.LocalAddr(), tc.rawConn.RemoteAddr())
		// callback on close
		if CloseFuncImpl != nil {
			CloseFuncImpl(tc)
		}
		// close net.Conn, any blocked read or write operation will be unblocked and return errors.
		if nc, ok := tc.rawConn.(*net.TCPConn); ok {
			// avoid time-wait state
			nc.SetLinger(0)
		}
		tc.rawConn.Close()
		// cancel readLoop, writeLoop and handleLoop go-routines.
		tc.cancel()
		// wait until all go-routines exited.
		tc.wg.Wait()
		// close all channels and block until all go-routines exited.
		close(tc.sendCh)
		close(tc.handCh)
		// close connection from parent
		tc.ctrl.CloseConn(tc.netid)
	})
}

// Write writes a message to the client.
func (sc *TcpConn) Write(message *Message) error {
	//return asyncWrite(sc, message)
	pkt, err := EncodeMessage(message)
	if err != nil {
		Errorf("TcpConn Write error %v\n", err)
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			err = ErrServerClosed
		}
	}()
	select {
	case sc.sendCh <- pkt:
		err = nil
	default:
		err = ErrWouldBlock
	}
	return err
}

/* readLoop() blocking read from connection, deserialize bytes into message, then find corresponding handler, put it into channel */
func readLoop(sc *TcpConn, wg *sync.WaitGroup) {
	defer func() {
		if p := recover(); p != nil {
			Errorf("readLoop panics: %v\n", p)
		}
		wg.Done()
		Debugln("readLoop go-routine exited")
		sc.Close()
	}()
	for {
		select {
		case <-sc.ctx.Done(): // connection closed
			return
		default:
			msg, err := DecodeMessag(sc.rawConn)
			if err != nil {
				Errorf("error decoding message %v\n", err)
				return
			}
			sc.SetHeartBeat(time.Now().UnixNano())
			sc.handCh <- msg
		}
	}
}

/* writeLoop() receive message from channel, serialize it into bytes,then blocking write into connection */
func writeLoop(sc *TcpConn, wg *sync.WaitGroup) {
	defer func() {
		if p := recover(); p != nil {
			Errorf("writeLoop panics: %v\n", p)
		}
		wg.Done()
		Debugln("writeLoop go-routine exited")
		sc.Close()
	}()
	for {
		var pkt []byte
		select {
		case <-sc.ctx.Done(): // connection closed
			return
		case pkt = <-sc.sendCh:
			if pkt != nil {
				if _, err := sc.rawConn.Write(pkt); err != nil {
					Errorf("error writing data %v\n", err)
					return
				}
			}
		}
	}
}

// handleLoop() - put handler or timeout callback into worker go-routines
func handleLoop(sc *TcpConn, wg *sync.WaitGroup) {
	defer func() {
		if p := recover(); p != nil {
			Errorf("handleLoop panics: %v\n", p)
		}
		wg.Done()
		Debugln("handleLoop go-routine exited")
		sc.Close()
	}()
	for {
		select {
		case <-sc.ctx.Done(): // connection closed
			return
		case msg := <-sc.handCh:
			handler := GetMeaageHandler(msg.MsgId)
			if handler == nil {
				if MessageFuncImpl != nil {
					MessageFuncImpl(msg, sc)
				} else {
					Warnf("no handler or onMessage() found for message %d\n", msg.MsgId)
				}
				continue
			} else {
				err := WorkerPoolInstance().Put(sc.netid, func() {
					handler(msg, sc)
				})
				if err != nil {
					Errorln(err)
				}
			}
		}
	}
}
