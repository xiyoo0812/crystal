package crystal

import (
	"net"
	"sync"
	"time"
	"context"
	"websocket"
)

// NetConn represents a connection to a TCP server, it implments Conn.
type NetConn struct {
	netid   int64
	heart   int64
	name    string
	encry 	*Encryptor
	once    *sync.Once
	wg   	*sync.WaitGroup
	send	*sync.WaitGroup
	mu		sync.Mutex
	ctrl	*ConnCtrl
	rawConn net.Conn
	webConn *websocket.Conn
	taskCh	chan context.Context
	sendCh  chan []byte
	handCh 	chan *Message
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewTcpConn returns a new server connection which has not started to
// serve requests yet.
func NewNetConn(id int64, ct *ConnCtrl, c net.Conn) *NetConn {
	sc := &NetConn{
		netid:    	id,
		rawConn:   	c,
		ctrl:		ct,
		once:      	&sync.Once{},
		wg:        	&sync.WaitGroup{},
		send:      	&sync.WaitGroup{},
		sendCh:    	make(chan []byte, ct.bufferSize),
		handCh: 	make(chan *Message, MaxMessageQueue),
		taskCh: 	make(chan context.Context, MaxTaskQueue),
		heart:     	time.Now().UnixNano(),
		encry:		new(Encryptor),
	}
	sc.ctx, sc.cancel = context.WithCancel(ct.ctx)
	sc.name = c.RemoteAddr().String()
	return sc
}

func NewWebConn(id int64, ct *ConnCtrl, c *websocket.Conn) *NetConn {
	sc := &NetConn{
		netid:    	id,
		webConn:   	c,
		ctrl:		ct,
		once:      	&sync.Once{},
		wg:        	&sync.WaitGroup{},
		send:      	&sync.WaitGroup{},
		sendCh:    	make(chan []byte, ct.bufferSize),
		handCh: 	make(chan *Message, MaxMessageQueue),
		taskCh: 	make(chan context.Context, MaxTaskQueue),
		heart:     	time.Now().UnixNano(),
		encry:		new(Encryptor),
	}
	sc.ctx, sc.cancel = context.WithCancel(ct.ctx)
	sc.name = c.RemoteAddr().String()
	return sc
}

// NetID returns net ID of server connection.
func (tc *NetConn) NetID() int64 {
	return tc.netid
}

// Name returns the name of server connection.
func (tc *NetConn) Name() string {
	return tc.name
}

// SetHeartBeat sets the heart beats of server connection.
func (tc *NetConn) SetHeartBeat(heart int64) {
	tc.heart = heart
}

// HeartBeat returns the heart beats of server connection.
func (tc *NetConn) HeartBeat() int64 {
	return tc.heart
}

// returns Type of server connection.
func (tc *NetConn) Type() uint16 {
	return tc.ctrl.ntype
}

// RemoteAddr returns the remote address of server connection.
func (tc *NetConn) RemoteAddr() net.Addr {
	if tc.rawConn != nil {
		return tc.rawConn.RemoteAddr()
	}
	return tc.webConn.RemoteAddr()
}

// LocalAddr returns the local address of server connection.
func (tc *NetConn) LocalAddr() net.Addr {
	if tc.rawConn != nil {
		return tc.rawConn.LocalAddr()
	}
	return tc.webConn.LocalAddr()
}

// Start starts the server connection, creating go-routines for reading, writing and handlng.
func (tc *NetConn) Start() {
	Infof("conn start, <%v -> %v>\n", tc.LocalAddr(), tc.RemoteAddr())
	tc.wg.Add(1)
	go readLoop(tc, tc.wg)
	tc.wg.Add(1)
	go writeLoop(tc, tc.wg)
	tc.wg.Add(1)
	handleLoop(tc, tc.wg)
}

// Close gracefully closes the server connection. It blocked until all sub go-routines are completed and returned.
func (tc *NetConn) Close() {
	tc.once.Do(func() {
		// close net.Conn, any blocked read or write operation will be unblocked and return errors.
		if tc.rawConn != nil {
			if nc, ok := tc.rawConn.(*net.TCPConn); ok {
				// avoid time-wait state
				nc.SetLinger(0)
			}
			tc.rawConn.Close()
		}
		if tc.webConn != nil {
			tc.webConn.Close()
		}
		// cancel readLoop, writeLoop and handleLoop go-routines.
		tc.cancel()
		// wait until all go-routines exited.
		tc.wg.Wait()
		// close all channels and block until all go-routines exited.
		close(tc.sendCh)
		close(tc.handCh)
		close(tc.taskCh)
		// close connection from parent
		tc.ctrl.CloseConn(tc.netid)
	})
}

// Task put a task to the client.
func (sc *NetConn) Task(ctx context.Context) {
	sc.taskCh <- ctx
}

// Write writes a message to the client.
func (sc *NetConn) EncryptWrite(message *Message) bool {
	return sc.Write(sc.encry.EncodeMessage(message))
}

// Write writes a message to the client.
func (sc *NetConn) Write(message *Message) bool {
	pkt := EncodeMessage(message)
	if pkt == nil {
		Warnf("NetConn Write Warning : encode message error\n")
		return false
	}
	defer func() {
		if p := recover(); p != nil {}
	}()
	select {
	case sc.sendCh <- pkt:
		sc.send.Add(1)
		return true
	default:
		Errorf("NetConn Write Block\n")
	}
	return false
}

/* readLoop() blocking read from connection, deserialize bytes into message, then find corresponding handler, put it into channel */
func readLoop(sc *NetConn, wg *sync.WaitGroup) {
	defer func() {
		if p := recover(); p != nil {
			Errorf("readLoop panics: %v\n", p)
		}
		wg.Done()
		sc.Close()
	}()
	for {
		select {
		case <-sc.ctx.Done(): // connection closed
			return
		default:
			if sc.rawConn != nil {
				if msg := DecodeMessag(sc); msg == nil {
					return
				} else {
					sc.SetHeartBeat(time.Now().UnixNano())
					sc.handCh <- msg
				}
			} else {
				if msg := DecodeWebMessag(sc); msg == nil {
					return
				} else {
					sc.SetHeartBeat(time.Now().UnixNano())
					sc.handCh <- msg
				}
			}
		}
	}
}

/* writeLoop() receive message from channel, serialize it into bytes,then blocking write into connection */
func writeLoop(sc *NetConn, wg *sync.WaitGroup) {
	defer func() {
		if p := recover(); p != nil {
			Errorf("writeLoop panics: %v\n", p)
		}
		wg.Done()
		sc.Close()
	}()
	for {
		var pkt []byte
		select {
		case pkt = <-sc.sendCh:
			if pkt != nil {
				if sc.rawConn != nil {
					if _, err := sc.rawConn.Write(pkt); err != nil {
						Errorf("error writing data %v\n", err)
						return
					}
				} else {
					if err := sc.webConn.WriteMessage(websocket.BinaryMessage, pkt); err != nil {
						Errorf("error web writing data %v\n", err)
						return
					}
				}
				sc.send.Done()
			}
		case <-sc.ctx.Done(): // connection closed
			//wait send over
			sc.send.Wait()
			return
		}
	}
}

// handleLoop() - put handler or timeout callback into worker go-routines
func handleLoop(sc *NetConn, wg *sync.WaitGroup) {
	defer func() {
		if p := recover(); p != nil {
			Errorf("handleLoop panics: %v\n", p)
		}
		// callback on close
		if CloseFuncImpl != nil {
			CloseFuncImpl(sc)
		}
		sc.Close()
		wg.Done()
	}()
	//处理协程返回
	if ConnectFuncImpl != nil {
		ConnectFuncImpl(sc)
	}
	for {
		select {
		case <-sc.ctx.Done(): // connection closed
			return
		case ctx := <-sc.taskCh:
			if TaskFuncImpl != nil {
				TaskFuncImpl(ctx, sc)
			} else {
				Warnln("no handler for task %d\n")
			}
		case msg := <-sc.handCh:
			handler := GetMeaageHandler(msg.MsgId)
			if handler == nil {
				if MessageFuncImpl != nil {
					MessageFuncImpl(msg, sc)
				} else {
					Warnf("no handler for message %d\n", msg.MsgId)
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
