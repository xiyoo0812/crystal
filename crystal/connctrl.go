package crystal

import (
	"net"
	"sync"
	"context"
	"net/http"
	"websocket"
	"time"
)

// ConnCtrl  is a ConnCtrl to serve TCP requests.
type ConnCtrl struct {
	ntype		uint16
	ctx    		context.Context
	cancel 		context.CancelFunc
	conns  		*sync.Map
	wg     		*sync.WaitGroup
	mu     		sync.Mutex 		// guards following
	listener   	net.Listener
	bufferSize 	uint32  // size of buffered channel

	ConnectFuncImpl ConnectFunc
	MessageFuncImpl MessageFunc
	CloseFuncImpl 	CloseFunc
	TaskFuncImpl 	TaskFunc
}

// NewConnCtrl returns a new ConnCtrl which has not started
// to serve requests yet.
func NewConnCtrl(ctx context.Context, connTtype	uint16, bufSize uint32) *ConnCtrl {
	if bufSize <= 0 {
		bufSize = BufferSize256
	}
	s := &ConnCtrl{
		ntype		: connTtype,
		conns		: &sync.Map{},
		wg			: &sync.WaitGroup{},
		bufferSize 	: bufSize,
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	return s
}

// ConnectFunc sets a callback
func (s *ConnCtrl) SetConnectFunc(connect ConnectFunc) {
	s.ConnectFuncImpl = connect
}

// MessageFunc sets a callback
func (s *ConnCtrl) SetMessageFunc(message MessageFunc) {
	s.MessageFuncImpl = message
}

// MessageFunc sets a callback
func (s *ConnCtrl) SetTaskFunc(task TaskFunc) {
	s.TaskFuncImpl = task
}

// CloseFunc sets a callback
func (s *ConnCtrl) SetCloseFunc(close CloseFunc) {
	s.CloseFuncImpl = close
}

func (s *ConnCtrl) CloseConn(id uint64){
	s.conns.Delete(id)
	s.wg.Done()
}

func (s *ConnCtrl) CheckConn(tick int64) []uint64 {
	var lost = []uint64{}
	nowtick := time.Now().Unix()
	s.conns.Range(func(k, v interface{}) bool {
		c := v.(*NetConn)
		if nowtick - c.heart >= tick {
			lost = append(lost, c.netid)
		}
		return true
	})
	return lost
}

// ConnsSize returns connections size.
func (s *ConnCtrl) ConnsSize() int {
	var sz int
	s.conns.Range(func(k, v interface{}) bool {
		sz++
		return true
	})
	return sz
}

// Broadcast broadcasts message to all ConnCtrl connections managed.
func (s *ConnCtrl) Broadcast(msg *Message) {
	s.conns.Range(func(k, v interface{}) bool {
		c := v.(*NetConn)
		return c.Write(msg)
	})
}

// Unicast unicasts message to a specified conn.
func (s *ConnCtrl) Unicast(id uint64, msg *Message) bool {
	v, ok := s.conns.Load(id)
	if ok {
		return v.(*NetConn).Write(msg)
	}
	Warnf("conn id %d not found", id)
	return false
}

// Conn returns a ConnCtrl connection with specified ID.
func (s *ConnCtrl) Conn(id uint64) (*NetConn, bool) {
	v, ok := s.conns.Load(id)
	if ok {
		return v.(*NetConn), ok
	}
	return nil, ok
}

func (s *ConnCtrl) Listen(address string) bool {
	if s.listener != nil {
		Errorln("listen error aready listen\n")
		return false
	}
	l, err := net.Listen("tcp", address)
	if err != nil {
		Errorln("Listen Failed:", err)
		return false
	}
	s.listener = l
	Infof("start, net %s addr %s\n", l.Addr().Network(), l.Addr().String())
	return true
}

func (s *ConnCtrl) Websocket(address, url string) bool {
	if s.listener != nil {
		Errorln("websocket error aready listen\n")
		return false
	}
	wsHandler := func(w http.ResponseWriter, r *http.Request) {
		var upgrader = websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin : func(r *http.Request) bool {
				// allow all connections by default
				return true
			},
		}
		if rawConn, err := upgrader.Upgrade(w, r, nil); err != nil{
			Errorln("Websocket Upgrade Failed:", err)
			return
		} else {
			netid := NewGuid(1000, 1)
			sc := NewWebConn(netid, s, rawConn)
			s.conns.Store(netid, sc)
			s.wg.Add(1) // this will be Done() in TCPConn.Close()
			sc.Start()
		}
	}
	http.HandleFunc(url, wsHandler)
	err := http.ListenAndServe(address, nil)
	if err != nil {
		Errorln("Websocket ListenAndServe Failed:", err)
		return false
	}
	return true
}

func (s *ConnCtrl) Accept() bool {
	for {
		rawConn, err := s.listener.Accept()
		if err != nil {
			Errorf("accept error %v\n", err)
			return false
		}
		// how many connections do we have ?
		sz := s.ConnsSize()
		if sz >= MaxConnections {
			Warnf("max connections size %d, refuse\n", sz)
			rawConn.Close()
			continue
		}
		netid := NewGuid(1000, 2)
		sc := NewNetConn(netid, s, rawConn)
		s.conns.Store(netid, sc)

		Debugf("accepted client %s, id %d, total %d\n", sc.Name(), netid, s.ConnsSize())

		s.wg.Add(1) // this will be Done() in TCPConn.Close()
		go sc.Start()
	} // for loop
	return true
}

func (s *ConnCtrl) Connect(address string) uint64 {
	if s.listener != nil {
		Errorln("connect error aready listen\n")
		return 0
	}
	rawConn, err := net.Dial("tcp", address)
	if err != nil {
		Errorf("connect error %v\n", err)
		return 0
	}

	netid := NewGuid(1000, 3)
		sc := NewNetConn(netid, s, rawConn)
	s.conns.Store(netid, sc)

	Debugf("connect server %s, id %d, total %d\n", sc.Name(), netid, s.ConnsSize())

	s.wg.Add(1) // this will be Done() in TCPConn.Close()
	go sc.Start()
	return netid
}

// Stop gracefully closes the ConnCtrl, it blocked until all connections
// are closed and all go-routines are exited.
func (s *ConnCtrl) Stop() {
	// immediately stop accepting new clients
	if s.listener != nil {
		s.listener.Close()
		s.listener = nil
		Debugf("stop accepting at address %s\n", s.listener.Addr().String())
	}
	// close all connections
	conns := map[int64]*NetConn{}
	s.conns.Range(func(k, v interface{}) bool {
		i := k.(int64)
		c := v.(*NetConn)
		conns[i] = c
		return true
	})
	// let GC do the cleanings
	for _, c := range conns {
		c.Close()
		Debugf("close client %s\n", c.Name())
	}
	s.conns = nil

	s.cancel()
	s.wg.Wait()

	Debugf("ConnCtrl stopped gracefully, bye.")
}

