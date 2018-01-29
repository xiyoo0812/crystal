package crystal

import (
	"fmt"
	"net"
	"sync"
	"context"
	"net/http"
	"websocket"
)

var netIdentifier *AtomicInt64
func init() {
	netIdentifier = NewAtomicInt64(1000)
}

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

func (s *ConnCtrl) CloseConn(id int64){
	s.conns.Delete(id)
	s.wg.Done()
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
		if err := c.Write(msg); err != nil {
			Errorf("broadcast error %v, conn id %d", err, k.(int64))
			return false
		}
		return true
	})
}

// Unicast unicasts message to a specified conn.
func (s *ConnCtrl) Unicast(id int64, msg *Message) error {
	v, ok := s.conns.Load(id)
	if ok {
		return v.(*NetConn).Write(msg)
	}
	return fmt.Errorf("conn id %d not found", id)
}

// Conn returns a ConnCtrl connection with specified ID.
func (s *ConnCtrl) Conn(id int64) (*NetConn, bool) {
	v, ok := s.conns.Load(id)
	if ok {
		return v.(*NetConn), ok
	}
	return nil, ok
}

func (s *ConnCtrl) Listen(address string) (bool, error) {
	if s.listener != nil {
		return false, ErrAreadyListen
	}
	l, err := net.Listen("tcp", address)
	if err != nil {
		return false, err
	}
	s.listener = l
	Infof("start, net %s addr %s\n", l.Addr().Network(), l.Addr().String())
	return true, nil
}

func (s *ConnCtrl) Websocket(address, url string) (bool, error) {
	if s.listener != nil {
		return false, ErrAreadyListen
	}
	wsHandler := func(rawConn *websocket.Conn) {
		netid := netIdentifier.GetAndIncrement()
		sc := NewNetConn(netid, s, rawConn)
		s.conns.Store(netid, sc)
		s.wg.Add(1) // this will be Done() in TCPConn.Close()
		sc.Start()
	}
	http.Handle(url, websocket.Handler(wsHandler))
	err := http.ListenAndServe(address, nil)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *ConnCtrl) Accept() (bool, error) {
	for {
		rawConn, err := s.listener.Accept()
		if err != nil {
			Errorf("accept error %v\n", err)
			return false, err
		}
		// how many connections do we have ?
		sz := s.ConnsSize()
		if sz >= MaxConnections {
			Warnf("max connections size %d, refuse\n", sz)
			rawConn.Close()
			continue
		}
		netid := netIdentifier.GetAndIncrement()
		sc := NewNetConn(netid, s, rawConn)
		s.conns.Store(netid, sc)

		Debugf("accepted client %s, id %d, total %d\n", sc.Name(), netid, s.ConnsSize())

		s.wg.Add(1) // this will be Done() in TCPConn.Close()
		go sc.Start()
	} // for loop
	return true, nil
}

func (s *ConnCtrl) Connect(address string) (bool, error) {
	if s.listener != nil {
		return false, ErrAreadyListen
	}
	rawConn, err := net.Dial("tcp", address)
	if err != nil {
		return false, err
	}

	netid := netIdentifier.GetAndIncrement()
		sc := NewNetConn(netid, s, rawConn)
	s.conns.Store(netid, sc)

	Debugf("connect server %s, id %d, total %d\n", sc.Name(), netid, s.ConnsSize())

	s.wg.Add(1) // this will be Done() in TCPConn.Close()
	go sc.Start()
	return true, nil
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

