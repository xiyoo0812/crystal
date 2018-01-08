package crystal

import (
	"fmt"
	"net"
	"sync"
	"context"
)

var netIdentifier *AtomicInt64
func init() {
	netIdentifier = NewAtomicInt64(1000)
}

// TCPCtrl  is a TCPCtrl to serve TCP requests.
type TCPCtrl struct {
	ntype		int16
	ctx    		context.Context
	cancel 		context.CancelFunc
	conns  		*sync.Map
	wg     		*sync.WaitGroup
	mu     		sync.Mutex 		// guards following
	listener   	net.Listener
	bufferSize 	uint32  // size of buffered channel
}

// NewTCPCtrl returns a new TCPCtrl which has not started
// to serve requests yet.
func NewTCPCtrl(ctx context.Context, connTtype	int16, bufSize uint32) *TCPCtrl {
	if bufSize <= 0 {
		bufSize = BufferSize256
	}
	s := &TCPCtrl{
		ntype		: connTtype,
		conns		: &sync.Map{},
		wg			: &sync.WaitGroup{},
		bufferSize 	: bufSize,
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	return s
}

func (s *TCPCtrl) CloseConn(id int64){
	s.conns.Delete(id)
	s.wg.Done()
}

// ConnsSize returns connections size.
func (s *TCPCtrl) ConnsSize() int {
	var sz int
	s.conns.Range(func(k, v interface{}) bool {
		sz++
		return true
	})
	return sz
}

// Broadcast broadcasts message to all TCPCtrl connections managed.
func (s *TCPCtrl) Broadcast(msg *Message) {
	s.conns.Range(func(k, v interface{}) bool {
		c := v.(*TcpConn)
		if err := c.Write(msg); err != nil {
			Errorf("broadcast error %v, conn id %d", err, k.(int64))
			return false
		}
		return true
	})
}

// Unicast unicasts message to a specified conn.
func (s *TCPCtrl) Unicast(id int64, msg *Message) error {
	v, ok := s.conns.Load(id)
	if ok {
		return v.(*TcpConn).Write(msg)
	}
	return fmt.Errorf("conn id %d not found", id)
}

// Conn returns a TCPCtrl connection with specified ID.
func (s *TCPCtrl) Conn(id int64) (*TcpConn, bool) {
	v, ok := s.conns.Load(id)
	if ok {
		return v.(*TcpConn), ok
	}
	return nil, ok
}

func (s *TCPCtrl) Listen(address string) (bool, error) {
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

func (s *TCPCtrl) Accept() (bool, error) {
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
		sc := NewTcpConn(netid, s, rawConn)
		s.conns.Store(netid, sc)

		Debugf("accepted client %s, id %d, total %d\n", sc.Name(), netid, s.ConnsSize())

		s.wg.Add(1) // this will be Done() in TCPConn.Close()
		go sc.Start()
	} // for loop
	return true, nil
}

func (s *TCPCtrl) Connect(address string) (bool, error) {
	if s.listener != nil {
		return false, ErrAreadyListen
	}
	rawConn, err := net.Dial("tcp", address)
	if err != nil {
		return false, err
	}

	netid := netIdentifier.GetAndIncrement()
	sc := NewTcpConn(netid, s, rawConn)
	s.conns.Store(netid, sc)

	Debugf("connect server %s, id %d, total %d\n", sc.Name(), netid, s.ConnsSize())

	s.wg.Add(1) // this will be Done() in TCPConn.Close()
	go sc.Start()
	return true, nil
}

// Stop gracefully closes the TCPCtrl, it blocked until all connections
// are closed and all go-routines are exited.
func (s *TCPCtrl) Stop() {
	// immediately stop accepting new clients
	if s.listener != nil {
		s.listener.Close()
		s.listener = nil
		Debugf("stop accepting at address %s\n", s.listener.Addr().String())
	}
	// close all connections
	conns := map[int64]*TcpConn{}
	s.conns.Range(func(k, v interface{}) bool {
		i := k.(int64)
		c := v.(*TcpConn)
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

	Debugf("TCPCtrl stopped gracefully, bye.")
}

