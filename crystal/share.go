package crystal

import (
	"os"
	"fmt"
	"unsafe"
	"errors"
	"runtime"
	"reflect"
	"hash/fnv"
	"context"
)

// Error codes returned by failures dealing with server or connection.
var (
	ErrParameter     = errors.New("parameter error")
	ErrNilKey        = errors.New("nil key")
	ErrNilValue      = errors.New("nil value")
	ErrWouldBlock    = errors.New("would block")
	ErrNotHashable   = errors.New("not hashable")
	ErrNilData       = errors.New("nil data")
	ErrBadData       = errors.New("more than 8M data")
	ErrServerClosed  = errors.New("server has been closed")
	ErrAreadyListen  = errors.New("server aready Listened")
	ErrMsgLength  	 = errors.New("message length error")

	ConnectFuncImpl ConnectFunc
	MessageFuncImpl MessageFunc
	CloseFuncImpl 	CloseFunc
	TaskFuncImpl 	TaskFunc
)

// definitions about some constants.
const (
	MaxConnections    = 3000
	BufferSize128     = 128
	BufferSize256     = 256
	BufferSize512     = 512
	BufferSize1024    = 1024
	MaxMessageQueue	 = 16
	MaxTaskQueue		 = 32
	defaultWorkersNum = 20
)

type ConnectFunc func(*NetConn)
type MessageFunc func(*Message, *NetConn)
type TaskFunc func(context.Context, *NetConn)
type CloseFunc func(*NetConn)

type workerFunc func()

// ErrUndefined for undefined message type.
type ErrUndefined int32

func (e ErrUndefined) Error() string {
	return fmt.Sprintf("undefined message type %d", e)
}

// ConnectFunc sets a callback
func SetConnectFunc(connect ConnectFunc) {
	ConnectFuncImpl = connect
}

// MessageFunc sets a callback
func SetMessageFunc(message MessageFunc) {
	MessageFuncImpl = message
}

// MessageFunc sets a callback
func SetTaskFunc(task TaskFunc) {
	TaskFuncImpl = task
}

// CloseFunc sets a callback
func SetCloseFunc(close CloseFunc) {
	CloseFuncImpl = close
}

// Hashable is a interface for hashable object.
type Hashable interface {
	HashCode() int32
}

const intSize = unsafe.Sizeof(1)

func HashCode(k interface{}) uint32 {
	var code uint32
	h := fnv.New32a()
	switch v := k.(type) {
	case bool:
		h.Write((*((*[1]byte)(unsafe.Pointer(&v))))[:])
		code = h.Sum32()
	case int:
		h.Write((*((*[intSize]byte)(unsafe.Pointer(&v))))[:])
		code = h.Sum32()
	case int8:
		h.Write((*((*[1]byte)(unsafe.Pointer(&v))))[:])
		code = h.Sum32()
	case int16:
		h.Write((*((*[2]byte)(unsafe.Pointer(&v))))[:])
		code = h.Sum32()
	case int32:
		h.Write((*((*[4]byte)(unsafe.Pointer(&v))))[:])
		code = h.Sum32()
	case int64:
		h.Write((*((*[8]byte)(unsafe.Pointer(&v))))[:])
		code = h.Sum32()
	case uint:
		h.Write((*((*[intSize]byte)(unsafe.Pointer(&v))))[:])
		code = h.Sum32()
	case uint8:
		h.Write((*((*[1]byte)(unsafe.Pointer(&v))))[:])
		code = h.Sum32()
	case uint16:
		h.Write((*((*[2]byte)(unsafe.Pointer(&v))))[:])
		code = h.Sum32()
	case uint32:
		h.Write((*((*[4]byte)(unsafe.Pointer(&v))))[:])
		code = h.Sum32()
	case uint64:
		h.Write((*((*[8]byte)(unsafe.Pointer(&v))))[:])
		code = h.Sum32()
	case string:
		h.Write([]byte(v))
		code = h.Sum32()
	case Hashable:
		c := v.HashCode()
		h.Write((*((*[4]byte)(unsafe.Pointer(&c))))[:])
		code = h.Sum32()
	default:
		panic("key not hashable")
	}
	return code
}

func isNil(v interface{}) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	kd := rv.Type().Kind()
	switch kd {
	case reflect.Ptr, reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

func printStack() {
	var buf [4096]byte
	n := runtime.Stack(buf[:], false)
	os.Stderr.Write(buf[:n])
}
