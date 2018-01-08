package crystal

import (
	"bytes"
	"unsafe"
	"encoding/binary"
	"fmt"
	"net"
	"io"
)

// MessageHeadLength is the length of type header.
const MessageHeadLength = unsafe.Sizeof(uint16(0)) * 3 + unsafe.Sizeof(uint64(0)) + unsafe.Sizeof(uint8(0))

// HandlerFunc serves as an adapter to allow the use of ordinary functions as handlers.
type HandlerFunc func(*Message, *TcpConn)

var (
	msgbuf *bytes.Buffer
	// messageRegistry is the registry of all
	// message-related unmarshal and handle functions.
	messageRegistry map[uint16]HandlerFunc
)

func init() {
	msgbuf = new(bytes.Buffer)
	messageRegistry = map[uint16]HandlerFunc{}
}

func RegisterMsgHander(msgId uint16, handler HandlerFunc) {
	if _, ok := messageRegistry[msgId]; !ok {
		messageRegistry[msgId] = handler
		panic(fmt.Sprintf("trying to register message %d twice", msgId))
	} else {
		Warnf("trying to register message %d twice", msgId)
	}
}

// GetHandlerFunc returns the corresponding handler function for msgType.
func GetMeaageHandler(msgId uint16) HandlerFunc {
	if handler, ok := messageRegistry[msgId]; ok{
		return handler
	}
	return nil
}

// Decode decodes the bytes data into Message
func DecodeMessag(raw net.Conn) (*Message, error) {
	// read header data
	headBytes := make([]byte, MessageHeadLength)
	_, err := io.ReadFull(raw, headBytes)
	if err != nil {
		return nil, err
	}
	var message Message
	headBuf := bytes.NewReader(headBytes)
	if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgLen); err != nil {
		return nil, err
	}
	if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgFlag); err != nil {
		return nil, err
	}
	if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgId); err != nil {
		return nil, err
	}
	if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgCode); err != nil {
		return nil, err
	}
	if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgCtx); err != nil {
		return nil, err
	}
	// read application data
	msgBytes := make([]byte, message.BodyLen())
	_, err = io.ReadFull(raw, msgBytes)
	if err != nil {
		return nil, err
	}
	message.Msgbuf.Write(msgBytes)
	return &message, nil
}

func EncodeMessage(m *Message) ([]byte, error) {
	var msgbuf bytes.Buffer
	m.MsgLen = uint16(m.Msgbuf.Len()) + uint16(MessageHeadLength)
	if err := binary.Write(&msgbuf, binary.LittleEndian, m.MsgLen); err != nil {
		return nil, err
	}
	if err := binary.Write(&msgbuf, binary.LittleEndian, m.MsgFlag); err != nil {
		return nil, err
	}
	if err := binary.Write(&msgbuf, binary.LittleEndian, m.MsgId); err != nil {
		return nil, err
	}
	if err := binary.Write(&msgbuf, binary.LittleEndian, m.MsgCode); err != nil {
		return nil, err
	}
	if err := binary.Write(&msgbuf, binary.LittleEndian, m.MsgCtx); err != nil {
		return nil, err
	}
	if err := binary.Write(&msgbuf, binary.LittleEndian, m.Msgbuf.Bytes()); err != nil {
		return nil, err
	}
	return msgbuf.Bytes(), nil
}

// Message represents the structured data that can be handled.
type Message struct {
	MsgLen 	uint16
	MsgFlag	uint16
	MsgId 	uint16
	MsgCode	uint8
	MsgCtx  uint64
	Msgbuf  bytes.Buffer
}

//消息体size
func (m Message) BodyLen() uint16 {
	return m.MsgLen - uint16(MessageHeadLength)
}

//消息size
func (m Message) Size() uint16 {
	return uint16(m.Msgbuf.Len()) + uint16(MessageHeadLength)
}

//生产一个消息
func (m *Message) General(id uint16, ctx uint64, data []byte) {
	m.MsgId = id
	m.MsgCtx = ctx
	if data != nil {
		m.Msgbuf.Write(data)
	}
}

//写入内置类型，bool/int*/[]byte等
func (m *Message) Write(data interface{}) error {
	return binary.Write(&m.Msgbuf, binary.LittleEndian, data)
}

//读取内置类型，bool/int*/[]byte等, 需传入指针
func (m *Message) Read(data interface{}) error {
	return binary.Read(&m.Msgbuf, binary.LittleEndian, data)
}

//WriteString
func (m *Message) WriteString(s string) error {
	if err := binary.Write(&m.Msgbuf, binary.LittleEndian, uint16(len(s))); err == nil {
		_, err = m.Msgbuf.WriteString(s)
		return err
	} else {
		return err
	}
}

//ReadString
func (m *Message) ReadString() (string, error) {
	var length uint16
	if err := binary.Read(&m.Msgbuf, binary.LittleEndian, &length); err == nil {
		sbytes := make([]byte, length)
		_, err := m.Msgbuf.Read(sbytes)
		return string(sbytes), err
	} else {
		return string(0), err
	}
}