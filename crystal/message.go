package crystal

import (
	"bytes"
	"unsafe"
	"encoding/binary"
	"fmt"
	"io"
)

// MessageHeadLength is the length of type header.
const (
	//消息标志位
	MsgFlagEncr	uint8	= 1		//加密
	MsgFlagComp	uint8	= 2		//压缩
	MsgFlagBoard	uint8	= 4		//广播
	MsgFlagGroup	uint8	= 8		//指定组发
	//消息类型位（兼容c++代码，暂定4/5）
	MsgClsTrans	uint8	= 4		//需要转发的协议
	MsgClsSelf 	uint8	= 5		//自己处理协议
	//消息头长度
	MsgHeadLen	 	uint16 	= uint16(unsafe.Sizeof(uint16(0))) * 2 + uint16(unsafe.Sizeof(uint64(0))) + uint16(unsafe.Sizeof(uint8(0))) * 3
)

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
func DecodeMessag(conn *TcpConn) (*Message, error) {
	// read header data
	raw := conn.rawConn
	headBytes := make([]byte, MsgHeadLen)
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
	if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgCls); err != nil {
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
	encry := conn.encry
	encry.SetRecvKey(uint32(message.MsgCtx))
	if message.HasFlag(MsgFlagEncr) {
		if buff := encry.Decode(msgBytes, message.MsgCode); buff != nil {
			message.Msgbuf.Write(buff)
		} else {
			Errorf("encry decode message %d twice", message.MsgId)
			return nil, nil
		}
	} else {
		message.Msgbuf.Write(msgBytes)
	}
	return &message, nil
}

// Decode decodes the bytes data into Message
func DecodeSliceMessag(slice []byte) *Message {
	if uint16(len(slice)) < MsgHeadLen {
		Errorf("decode slice message error: %s", ErrMsgLength)
		return nil
	}
	// read header data
	var message Message
	headBuf := bytes.NewReader(slice)
	if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgLen); err != nil {
		Errorf("decode slice message error: %s", err)
		return nil
	}
	if uint16(len(slice)) < message.MsgLen {
		Errorf("decode slice message error: %s", ErrMsgLength)
		return nil
	}
	if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgFlag); err != nil {
		Errorf("decode slice message error: %s", err)
		return nil
	}
	if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgCls); err != nil {
		Errorf("decode slice message error: %s", err)
		return nil
	}
	if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgId); err != nil {
		Errorf("decode slice message error: %s", err)
		return nil
	}
	if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgCode); err != nil {
		Errorf("decode slice message error: %s", err)
		return nil
	}
	if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgCtx); err != nil {
		Errorf("decode slice message error: %s", err)
		return nil
	}
	// read application data
	message.Msgbuf.Write(slice[message.MsgLen:])
	return &message
}

func EncodeMessage(m *Message) ([]byte, error) {
	var msgbuf bytes.Buffer
	m.MsgLen = uint16(m.Msgbuf.Len()) + MsgHeadLen
	if err := binary.Write(&msgbuf, binary.LittleEndian, m.MsgLen); err != nil {
		return nil, err
	}
	if err := binary.Write(&msgbuf, binary.LittleEndian, m.MsgFlag); err != nil {
		return nil, err
	}
	if err := binary.Write(&msgbuf, binary.LittleEndian, m.MsgCls); err != nil {
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
//兼容c++，暂定这个结构
type Message struct {
	MsgLen 	uint16
	MsgFlag	uint8
	MsgCls	uint8
	MsgId 	uint16
	MsgCode	uint8
	MsgCtx  uint64
	Msgbuf  bytes.Buffer
}

//has flag
func (m Message) HasFlag(flag uint8) bool {
	return (m.MsgFlag & flag) == flag
}

//add flag
func (m *Message) AddFlag(flag uint8) {
	m.MsgFlag |= flag
}

//remove flag
func (m *Message) RemoveFlag(flag uint8) {
	m.MsgFlag ^= flag
}


//消息体size
func (m Message) BodyLen() uint16 {
	return m.MsgLen - MsgHeadLen
}

//消息size
func (m Message) Size() uint16 {
	return uint16(m.Msgbuf.Len()) + MsgHeadLen
}

//生产一个消息
func (m *Message) General(id uint16, ctx uint64, data []byte) bool {
	m.MsgId = id
	m.MsgCtx = ctx
	m.MsgCls = MsgClsSelf
	if data != nil {
		if _, err := m.Msgbuf.Write(data); err != nil {
			Errorf("message (%d) write string error: %s", id, err)
			return false
		}
	}
	return true
}

//写入内置类型，bool/int*/[]byte等
func (m *Message) Write(data interface{}) bool {
	if err := binary.Write(&m.Msgbuf, binary.LittleEndian, data); err != nil {
		Errorf("message (%d) write error: %s", m.MsgId, err)
		return false
	}
	return true
}

//读取内置类型，bool/int*/[]byte等, 需传入指针
func (m *Message) Read(data interface{}) bool {
	if err := binary.Read(&m.Msgbuf, binary.LittleEndian, data); err != nil {
		Errorf("message (%d) read error: %s", m.MsgId, err)
		return false
	}
	return true
}

//WriteString
func (m *Message) WriteString(s string) bool {
	if err := binary.Write(&m.Msgbuf, binary.LittleEndian, uint16(len(s))); err != nil {
		Errorf("message (%d) write string length error: %s", m.MsgId, err)
		return false
	}
	if _, err := m.Msgbuf.WriteString(s); err != nil {
		Errorf("message (%d) write string error: %s", m.MsgId, err)
		return false
	}
	return true
}

//ReadString
func (m *Message) ReadString() (string, bool) {
	var length uint16
	if err := binary.Read(&m.Msgbuf, binary.LittleEndian, &length); err != nil {
		Errorf("message (%d) read string length error: %s", m.MsgId, err)
		return "", false
	}
	sbytes := make([]byte, length)
	if _, err := m.Msgbuf.Read(sbytes); err != nil {
		Errorf("message (%d) read string error: %s", m.MsgId, err)
		return "", false
	}
	return string(sbytes), true
}