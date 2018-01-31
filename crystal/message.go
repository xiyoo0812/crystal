package crystal

import (
	"bytes"
	"unsafe"
	"encoding/binary"
	"fmt"
	"io"
	"websocket"
)

// MessageHeadLength is the length of type header.
const (
	//消息标志位
	MsgFlagEncr	uint8	= 0x01		//加密
	MsgFlagBoard	uint8	= 0x02		//广播
	MsgFlagGroup	uint8	= 0x04		//指定组发
	MsgFlagComp	uint8	= 0x08		//压缩
	MsgFlagLittle	uint8	= 0x10		//小包
	//消息头长度
	MsgHeadLen	 	uint16 	= uint16(unsafe.Sizeof(uint16(0))) * 2 + uint16(unsafe.Sizeof(uint8(0))) * 2
	MsgHeadLenBig 	uint16 	= uint16(unsafe.Sizeof(uint32(0)))
)

// HandlerFunc serves as an adapter to allow the use of ordinary functions as handlers.
type HandlerFunc func(*Message, *NetConn)

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
func DecodeWebMessag(conn *NetConn) *Message {
	raw := conn.webConn
	var message Message
	if mtype, readBytes, err := raw.ReadMessage(); err != nil{
		Errorf("error decoding web message %v", err)
		return nil
	} else if mtype == websocket.CloseMessage {
		return nil
	} else if mtype == websocket.BinaryMessage {
		for len(readBytes) < int(message.MsgLen) {
			if mtype, newBytes, err := raw.ReadMessage(); err != nil {
				return nil
			} else if mtype == websocket.BinaryMessage {
				readBytes = append(readBytes, newBytes ...)
			}
		}
		headBuf := bytes.NewReader(readBytes[0 : MsgHeadLen+MsgHeadLenBig])
		if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgLen); err != nil {
			Errorf("error decoding message %v", err)
			return nil
		}
		if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgFlag); err != nil {
			Errorf("error decoding message %v", err)
			return nil
		}
		if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgId); err != nil {
			Errorf("error decoding message %v", err)
			return nil
		}
		if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgCode); err != nil {
			Errorf("error decoding message %v", err)
			return nil
		}
		headLen := MsgHeadLen
		if !message.HasFlag(MsgFlagLittle) {
			headLen += MsgHeadLenBig
			if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgCtx); err != nil {
				Errorf("error decoding message %v", err)
				return nil
			}
		}
		msgBytes := readBytes[headLen : ]
		if message.HasFlag(MsgFlagEncr) {
			if buff := conn.encry.Decode(msgBytes, message.MsgCode); buff != nil {
				message.Msgbuf.Write(buff)
			} else {
				Errorf("encry decode message %d error", message.MsgId)
				return nil
			}
		} else {
			message.Msgbuf.Write(msgBytes)
		}
	}
	return &message
}

// Decode decodes the bytes data into Message
func DecodeMessag(conn *NetConn) *Message {
	// read header data
	raw := conn.rawConn
	headBytes := make([]byte, MsgHeadLen)
	_, err := io.ReadFull(raw, headBytes)
	if err != nil {
		Errorf("error decoding message %v", err)
		return nil
	}
	Errorln("decoding message0", headBytes)
	var message Message
	headBuf := bytes.NewReader(headBytes)
	if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgLen); err != nil {
		Errorf("error decoding message %v", err)
		return nil
	}
	if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgFlag); err != nil {
		Errorf("error decoding message %v", err)
		return nil
	}
	if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgId); err != nil {
		Errorf("error decoding message %v", err)
		return nil
	}
	if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgCode); err != nil {
		Errorf("error decoding message %v", err)
		return nil
	}
	bodyLen := message.MsgLen - MsgHeadLen
	if !message.HasFlag(MsgFlagLittle) {
		headBytes := make([]byte, MsgHeadLenBig)
		_, err = io.ReadFull(raw, headBytes)
		if err != nil {
			Errorf("error decoding message %v", err)
			return nil
		}
		headBuf := bytes.NewReader(headBytes)
		if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgCtx); err != nil {
			Errorf("error decoding message %v", err)
			return nil
		}
		bodyLen -= MsgHeadLenBig
		conn.encry.SetRecvKey(uint32(message.MsgCtx))
	}
	// read application data
	msgBytes := make([]byte, bodyLen)
	_, err = io.ReadFull(raw, msgBytes)
	if err != nil {
		Errorf("error decoding message %v", err)
		return nil
	}
	if message.HasFlag(MsgFlagEncr) {
		if buff := conn.encry.Decode(msgBytes, message.MsgCode); buff != nil {
			message.Msgbuf.Write(buff)
		} else {
			Errorf("encry decode message %d error", message.MsgId)
			return nil
		}
	} else {
		message.Msgbuf.Write(msgBytes)
	}
	return &message
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
	if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgId); err != nil {
		Errorf("decode slice message error: %s", err)
		return nil
	}
	if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgCode); err != nil {
		Errorf("decode slice message error: %s", err)
		return nil
	}
	headLen := MsgHeadLen
	if !message.HasFlag(MsgFlagLittle) {
		if err := binary.Read(headBuf, binary.LittleEndian, &message.MsgCtx); err != nil {
			Errorf("decode slice message error: %s", err)
			return nil
		}
		headLen += MsgHeadLenBig
	}
	// read application data
	message.Msgbuf.Write(slice[headLen:])
	return &message
}

func EncodeMessage(m *Message) []byte {
	m.CheckSize()
	var msgbuf bytes.Buffer
	if err := binary.Write(&msgbuf, binary.LittleEndian, m.MsgLen); err != nil {
		Errorf("error encoding message %v", err)
		return nil
	}
	if err := binary.Write(&msgbuf, binary.LittleEndian, m.MsgFlag); err != nil {
		Errorf("error encoding message %v", err)
		return nil
	}
	if err := binary.Write(&msgbuf, binary.LittleEndian, m.MsgId); err != nil {
		Errorf("error encoding message %v", err)
		return nil
	}
	if err := binary.Write(&msgbuf, binary.LittleEndian, m.MsgCode); err != nil {
		Errorf("error encoding message %v", err)
		return nil
	}
	if !m.HasFlag(MsgFlagLittle) {
		if err := binary.Write(&msgbuf, binary.LittleEndian, m.MsgCtx); err != nil {
			Errorf("error encoding message %v", err)
			return nil
		}
	}
	if err := binary.Write(&msgbuf, binary.LittleEndian, m.Msgbuf.Bytes()); err != nil {
		Errorf("error encoding message %v", err)
		return nil
	}
	return msgbuf.Bytes()
}

// Message represents the structured data that can be handled.
//兼容c++，暂定这个结构
type Message struct {
	MsgLen 	uint16
	MsgFlag	uint8
	MsgId 	uint16
	MsgCode	uint8
	MsgCtx  uint32
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

func (m *Message) CheckSize(){
	m.MsgLen = uint16(m.Msgbuf.Len()) + MsgHeadLen
	if m.MsgCtx > 0 {
		m.MsgLen += MsgHeadLenBig
		m.RemoveFlag(MsgFlagLittle)
	} else {
		m.AddFlag(MsgFlagLittle)
	}
}

//生产一个消息
func (m *Message) General(id uint16, ctx uint32, data []byte) bool {
	m.MsgId = id
	m.MsgCtx = ctx
	m.MsgFlag = MsgFlagLittle
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
