package crystal

import (
	"strconv"
	"sync"
	"time"
)

const(
 	MAX_WORLD	 	=	4096
	MAX_SERIAL 	=	4096
)
var(
	mutex sync.Mutex
	serials int64 = 0
	last_time int64 = 0
)

// w - 世界号，12位，(0~4095)
// t - 实体类型，8位 (0~255)
// s - 序号，12位 (0~4095)
// ts - 时间戳，32位
func NewGuid(world uint16, uType uint8) uint64 {
	mutex.Lock()
	defer mutex.Unlock()

	now_time := time.Now().Unix()
	if now_time > last_time {
		serials = 0
		last_time = now_time
	} else {
		serials ++
		if serials >= MAX_SERIAL {
			now_time++
			serials = 0
			last_time = now_time
		}
	}
	world %= MAX_WORLD
	return uint64(world) << 52 | uint64(uType) << 44 | uint64(serials) << 32 | uint64(now_time)
}

func NewGuidStr(world uint16, uType uint8) string {
	guid := NewGuid(world, uType)
	return strconv.FormatUint(guid, 10)
}

func Guid2Str(guid uint64) string {
	return strconv.FormatUint(guid, 10)
}

func Str2Guid(idStr string) (uint64, bool) {
	if i64, err := strconv.ParseUint(idStr, 10, 0); err != nil {
		Errorf("Parse String %s To Uint64 Error!", idStr)
		return 0, false
	} else {
		return i64, true
	}
}

func GuidWorld(guid uint64) uint16 {
	return uint16((guid >> 52) & 0xfff)
}

func GuidType(guid uint64) uint8 {
	return uint8((guid >> 44) & 0xff)
}

func GuidSerial(guid uint64) uint16 {
	return uint16((guid >> 32) & 0xfff)
}

func GuidTimeStamp(guid uint64) uint32 {
	return uint32(guid & 0xffffffff)
}



