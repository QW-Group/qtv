package qtv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"unsafe"
)

//
// Contains data reader/writer for primitive types.
//

// Message reader.
// Deserialize primitive types like uint16/uint32/float32/string etc.
type netMsgR struct {
	b          []byte
	rPos       int
	floatCoord bool
	error      bool
}

func newNetMsgR(b []byte, floatCoord bool) *netMsgR {
	msg := &netMsgR{}
	msg.Reset(b, floatCoord)
	return msg
}

func (msg *netMsgR) Reset(b []byte, floatCoord bool) {
	msg.b = b
	msg.rPos = 0
	msg.floatCoord = floatCoord
	msg.error = false
}

// Do not reset buffer and floatCoord.
func (msg *netMsgR) Clear() *netMsgR {
	msg.rPos = 0
	msg.error = false
	return msg
}

// Read position.
func (msg *netMsgR) RPos() int {
	return msg.rPos
}

// Underlying buffer len.
func (msg *netMsgR) BLen() int {
	return len(msg.b)
}

func (msg *netMsgR) GetByte() byte {
	if msg.error || msg.rPos+1 > len(msg.b) {
		msg.error = true
		return 0
	}
	v := msg.b[msg.rPos]
	msg.rPos += 1
	return v
}

func (msg *netMsgR) GetSVC() protocolSvc {
	return protocolSvc(msg.GetByte())
}

func (msg *netMsgR) GetUint16() uint16 {
	if msg.error || msg.rPos+2 > len(msg.b) {
		msg.error = true
		return 0
	}
	v := binary.LittleEndian.Uint16(msg.b[msg.rPos:])
	msg.rPos += 2
	return v
}

func (msg *netMsgR) GetInt16() int16 {
	return int16(msg.GetUint16())
}

func (msg *netMsgR) GetUint32() uint32 {
	if msg.error || msg.rPos+4 > len(msg.b) {
		msg.error = true
		return 0
	}
	v := binary.LittleEndian.Uint32(msg.b[msg.rPos:])
	msg.rPos += 4
	return v
}

func (msg *netMsgR) GetAngle16() float32 {
	return float32(msg.GetUint16()) * 360 / 65536
}

func (msg *netMsgR) GetAngle() float32 {
	if msg.floatCoord {
		return msg.GetAngle16()
	}
	return float32(msg.GetByte()) * 360 / 256
}

func (msg *netMsgR) GetCoord() float32 {
	if msg.floatCoord {
		return msg.GetFloat32()
	}
	return float32(msg.GetInt16()) / 8
}

func (msg *netMsgR) GetFloat32() float32 {
	l := msg.GetUint32()
	return *(*float32)(unsafe.Pointer(&l))
}

// Avoid using this functions, it returns temporary data which could be overwritten.
// It exists for the sake of reducing memory allocations.
func (msg *netMsgR) GetTmpBytes() []byte {
	if msg.error || msg.rPos+1 > len(msg.b) {
		msg.error = true
		return nil
	}
	oldRPos := msg.rPos
	idx := bytes.IndexByte(msg.b[msg.rPos:], 0)
	if idx == -1 {
		idx = len(msg.b)
		msg.rPos = idx // To the end of string, zero byte nor found.
	} else {
		idx = msg.rPos + idx
		msg.rPos = idx + 1 // Skip zero byte.
	}
	return msg.b[oldRPos:idx]
}

func (msg *netMsgR) GetString() string {
	var b strings.Builder
	for c := msg.GetByte(); c != 0; c = msg.GetByte() {
		b.WriteByte(c)
	}
	return b.String()
}

func (msg *netMsgR) SkipString() {
	for msg.GetByte() != 0 {
	}
}

// Message writer.
// Serialize primitive types like uint16/uint32/float32/string etc.
type netMsgW struct {
	b          []byte
	wPos       int
	floatCoord bool
	error      bool
}

func newNetMsgW(b []byte, floatCoord bool) *netMsgW {
	msg := &netMsgW{}
	msg.Reset(b, floatCoord)
	return msg
}

func (msg *netMsgW) Reset(b []byte, floatCoord bool) {
	msg.b = b
	msg.wPos = 0
	msg.floatCoord = floatCoord
	msg.error = false
}

// Do not reset buffer and floatCoord.
func (msg *netMsgW) Clear() *netMsgW {
	msg.wPos = 0
	msg.error = false
	return msg
}

func (msg *netMsgW) Bytes() []byte {
	return msg.b[0:msg.wPos]
}

// Write position, equals to how much bytes written to this msg.
func (msg *netMsgW) WPos() int {
	return msg.wPos
}

// Underlying buffer len.
func (msg *netMsgW) BLen() int {
	return len(msg.b)
}

func (msg *netMsgW) PutByte(b byte) {
	if msg.error || msg.wPos+1 > len(msg.b) {
		msg.error = true
		return
	}
	msg.b[msg.wPos] = b
	msg.wPos += 1
}

func (msg *netMsgW) PutSVC(svc protocolSvc) {
	msg.PutByte(byte(svc))
}

func (msg *netMsgW) PutUint16(v uint16) {
	if msg.error || msg.wPos+2 > len(msg.b) {
		msg.error = true
		return
	}
	binary.LittleEndian.PutUint16(msg.b[msg.wPos:], v)
	msg.wPos += 2
	return
}

// Updates previously written value, does not advance write position.
func (msg *netMsgW) UpdateUint16At(v uint16, pos int) {
	if msg.error || pos+2 > len(msg.b) || pos+2 > msg.wPos {
		msg.error = true
		return
	}
	binary.LittleEndian.PutUint16(msg.b[pos:], v)
	return
}

func (msg *netMsgW) PutUint32(v uint32) {
	if msg.error || msg.wPos+4 > len(msg.b) {
		msg.error = true
		return
	}
	binary.LittleEndian.PutUint32(msg.b[msg.wPos:], v)
	msg.wPos += 4
	return
}

// Updates previously written value, does not advance write position.
func (msg *netMsgW) UpdateUint32At(v uint32, pos int) {
	if msg.error || pos+4 > len(msg.b) || pos+4 > msg.wPos {
		msg.error = true
		return
	}
	binary.LittleEndian.PutUint32(msg.b[pos:], v)
	return
}

func (msg *netMsgW) PutFloat32(v float32) {
	l := *(*uint32)(unsafe.Pointer(&v))
	msg.PutUint32(l)
}

func qRint(x float32) int {
	if x > 0 {
		return int(x + 0.5)
	} else {
		return int(x - 0.5)
	}
}

func (msg *netMsgW) PutAngle16(v float32) {
	msg.PutUint16(uint16(qRint(v*65536/360) & 65535))
}

func (msg *netMsgW) PutAngle(v float32) {
	if msg.floatCoord {
		msg.PutAngle16(v)
	} else {
		msg.PutByte(byte(qRint(v*256/360) & 255))
	}
}

func (msg *netMsgW) PutCoord(v float32) {
	if msg.floatCoord {
		msg.PutFloat32(v)
	} else {
		msg.PutUint16(uint16(v * 8))
	}
}

// Write string without NUL terminator.
func (msg *netMsgW) PutString2(str string) {
	for i := 0; i < len(str) && str[i] != 0; i++ {
		msg.PutByte(str[i])
	}
}

func (msg *netMsgW) PutString2f(format string, a ...interface{}) {
	msg.PutString2(fmt.Sprintf(format, a...))
}

func (msg *netMsgW) PutString(str string) {
	msg.PutString2(str)
	msg.PutByte(0)
}

func (msg *netMsgW) PutStringf(format string, a ...interface{}) {
	msg.PutString(fmt.Sprintf(format, a...))
}

func (msg *netMsgW) PutData(b []byte) {
	if msg.error || msg.wPos+len(b) > len(msg.b) {
		msg.error = true
		return
	}
	copy(msg.b[msg.wPos:], b)
	msg.wPos += len(b)
}

func (msg *netMsgW) PutDataFromReader(r io.Reader, n int) {
	if msg.error || msg.wPos+n > len(msg.b) || r == nil {
		msg.error = true
		return
	}
	rn, err := r.Read(msg.b[msg.wPos : msg.wPos+n])
	if rn != n {
		msg.error = true // Consider short read as an error.
		return
	}
	msg.wPos += rn
	if err != nil && rn == 0 {
		msg.error = true
	}
}
