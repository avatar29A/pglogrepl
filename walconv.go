package pglogrepl

import (
	"encoding/binary"
)

//
// Contains various converters from bytes to specific value types.

func toInt64(bs []byte) int64 {
	return int64(binary.BigEndian.Uint64(bs))
}

func toInt32(bs []byte) int32 {
	return int32(binary.BigEndian.Uint32(bs))
}

func toInt16(bs []byte) int16 {
	return int16(binary.BigEndian.Uint16(bs))
}

func toInt8(bs []byte) int8 {
	return int8(bs[0])
}

func toBool(bs []byte) bool {
	return bs[0] == 1
}

func toString(bs []byte) (s string, n int) {
	for i:=0; i<len(bs); i++ {
		if bs[i] == 0 {
			return string(bs[:i]), i+1
		}
	}

	return
}
