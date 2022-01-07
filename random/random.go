package random

import (
	"encoding/binary"
	"math/rand"
	"sync"
	"time"
)

const letterBytes = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func String(n int) string {
	return string(VisibleBytes(n))
}

var src = rand.NewSource(time.Now().UnixNano())
var l sync.Mutex

func VisibleBytes(n int) []byte {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	l.Lock()
	cache := src.Int63()
	l.Unlock()
	for i, remain := n-1, letterIdxMax; i >= 0; {
		if remain == 0 {
			l.Lock()
			cache = src.Int63()
			l.Unlock()
			remain = letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return b
}

const channelSize = 64

var (
	uint64Channel = make(chan uint64, channelSize)
)

func init() {
	for i := 0; i < channelSize; i++ {
		go func() {
			r := rand.NewSource(time.Now().UnixNano())
			for {
				uint64Channel <- uint64(r.Int63())
			}
		}()
	}
}

func ceil8(n int) int {
	if n%8 != 0 {
		n = (n/8 + 1) * 8
	}
	return n
}

func Bytes(n int) []byte {
	m := ceil8(n)
	b := make([]byte, m)
	for i := 0; i < m; i += 8 {
		binary.BigEndian.PutUint64(b[i:i+8], <-uint64Channel)
	}
	return b[:n]
}
