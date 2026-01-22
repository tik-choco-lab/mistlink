package main

import (
	"crypto/rand"
	"encoding/hex"
)

func generateRoomID() string {
	b := make([]byte, 4) // 8文字の16進数
	rand.Read(b)
	return hex.EncodeToString(b)
}
