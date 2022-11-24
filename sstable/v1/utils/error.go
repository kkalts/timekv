package utils

import "errors"

/*
	相关错误
*/
var (
	ErrChecksumMismatch = errors.New("checksum mismatch")
	ErrKeyNotFound      = errors.New("Key not found")
)
