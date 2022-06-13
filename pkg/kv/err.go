package kv

import "errors"

var (
	ErrKeyNotFound      = errors.New("key not found")
	ErrKeyAlreadyExists = errors.New("key already exists")
	ErrMethodNotAllowed = errors.New("method is not allowd")
	ErrNonexistentDB    = errors.New("db file does not exist")
)
