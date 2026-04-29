package domain

import "errors"

var (
	ErrNotFound         = errors.New("not found")
	ErrUsernameConflict = errors.New("username already taken")
	ErrAlreadyFollowing = errors.New("already following")
	ErrSelfFollow       = errors.New("cannot follow yourself")
	ErrEmptyUsername    = errors.New("username cannot be empty")
	ErrEmptyContent     = errors.New("content cannot be empty")
	ErrContentTooLong   = errors.New("content exceeds 280 characters")
)
