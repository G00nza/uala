package domain

import "errors"

var (
	ErrNotFound         = errors.New("not found")
	ErrUsernameConflict = errors.New("username already taken")
	ErrAlreadyFollowing = errors.New("already following")
	ErrSelfFollow       = errors.New("cannot follow yourself")
)
