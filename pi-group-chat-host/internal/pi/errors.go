package pi

import "errors"

var (
	// ErrNotImplemented is returned by every executable stub in this package.
	ErrNotImplemented = errors.New("pi: not implemented")

	// These protocol errors define the eventual adapter contract. The initial
	// stubs deliberately return ErrNotImplemented instead.
	ErrVersionMismatch = errors.New("PI_VERSION_MISMATCH")
	ErrPromptRejected  = errors.New("PI_PROMPT_REJECTED")
	ErrProtocol        = errors.New("PI_PROTOCOL_ERROR")
	ErrProcessExited   = errors.New("PI_PROCESS_EXITED")
	ErrCancelled       = errors.New("PI_CANCELLED")
	ErrToolNotAllowed  = errors.New("pi: tool not allowed")
)
