package dispatch

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPermanentError_ErrorAndUnwrap(t *testing.T) {
	base := errors.New("bad thing")
	pe := NewPermanentError(base)

	assert.Equal(t, "bad thing", pe.Error())
	assert.True(t, errors.Is(pe, base), "errors.Is must unwrap to base")
}

func TestIsPermanent_True(t *testing.T) {
	pe := NewPermanentError(errors.New("x"))
	assert.True(t, isPermanent(pe))
}

func TestIsPermanent_TrueWhenWrapped(t *testing.T) {
	pe := NewPermanentError(errors.New("inner"))
	wrapped := fmt.Errorf("outer: %w", pe)
	assert.True(t, isPermanent(wrapped), "errors.As must see PermanentError through wrapping")
}

func TestIsPermanent_FalseForPlainError(t *testing.T) {
	assert.False(t, isPermanent(errors.New("plain")))
}

func TestIsPermanent_FalseForNil(t *testing.T) {
	assert.False(t, isPermanent(nil))
}
