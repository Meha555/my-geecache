package util

// ByteView holds an immutable view of bytes.
type ByteView struct {
	B []byte
}

func (b ByteView) Size() int {
	return len(b.B)
}

// ByteSlice returns a copy of the data as a byte slice.
func (b ByteView) ByteSlice() []byte {
	return CloneBytes(b.B)
}

// String returns the data as a string, making a copy if necessary.
func (v ByteView) String() string {
	return string(v.B)
}

func CloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
