package dbidx

type Iterator interface {
	// Key returns the key of the currently seeked item
	Key() []byte

	// Release releases resources and must be called after using the iterator
	Release()

	// Next seeks to the next record
	Next() bool

	// Prev seeks to the previous record
	Prev() bool
}
