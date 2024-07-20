package dbidx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io/fs"
)

// BoltBucket interface compatible with both the original bolt and etc's bbolt
type BoltBucket interface {
	Get(key []byte) []byte
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	ForEach(fn func(k, v []byte) error) error
	// Cursor() *Cursor
}

// BoltCursor interface compatible with both the original bolt and etc's bbolt
type BoltCursor interface {
	First() (key []byte, value []byte)
	Last() (key []byte, value []byte)
	Next() (key []byte, value []byte)
	Prev() (key []byte, value []byte)
	Seek(seek []byte) (key []byte, value []byte)
	//Delete() error
}

// BoltSet is a helper allowing to store indices relating to an object into
// a specific index bucket. prefix is needed only if the same index bucket is
// used to store different type of objects and should correspond to obj's type
// but can be nil if the index bucket only has one type of object.
//
// BoltSet will also handle updates and delete any indices that aren't needed
// anymore. This is expected to run during a bolt Update operation and to be
// isolated (locked) from other updates (bolt will do that for you, typically).
func BoltSet(idxBucket BoltBucket, idxCursor BoltCursor, prefix, key []byte, obj any, indices ...*Index) error {
	// set unique key in @key index (used for quick listing, etc)
	err := idxBucket.Put(append(keyIndex.dataPrefix(prefix, 1, nil), key...), key)
	if err != nil {
		return err
	}

	for _, idx := range indices {
		ptrlist := make(map[string]bool)
		idxprefix := idx.dataPrefix(prefix, 1, nil)
		metaprefix := idx.dataPrefix(prefix, 2, key)

		list := idx.ComputeIndices(idxprefix, key, obj)
		for _, ptr := range list {
			// map[string]... has an optimization when using a []byte conversion to string here
			ptrlist[string(ptr)] = true
			err := idxBucket.Put(ptr, key)
			if err != nil {
				return err
			}
			err = idxBucket.Put(append(metaprefix, ptr[len(idxprefix):]...), ptr)
			if err != nil {
				return err
			}
		}

		k, v := idxCursor.Seek(metaprefix)
		for bytes.HasPrefix(k, metaprefix) {
			if _, found := ptrlist[string(v)]; !found {
				// this does not exist anymore
				err := idxBucket.Delete(v)
				if err != nil {
					return err
				}
				err = idxBucket.Delete(k)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// BoltSearch searches the index for a given entry. A key matching exactly the
// search is required, unless partial is >0, in which case only the number of
// keys specified in partial need to match.
func BoltSearch(idxBucket BoltBucket, idxCursor BoltCursor, prefix []byte, search map[string]any, partial int, indices ...*Index) (Iterator, error) {
	// check if any of the passed indices match the requested search
	if search == nil || len(search) == 0 {
		// this is just a select *
		pfx := keyIndex.dataPrefix(prefix, 1, nil)
		return &boltIterator{idxBucket: idxBucket, idxCursor: idxCursor, prefix: pfx}, nil
	}

	cnt := len(search)
keysloop:
	for _, k := range indices {
		found := 0
		for _, s := range k.Fields {
			if _, ok := search[s]; ok {
				found += 1
			} else {
				continue keysloop
			}
		}
		if found != cnt {
			continue
		}

		// found the one!
		spfx, err := k.ComputeSearchPrefix(prefix, search, partial)
		if err != nil {
			return nil, err
		}
		return &boltIterator{idxBucket: idxBucket, idxCursor: idxCursor, prefix: spfx}, nil
	}

	return nil, errors.New("no key matching search")
}

// BoltSearchOne will return the first key matching the search
func BoltSearchOne(idxBucket BoltBucket, idxCursor BoltCursor, prefix []byte, search map[string]any, partial int, indices ...*Index) ([]byte, error) {
	it, err := BoltSearch(idxBucket, idxCursor, prefix, search, partial, indices...)
	if err != nil {
		return nil, err
	}
	// release is not needed for bolt, but it's a good habit to put it here
	defer it.Release()

	if !it.Next() {
		return nil, fs.ErrNotExist
	}
	return it.Key(), nil
}

type boltIterator struct {
	idxBucket BoltBucket
	idxCursor BoltCursor
	prefix    []byte
	seeked    bool
	valid     bool
	k, v      []byte
}

func (b *boltIterator) Key() []byte {
	if !b.valid {
		return nil
	}
	return b.v
}

func (b *boltIterator) Next() bool {
	if !b.seeked {
		b.k, b.v = b.idxCursor.Seek(b.prefix)
		b.valid = bytes.HasPrefix(b.k, b.prefix)
		return b.valid
	}
	if !b.valid {
		return false
	}
	b.k, b.v = b.idxCursor.Next()
	b.valid = bytes.HasPrefix(b.k, b.prefix)
	return b.valid
}

func (b *boltIterator) Prev() bool {
	if !b.valid {
		return false
	}
	b.k, b.v = b.idxCursor.Prev()
	b.valid = bytes.HasPrefix(b.k, b.prefix)
	return b.valid
}

func (b *boltIterator) Release() {
}

// BoltDelete deletes all entries related to the object at the given key
func BoltDelete(idxBucket BoltBucket, idxCursor BoltCursor, prefix, key []byte) error {
	//idxprefix := append(prefix, 0x01)
	metaprefix := append(append(append(prefix, 0x02), binary.AppendUvarint(nil, uint64(len(key)))...), key...)

	k, v := idxCursor.Seek(metaprefix)
	for bytes.HasPrefix(k, metaprefix) {
		err := idxBucket.Delete(v)
		if err != nil {
			return err
		}
		err = idxBucket.Delete(k)
		if err != nil {
			return err
		}
	}
	err := idxBucket.Delete(append(keyIndex.dataPrefix(prefix, 1, nil), key...))
	if err != nil {
		return err
	}
	return nil
}
