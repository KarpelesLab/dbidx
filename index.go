package dbidx

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"

	"github.com/KarpelesLab/typutil"
	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

type Index struct {
	Name   string   `json:"name"`
	Fields []string `json:"fields"`
	Method string   `json:"method"` // one of: utf8, binary (todo: add more)
	Unique bool     `json:"unique,omitempty"`
}

// encodeValueContext is used to optimize some aspects of value encoding
type encodeValueContext struct {
	colBuf *collate.Buffer
}

var (
	keyIndex = &Index{Name: "@key", Method: "binary", Unique: true}
)

// ComputeIndices returns a number of indices that can be used to index the given value
func (k *Index) ComputeIndices(pfx []byte, id []byte, v any) [][]byte {
	var res [][]byte
	encodeCtx := &encodeValueContext{}

	// start key with type name & key name
	newKey := append(pfx, []byte(k.Name+"\x00")...)

	cnt := 0
	forceNotUnique := false
	for _, fn := range k.Fields {
		val := getValueForField(v, fn)
		if val == nil {
			if cnt > 0 {
				// partial key that ends on a nil, force it to be non-unique
				forceNotUnique = true
				break
			}
			// first part of key is missing, drop the whole key
			return nil
		}
		cnt += 1
		newKey = append(append(newKey, 0), k.encodeValue(encodeCtx, val)...)
	}

	if forceNotUnique || !k.Unique {
		// append object ID to ensure key will not hit anything existing
		newKey = append(append(newKey, 0), id...)
	}

	res = append(res, newKey)

	return res
}

// ComputeSearchPrefix will compute a prefix that can be used to locate data based on a given search
func (k *Index) ComputeSearchPrefix(pfx []byte, search map[string]any, partial int) ([]byte, error) {
	encodeCtx := &encodeValueContext{}
	newKey := k.dataPrefix(pfx, 1, nil)

	for n, fn := range k.Fields {
		if partial > 0 && n >= partial {
			return newKey, nil
		}
		val, found := search[fn]
		if !found || val == nil {
			return nil, errors.New("search value cannot be nil")
		}

		newKey = append(append(newKey, 0), k.encodeValue(encodeCtx, val)...)
	}

	return newKey, nil
}

// encodeValue converts a value into the appropriate index representation based on the collation
func (k *Index) encodeValue(ctx *encodeValueContext, val any) []byte {
	switch k.Method {
	case "utf8": // utf8_general_ci
		// TODO allow parameters of collation to be set in index
		col := collate.New(language.Und, collate.Loose)
		ss, _ := typutil.AsString(val)
		if ctx.colBuf == nil {
			ctx.colBuf = &collate.Buffer{}
		}
		res := col.KeyFromString(ctx.colBuf, ss)
		//log.Printf("ss = %s key = %x", ss, res)
		return res
	case "binary":
		ss, _ := typutil.AsByteArray(val)
		return []byte(ss)
	default:
		// do same as binary
		ss, _ := typutil.AsString(val)
		return []byte(ss)
	}
}

// getValueForField returns the value for a specified field on many kinds of golang values
func getValueForField(v any, f string) any {
	fa := strings.Split(f, "/")
	var e error
	ctx := context.Background()

	for _, s := range fa {
		v, e = typutil.OffsetGet(ctx, v, s)
		if e != nil {
			return nil
		}
	}
	return v
}

// dataPrefix appends the index name followed by a nil char to pfx. If a key is specified, its length followed by the
// key value itself is also appended
func (k *Index) dataPrefix(pfx []byte, typ byte, key []byte) []byte {
	res := append(append(append(pfx, typ), k.Name...), 0)
	if key != nil {
		res = append(binary.AppendUvarint(res, uint64(len(key))), key...)
	}
	return res
}
