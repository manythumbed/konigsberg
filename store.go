package storage

import (
	"errors"
	"io"
)

type NodeStore interface {
	Fetch(index Index) (*Node, bool)
	Store(index Index, node Node) bool
}

type RelationshipStore interface {
	Fetch(index Index) (*Relationship, bool)
	Store(index Index, relationship Relationship) bool
}

type fetcher struct {
	size int
	io.ReaderAt
}

func (f fetcher) fetch(index int64) ([]byte, error) {
	offset := int64(f.size) * index
	buffer := make([]byte, f.size)

	n, err := f.ReadAt(buffer, offset)
	if err != nil {
		return nil, err
	}
	if n != f.size {
		return nil, errors.New("Unable to read correct number of bytes")
	}

	return buffer, nil
}
