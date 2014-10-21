package store

import (
	"errors"
	"fmt"
	"io"
)

type Fetcher interface {
	Fetch(index int64) ([]byte, error)
}

type ReaderFetcher struct {
	recordSize int
	io.ReaderAt
}

func (f ReaderFetcher) Fetch(index int64) ([]byte, error) {
	offset := int64(f.recordSize) * index
	buffer := make([]byte, f.recordSize)

	n, err := f.ReadAt(buffer, offset)
	if err != nil {
		return nil, err
	}
	if n != f.recordSize {
		return nil, errors.New(fmt.Sprintf("%d bytes extracted from reader expected %d", n, f.recordSize))
	}

	return buffer, nil
}

type Index int64

func (i Index) Bytes() []byte {
	b := make([]byte, 4)

	b[0] = byte(i)
	b[1] = byte(i >> 8)
	b[2] = byte(i >> 16)
	b[3] = byte(i >> 24)

	return b
}

type NodeRecord struct {
	Active        bool
	Relationships Index
	Properties    Index
}

func NewNodeRecord(b []byte) (*NodeRecord, error) {
	if b == nil || len(b) != 9 {
		return nil, errors.New(fmt.Sprintf("Invalid bytes for node record [%v]", b))
	}

	return &NodeRecord{b[0] == 1, extractIndex(b[1:5]), extractIndex(b[5:])}, nil
}

func extractIndex(b []byte) Index {
	return Index(int64(b[0]) | int64(b[1])<<8 | int64(b[2])<<16 | int64(b[3])<<24)
}

func (n NodeRecord) Bytes() []byte {
	b := make([]byte, 9)

	if n.Active {
		b[0] = 1
	}

	r := n.Relationships.Bytes()
	p := n.Properties.Bytes()

	b[1] = r[0]
	b[2] = r[1]
	b[3] = r[2]
	b[4] = r[3]

	b[5] = p[0]
	b[6] = p[1]
	b[7] = p[2]
	b[8] = p[3]

	return b
}
