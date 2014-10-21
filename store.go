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
