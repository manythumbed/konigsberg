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

var Empty Index = extractIndex([]byte{255, 255, 255, 255})

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

	for i, v := range n.Relationships.Bytes() {
		b[1+i] = v
	}
	for i, v := range n.Properties.Bytes() {
		b[5+i] = v
	}

	return b
}

type Link struct {
	Index
	Previous Index
	Next     Index
}

func (l Link) Bytes() []byte {
	b := make([]byte, 12)

	for i, v := range l.Index.Bytes() {
		b[i] = v
	}
	for i, v := range l.Previous.Bytes() {
		b[4+i] = v
	}
	for i, v := range l.Next.Bytes() {
		b[8+i] = v
	}

	return b
}

func extractLink(b []byte) Link {
	return Link{
		Index:    extractIndex(b[0:4]),
		Previous: extractIndex(b[4:8]),
		Next:     extractIndex(b[8:]),
	}
}

type RelationshipRecord struct {
	Active     bool
	Type       Index
	Properties Index
	Start      Link
	End        Link
}

func (r RelationshipRecord) Bytes() []byte {
	b := make([]byte, 33)

	if r.Active {
		b[0] = 1
	}

	for i, v := range r.Type.Bytes() {
		b[1+i] = v
	}
	for i, v := range r.Properties.Bytes() {
		b[5+i] = v
	}
	for i, v := range r.Start.Bytes() {
		b[9+i] = v
	}
	for i, v := range r.End.Bytes() {
		b[21+i] = v
	}
	return b
}

func NewRelationshipRecord(b []byte) (*RelationshipRecord, error) {
	if b == nil || len(b) != 33 {
		return nil, errors.New(fmt.Sprintf("Invalid bytes for relationship record [%v]", b))
	}

	return &RelationshipRecord{
		Active:     b[0] == 1,
		Type:       extractIndex(b[1:5]),
		Properties: extractIndex(b[5:9]),
		Start:      extractLink(b[9:21]),
		End:        extractLink(b[21:]),
	}, nil
}
