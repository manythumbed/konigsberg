package storage

import (
	"errors"
	"fmt"
)

type Index int64

var Empty Index = extractIndex([]byte{255, 255, 255, 255})

func extractIndex(b []byte) Index {
	return Index(int64(b[0]) | int64(b[1])<<8 | int64(b[2])<<16 | int64(b[3])<<24)
}

func (i Index) bytes() []byte {
	b := make([]byte, 4)

	b[0] = byte(i)
	b[1] = byte(i >> 8)
	b[2] = byte(i >> 16)
	b[3] = byte(i >> 24)

	return b
}

type Node struct {
	Active        bool
	Relationships Index
	Properties    Index
}

func newNode(b []byte) (*Node, error) {
	if b == nil || len(b) != 9 {
		return nil, errors.New(fmt.Sprintf("Invalid bytes for node record [%v]", b))
	}

	return &Node{b[0] == 1, extractIndex(b[1:5]), extractIndex(b[5:])}, nil
}

func (n Node) bytes() []byte {
	b := make([]byte, 9)

	if n.Active {
		b[0] = 1
	}

	for i, v := range n.Relationships.bytes() {
		b[1+i] = v
	}
	for i, v := range n.Properties.bytes() {
		b[5+i] = v
	}

	return b
}

type Link struct {
	Index
	Previous Index
	Next     Index
}

func (l Link) bytes() []byte {
	b := make([]byte, 12)

	for i, v := range l.Index.bytes() {
		b[i] = v
	}
	for i, v := range l.Previous.bytes() {
		b[4+i] = v
	}
	for i, v := range l.Next.bytes() {
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

type Relationship struct {
	Active     bool
	Type       Index
	Properties Index
	Start      Link
	End        Link
}

func (r Relationship) bytes() []byte {
	b := make([]byte, 33)

	if r.Active {
		b[0] = 1
	}

	for i, v := range r.Type.bytes() {
		b[1+i] = v
	}
	for i, v := range r.Properties.bytes() {
		b[5+i] = v
	}
	for i, v := range r.Start.bytes() {
		b[9+i] = v
	}
	for i, v := range r.End.bytes() {
		b[21+i] = v
	}
	return b
}

func newRelationship(b []byte) (*Relationship, error) {
	if b == nil || len(b) != 33 {
		return nil, errors.New(fmt.Sprintf("Invalid bytes for relationship record [%v]", b))
	}

	return &Relationship{
		Active:     b[0] == 1,
		Type:       extractIndex(b[1:5]),
		Properties: extractIndex(b[5:9]),
		Start:      extractLink(b[9:21]),
		End:        extractLink(b[21:]),
	}, nil
}
