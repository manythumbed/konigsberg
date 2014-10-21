package store

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestFetcher(t *testing.T) {
	var tests = []struct {
		data  []byte
		size  int
		index int64
		want  []byte
		err   error
	}{
		{[]byte{1, 2, 3}, 1, 0, []byte{1}, nil},
		{[]byte{1, 2, 3}, 1, 1, []byte{2}, nil},
		{[]byte{1, 2, 3}, 1, 2, []byte{3}, nil},
		{[]byte{1, 2, 3}, 1, 3, nil, errors.New("EOF")},
		{[]byte{1, 2, 3}, 2, 0, []byte{1, 2}, nil},
		{[]byte{1, 2, 3}, 2, 1, nil, errors.New("EOF")},
	}

	for _, test := range tests {
		f := ReaderFetcher{test.size, bytes.NewReader(test.data)}
		out, err := f.Fetch(test.index)
		if !reflect.DeepEqual(err, test.err) {
			t.Errorf("[%v %v].Fetch(%v) = %v, want %v", test.size, test.data, test.index, err, test.err)
		}
		if !reflect.DeepEqual(out, test.want) {
			t.Errorf("[%v %v].Fetch(%v) = %v, want %v", test.size, test.data, test.index, out, test.want)
		}
	}
}
