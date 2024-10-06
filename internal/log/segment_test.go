package log

import (
	api "glog/api/v1"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T){
	dir,err := os.MkdirTemp("","segment_test")
	defer os.RemoveAll(dir)

	expected := &api.Record{Value : []byte("test string")}
	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entryWidth*3
	s,err := newSegment(c,dir,16)
	require.NoError(t,err)
	require.Equal(t, uint64(16), s.nextOffset,s.nextOffset)
	require.False(t, s.IsFull())
	for i:= uint64(0); i<3;i++{
		off,err:= s.Append(expected)
		require.NoError(t,err)
		require.Equal(t,off,16+i)
		got, err:= s.Read(off)
		require.NoError(t,err)
		require.Equal(t,expected.Value,got.Value)
	}
	_, err = s.Append(expected)
	require.Equal(t,io.EOF,err)
	require.True(t,s.IsFull())
	c.Segment.MaxStoreBytes = uint64(len(expected.Value)*3)
	c.Segment.MaxIndexBytes = 1024
	s, err = newSegment(c,dir,16)
	require.NoError(t,err)
	require.True(t,s.IsFull())
	err = s.Clear()
	require.NoError(t,err)
	s, err = newSegment(c,dir,16)
	require.NoError(t,err)
	require.False(t, s.IsFull())
}