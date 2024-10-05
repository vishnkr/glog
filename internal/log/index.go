package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offsetWidth uint64 = 4
	positionWidth uint64 = 8
	entryWidth = offsetWidth + positionWidth
)

type index struct{
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File,c Config)(*index,error){
	idx:= &index{
		file:f,
	}
	fi,err := os.Stat(f.Name())
	if err!=nil{
		return nil,err
	}

	idx.size = uint64(fi.Size())
	if err = os.Truncate(
		f.Name(),int64(c.Segment.MaxIndexBytes),
	); err!=nil {
		return nil,err
	}
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ | gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err!=nil{
		return nil,err
	}
	return idx,nil
}

func (i *index) Read(in int64)(out uint32,pos uint64, err error){
	if i.size == 0{
		return 0,0,io.EOF
	}
	if in==-1{
		out = uint32((i.size/entryWidth)-1)
	} else{
		out = uint32(in)
	}
	pos = uint64(out)*entryWidth
	if i.size < pos+entryWidth{
		return 0,0, io.EOF
	}
	out = encoding.Uint32(i.mmap[pos:pos+offsetWidth])
	pos = encoding.Uint64(i.mmap[pos+offsetWidth: pos+entryWidth])
	return out, pos,nil
}

func (i *index) Write(off uint32, pos uint64) error{
	// check if space exists in mmap
	if uint64(len(i.mmap)) < i.size + entryWidth{
		return io.EOF
	}
	encoding.PutUint32(i.mmap[i.size:i.size+offsetWidth],off)
	encoding.PutUint64(i.mmap[i.size+offsetWidth:i.size+entryWidth],pos)
	i.size += uint64(entryWidth)
	return nil
}

func (i *index) Name() string{
	return i.file.Name()
}

func (i *index) Close()error{
	if err := i.mmap.Sync(gommap.MS_SYNC);err!=nil{
		return err
	}
	// memory mapped region written back to disk
	if err:= i.file.Sync();err!=nil{
		return err
	}
	// truncate file size to reflect mmap size
	if err := i.file.Truncate(int64(i.size));err!=nil{
		return err
	}
	return i.file.Close()
}