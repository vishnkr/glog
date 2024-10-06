package log

import (
	"fmt"
	api "glog/api/v1"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Log struct{
	Dir string
	Config Config
	activeSegment *segment
	segments []*segment
	mu sync.RWMutex
}

func NewLog(dir string, c Config)(*Log, error){
	if c.Segment.MaxStoreBytes == 0{
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0{
		c.Segment.MaxIndexBytes = 1024
	}

	l := &Log{Dir: dir,Config: c}
	return l,l.init()
}

func (l *Log) init() error{
	files, err := os.ReadDir(l.Dir)
	if err!=nil{ return err}
	var baseOffsets []uint64
	for _, f:= range files{
		offStr := strings.TrimSuffix(f.Name(),path.Ext(f.Name()))
		off, _ := strconv.ParseUint(offStr,10,0)
		baseOffsets = append(baseOffsets,off)
	}
	sort.Slice(baseOffsets,func (i,j int) bool {
		return baseOffsets[i]<baseOffsets[j]
	})

	for i:=0;i<len(baseOffsets);i++{
		if err = l.newSegment(baseOffsets[i]); err!=nil{
			return err
		}
		i+=1
	}
	if l.segments == nil{
		if err:=l.newSegment(l.Config.Segment.InitialOffset); err!=nil{
			return err
		}
	}
	return nil
	
}

func (l *Log) Append(record *api.Record)(uint64,error){
	l.mu.Lock()
	defer l.mu.Unlock()
	off, err:= l.activeSegment.Append(record)
	if err!=nil{
		return 0,err
	}
	if l.activeSegment.IsFull(){
		err = l.newSegment(off + 1)
	}
	return off, err
}

func (l *Log) Read(off uint64)(*api.Record,error){
	l.mu.RLock()
	defer l.mu.RUnlock()
	var s *segment
	for _,segment := range l.segments{
		if segment.baseOffset<= off && segment.nextOffset>off{
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset<=off{
		return nil, fmt.Errorf("offset out of bounds:%d",off)
	}
	return s.Read(off)
}

func (l *Log) Close() error{
	l.mu.Lock()
	defer l.mu.Unlock()
	for _,segment := range l.segments{
		if err:= segment.Close(); err!=nil{
			return err
		}
	}
	return nil
}

func (l *Log) Remove()error{
	if err := l.Close(); err!=nil{
		return err
	}
	return os.RemoveAll(l.Dir)
}

func (l *Log) ResetLog() error{
	if err := l.Remove(); err!=nil{
		return err
	}
	return l.init()
}

func (l *Log) newSegment(offset uint64) error{
	s, err := newSegment(l.Config,l.Dir,offset)
	if err!=nil{
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}