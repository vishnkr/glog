package log

import (
	"fmt"
	api "glog/api/v1"
	"os"
	"path"

	"google.golang.org/protobuf/proto"
)
type segment struct{
	store *store
	index *index
	baseOffset uint64
	nextOffset uint64
	config Config
}


func newSegment(config Config,dir string,baseOffset uint64) (*segment,error){
	var err error	
	storeFile,err := createNewFile(path.Join(dir,fmt.Sprintf("%d%s",baseOffset,".st")))
	if err!=nil{return nil,err}
	var store *store
	if store, err = newStore(storeFile);err!=nil{
		return nil,err
	}

	var index *index
	indexFile,err := createNewFile(path.Join(dir,fmt.Sprintf("%d%s",baseOffset,".idx")))
	if err!=nil{return nil,err}
	if index,err = newIndex(indexFile,config);err!=nil{
		return nil,err
	}
	var nextOffset uint64
	// empty? start from base otherwise last entry offset+1
	if off,_,err := index.Read(-1);err!=nil{
		nextOffset = baseOffset
	} else { nextOffset = baseOffset + uint64(off)+1}
	return &segment{
		store,
		index,
		baseOffset,
		nextOffset,
		config,
	},nil
}

func (s *segment) Append(record *api.Record)(offset uint64, err error){

	current := s.nextOffset
	record.Offset = current
	p, err := proto.Marshal(record)
	if err!=nil{return 0,err}
	_,pos,err := s.store.Append(p)
	if err!=nil{return 0,err}
	if err = s.index.Write(uint32(s.nextOffset)-uint32(s.baseOffset),pos);err!=nil{
		return 0,err
	}
	s.nextOffset+=1
	return current,nil
}

func (s *segment) IsFull() bool{
	return s.index.size>=s.config.Segment.MaxIndexBytes || s.store.size>=s.config.Segment.MaxStoreBytes
}

func (s *segment) Clear() error{
	if err:= s.Close();err!=nil{ return err}
	if err := os.Remove(s.index.Name());err!=nil{return err}
	if err:= os.Remove(s.store.Name());err!=nil{return err}
	return nil
}

func (s *segment) Close() error{
	if err:= s.index.Close(); err!=nil {return err}
	if err:= s.store.Close(); err!=nil {return err}
	return nil
}

func (s *segment) Read(offset uint64)(*api.Record,error){
	_, pos, err := s.index.Read(int64(offset)-int64(s.baseOffset))
	if err!=nil{
		return nil,err
	}
	p,err := s.store.Read(pos)
	if err!=nil{
		return nil,err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p,record)
	return record, err
}

func createNewFile(filePath string)(*os.File,error){
	f,err := os.OpenFile(filePath,
			os.O_RDWR|os.O_CREATE|os.O_APPEND,
			0644,
	)
	if err!=nil{
		return nil,err
	}
	return f,nil
}