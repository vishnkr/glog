package server

import (
	"context"
	api "glog/api/v1"
	"io"

	"google.golang.org/grpc"
)

type CommitLog interface{
	Append(*api.Record) (uint64,error)
	Read(uint64) (*api.Record,error)
}

type Config struct{
	CommitLog CommitLog
}

type grpcServer struct{
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (s *grpcServer, err error){
	s = &grpcServer{Config:config}
	return s,nil
}
var _ api.LogServer = (*grpcServer)(nil)


func NewGRPCServer(config *Config) (*grpc.Server,error){
	gs := grpc.NewServer()
	s,err := newgrpcServer(config)
	if err!=nil{ return nil,err}
	api.RegisterLogServer(gs,s)
	return gs,nil

}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest)(*api.ProduceResponse,error){
	offset, err:= s.CommitLog.Append(req.Record)
	if err!=nil{
		return nil,err
	}
	return &api.ProduceResponse{Offset:offset},nil
}

func (s *grpcServer) Consume(ctx context.Context,req *api.ConsumeRequest)(*api.ConsumeResponse,error){
	record, err:= s.CommitLog.Read(req.Offset)
	if err!=nil{
		return nil,err
	}
	return &api.ConsumeResponse{Record:record},nil
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error{
	for{
		req, err:= stream.Recv()
		if err!=nil{
			return err
		}
		res, err:= s.Produce(stream.Context(),req)
		if err!=nil{
			return err
		}
		if err = stream.Send(res);err!=nil{
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(stream api.Log_ConsumeStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		res, err := s.Consume(stream.Context(), req)
		if err != nil {
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
		}

		if err := stream.Send(res); err != nil {
			return err
		}
		req.Offset += 1
	}
}