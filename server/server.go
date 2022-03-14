package server

import (
	"context"
	"fmt"
	"net"

	proto "go-redis-grpc/proto"
	"go-redis-grpc/redisdb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	log "github.com/sirupsen/logrus"
)

type server struct {
	proto.UnimplementedRedisGrpcServer
}

var db redisdb.RedisClient

func StartServer(redisaddress string, port uint) {

	p := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", p)
	if err != nil {
		panic(err) // The port may be on use
	}
	srv := grpc.NewServer()

	db, err = redisdb.CreateRedisClient(context.Background(), redisaddress)
	if err != nil {
		panic(err)
	}
	proto.RegisterRedisGrpcServer(srv, &server{})
	reflection.Register(srv)
	log.Println("Ready to serve port ", p)
	if e := srv.Serve(listener); e != nil {
		panic(err)
	}
}

/*
Define in pb.go

Command(context.Context, *CommandRequest) (*CommandResponse, error)
Subscribe(*SubscribeRequest, RedisGrpc_SubscribeServer) error
Publish(context.Context, *PublishRequest) (*IntResponse, error)
Keys(context.Context, *KeysRequest) (*StringListResponse, error)
Get(context.Context, *GetRequest) (*StringResponse, error)
Set(context.Context, *SetRequest) (*StringResponse, error)
Del(context.Context, *DelRequest) (*IntResponse, error)
Lpush(context.Context, *LPushRequest) (*IntResponse, error)
Rpush(context.Context, *RPushRequest) (*IntResponse, error)
*/

func (s *server) Command(ctx context.Context, in *proto.CommandRequest) (*proto.CommandResponse, error) {
	value, err := db.Command(ctx, in.GetCommand())
	if err != nil {
		log.Printf("command %s err :%v", in.GetCommand(), err)
		return &proto.CommandResponse{Message: &value}, err
	} else {
		log.Printf("command %s done :%v", in.GetCommand(), value)
		return &proto.CommandResponse{Message: &value}, nil
	}
}

func (s *server) Subscribe(in *proto.SubscribeRequest, sub proto.RedisGrpc_SubscribeServer) error {
	channel, msg := db.Subscribe(in.GetChannels()...)
	if channel != "" {
		log.Printf("subscribe %v done :%s|%s", in.GetChannels(), channel, msg)
		sub.Send(&proto.SubscribeResponse{Channel: channel, Message: msg})
	}
	return nil
}
func (s *server) Publish(ctx context.Context, in *proto.PublishRequest) (*proto.IntResponse, error) {
	value, err := db.Publish(ctx, in.GetChannel(), in.GetMessage())
	if err != nil {
		log.Printf("publish %s %s err :%v", in.GetChannel(), in.GetMessage(), err)
		return &proto.IntResponse{}, err
	} else {
		log.Printf("publish %s %s done :%v", in.GetChannel(), in.GetMessage(), value)
		return &proto.IntResponse{Result: value}, nil
	}
}

func (s *server) Keys(ctx context.Context, in *proto.KeysRequest) (*proto.StringListResponse, error) {
	value, err := db.Keys(ctx, in.GetPattern())
	if err != nil {
		log.Printf("keys %s err :%v", in.GetPattern(), err)
		return &proto.StringListResponse{}, err
	} else {
		log.Printf("keys %s done :%v", in.GetPattern(), value)
		return &proto.StringListResponse{Result: value}, nil
	}
}

func (s *server) Set(ctx context.Context, in *proto.SetRequest) (*proto.StringResponse, error) {
	value, err := db.Set(ctx, in.GetKey(), in.GetValue(), 0)
	if err != nil {
		log.Printf("set %s %s err :%v", in.GetKey(), in.GetValue(), err)
		return &proto.StringResponse{}, err
	} else {
		log.Printf("set %s %s done :%v", in.GetKey(), in.GetValue(), value)
		return &proto.StringResponse{Result: &value}, nil
	}
}
func (s *server) Get(ctx context.Context, in *proto.GetRequest) (*proto.StringResponse, error) {
	value, err := db.Get(ctx, in.GetKey())
	if err != nil {
		log.Printf("get %s err :%v", in.GetKey(), err)
		return &proto.StringResponse{}, err
	} else {
		log.Printf("get %s done :%v", in.GetKey(), value)
		return &proto.StringResponse{Result: &value}, nil
	}
}
func (s *server) Del(ctx context.Context, in *proto.DelRequest) (*proto.IntResponse, error) {
	value, err := db.Delete(ctx, in.GetKey())
	if err != nil {
		log.Printf("delete %s err :%v", in.GetKey(), err)
		return &proto.IntResponse{}, err
	} else {
		log.Printf("delete %s done :%v", in.GetKey(), value)
		return &proto.IntResponse{Result: value}, nil
	}
}

func (s *server) Lpush(ctx context.Context, in *proto.LPushRequest) (*proto.IntResponse, error) {
	value, err := db.Lpush(ctx, in.GetKey(), in.GetElement())
	if err != nil {
		log.Printf("lpush %s %s err :%v", in.GetKey(), in.GetElement(), err)
		return &proto.IntResponse{}, err
	} else {
		log.Printf("lpush %s %s done :%v", in.GetKey(), in.GetElement(), value)
		return &proto.IntResponse{Result: value}, nil
	}
}

func (s *server) Rpush(ctx context.Context, in *proto.RPushRequest) (*proto.IntResponse, error) {
	value, err := db.Rpush(ctx, in.GetKey(), in.GetElement())
	if err != nil {
		log.Printf("rpush %s %s err :%v", in.GetKey(), in.GetElement(), err)
		return &proto.IntResponse{}, err
	} else {
		log.Printf("rpush %s %s done :%v", in.GetKey(), in.GetElement(), value)
		return &proto.IntResponse{Result: value}, nil
	}
}
