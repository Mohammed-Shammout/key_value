package main

import (
	"context"
	"log"
	"net"

	pb "github.com/Mohammed-Shammout/key_value/key_value_proto/proto"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedKeyValueServiceServer
}

var m map[int64]int64

func (s *server) Create(ctx context.Context, key_val *pb.Key_Value) (*pb.Response, error) {
	log.Printf("Server: Create Key=%d Val=%d",key_val.Key,key_val.Val)
	_, ok := m[key_val.Key]
	log.Printf("Server: Create Key exists=%v",ok)
	if(ok){
		return &pb.Response{
			Success: false,
		}, nil
	}
	m[key_val.Key] = key_val.Val

	return &pb.Response{
		Success: true,
	}, nil
}
func (s *server) Update(ctx context.Context, key_val *pb.Key_Value) (*pb.Response, error) {
	log.Printf("Server: Update Key=%d Val=%d",key_val.Key,key_val.Val)
	_, ok := m[key_val.Key]
	log.Printf("Server: Update Key exists=%v",ok)
	if(!ok){
		return &pb.Response{
			Success: false,
		}, nil
	}
	m[key_val.Key] = key_val.Val
	return &pb.Response{
		Success: true,
	}, nil
}
func (s *server) Read(ctx context.Context, key *pb.Key) (*pb.Value_Response, error) {
	log.Printf("Server: Read Key=%d",key.Key)
	val , ok := m[key.Key]
	log.Printf("Server: Read Key exists=%v",ok)	
	if(!ok){
		return &pb.Value_Response{
			Success: false,
			Val: 0,
		}, nil
	}
	
	return &pb.Value_Response{
		Success: true,
		Val: val,
	}, nil
}
func (s *server) Delete(ctx context.Context, key *pb.Key) (*pb.Response, error) {
	log.Printf("Server: Delete Key=%d",key.Key)
	_, ok := m[key.Key]
	log.Printf("Server: Delete Key exists=%v",ok)	
	if(!ok){
		return &pb.Response{
			Success: false,
		}, nil
	}
	delete(m, key.Key)
	return &pb.Response{
		Success: true,
	}, nil
}


func main() {
	m = make(map[int64]int64)

	lis, err := net.Listen("tcp",":9001")
	if(err != nil){
		log.Fatalf("Server: Failed to Listen %v",err)
	}
	grpc_server := grpc.NewServer()
	pb.RegisterKeyValueServiceServer(grpc_server, &server{})
	log.Printf("Server: Started new gRPC Server")
	if err := grpc_server.Serve(lis); err != nil{
		log.Fatalf("Server: Failed to Serve %v",err)
	}
}