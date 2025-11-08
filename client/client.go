package main

import (
	"fmt"
	"log"
	"context"
	"time"

	pb "github.com/Mohammed-Shammout/key_value/key_value_proto/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	conn,err :=grpc.NewClient("localhost:9001",grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err!=nil {
		log.Fatalf("Client: Failed to connect to gRPC server")
	}
	defer conn.Close()

	c:= pb.NewKeyValueServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
	defer cancel()

	
	log.Printf("Enter your commands to execute:")
	var CMD byte
	var key int64
	var value int64

	for{
		value=0
		key=0
		fmt.Scanf("%c %d %d", &CMD, &key, &value) 
		log.Printf("Got command %c key=%d val=%d",CMD,key,value)

		switch CMD {
		case 'C','c': // create
			res, err := c.Create(ctx,&pb.Key_Value{
				Key: key,
				Val: value,
			})
			if(err!=nil){
				log.Fatalf("Client: Create returned %v",err)
			}
			if(res.Success){
				log.Printf("Client: Create Key=%d succeeded",key)	
			} else {
				log.Printf("Client: Create Key=%d failed",key)
			}	
		case 'R','r': // read
			res, err := c.Read(ctx,&pb.Key{
				Key: key,
			})
			if(err!=nil){
				log.Fatalf("Client: Read returned %v",err)
			}
			if(res.Success){
				log.Printf("Client: Read Key=%d succeeded with Value=%d",key,res.Val)	
			} else {
				log.Printf("Client: Read Key=%d failed",key)
			}
		case 'U','u': // update
			res, err := c.Update(ctx,&pb.Key_Value{
				Key: key,
				Val: value,
			})
			if(err!=nil){
				log.Fatalf("Client: Update returned %v",err)
			}
			if(res.Success){
				log.Printf("Client: Update Key=%d succeeded",key)
			} else {
				log.Printf("Client: Update Key=%d failed",key)
			}	
		case 'D','d': // delete
			res, err := c.Delete(ctx,&pb.Key{
				Key: key,
			})	
			if(err!=nil){
				log.Fatalf("Client: Delete returned %v",err)
			}
			if(res.Success){
				log.Printf("Client: Delete Key=%d succeeded",key)
			} else {
				log.Printf("Client: Delete Key=%d failed",key)
			}
		case 'E','e': // exit
			log.Printf("Client: Exiting...")
			return
		default:
			log.Printf("Client: Unknown Command %c",CMD)
		}
	}


}