package main

import (
	"context"
	"io"
	"log"

	pb "github.com/timjtchang/quantcore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// 1. Connect to gRPC Server
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	
	client := pb.NewMarketDataServiceClient(conn)

	// 2. Open the Stream
	stream, err := client.SubscribeToMetrics(context.Background(), &pb.SubscribeRequest{})
	if err != nil {
		log.Fatalf("Error opening stream: %v", err)
	}

	log.Println("✅ Connected to Stream. Waiting for Market Data...")

	// 3. Listen for updates
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Stream error: %v", err)
		}

		log.Printf("--- Update at %d ---", msg.Timestamp)
		for _, m := range msg.Data {
			log.Printf("[%s] OBI: %s", m.Symbol, m.Obi)
		}
	}
}