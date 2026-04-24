package main

import (
	"context"
	"io"
	"log"
	"os"
	"time"

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

	logFile, err := os.OpenFile("market_stream.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
    if err != nil {
        log.Fatalf("Failed to open log file: %v", err)
    }
    defer logFile.Close()

	multiWriter := io.MultiWriter(os.Stdout, logFile)
    log.SetOutput(multiWriter)

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

		now := time.Now()
		log.Printf("--- Current %d ---", now.UnixMilli() )
		for _, m := range msg.Data {
			log.Printf("[%s] OBI: %s | Updated: %s", m.Symbol, m.Obi, m.Update)
		}
	}
}