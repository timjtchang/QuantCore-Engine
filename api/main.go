package main

import (
	"context"
	"log"
	"net"
	"strings" // don't forget to import this
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	// Import the generated code
	pb "github.com/timjtchang/quantcore/proto"
)

const (
	REDIS_ADDR = "localhost:6379"
	PORT       = ":50051"
)

// Server struct
type server struct {
	pb.UnimplementedMarketDataServiceServer
	rdb *redis.Client
}

// The Streaming Implementation
func (s *server) SubscribeToMetrics(req *pb.SubscribeRequest, stream pb.MarketDataService_SubscribeToMetricsServer) error {
	log.Println("🎧 New Client Connected to Stream!")

	// Loop forever (or until client disconnects)
	for {
		// 1. Fetch Data from Redis
		result, err := s.rdb.HGetAll(context.Background(), "market_metrics").Result()
		if err != nil {
			log.Printf("Redis Error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// 2. Convert to Proto format
		var metrics []*pb.Metric
		for sym, rawVal := range result {
			parts := strings.Split(rawVal, ":")
			
			// SAFETY CHECK: Prevent index out of bounds panic
			if len(parts) != 3 {
				log.Printf("⚠️ Warning: Malformed data for %s: %s", sym, rawVal)
				continue 
			}

			print(parts)

			metrics = append(metrics, &pb.Metric{
				Symbol: sym,
				Obi:    parts[0],
				Update: parts[1],
				ProcessTs: parts[2],
			})
		}

		// 3. Send the message down the stream
		response := &pb.MarketUpdate{
			Timestamp: time.Now().UnixMilli(),
			Data:      metrics,
		}

		if err := stream.Send(response); err != nil {
			log.Printf("❌ Client Disconnected: %v", err)
			return err
		}

		// 4. Wait before next push (Simulate Ticker)
		time.Sleep(500 * time.Millisecond)
	}
}

func main() {
	// 1. Setup Redis
	rdb := redis.NewClient(&redis.Options{Addr: REDIS_ADDR})

	// 2. Setup TCP Listener
	lis, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// 3. Start gRPC Server
	s := grpc.NewServer()
	pb.RegisterMarketDataServiceServer(s, &server{rdb: rdb})

	log.Printf("🚀 gRPC Market Data Server listening on %s", PORT)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}