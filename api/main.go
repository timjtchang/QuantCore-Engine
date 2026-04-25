package main

import (
	"encoding/json"
	"log"
	"net" // don't forget to import this
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

type MarketTick struct {
    Symbol string `json:"symbol"`
    Obi    string `json:"obi"`
    Update string `json:"update"`
	ProcessTs string `json:"process_ts"`
}

// The Streaming Implementation
func (s *server) SubscribeToMetrics(req *pb.SubscribeRequest, stream pb.MarketDataService_SubscribeToMetricsServer) error {
    log.Println("🎧 New Client Connected! Starting Redis Pub/Sub stream...")

    pubsub := s.rdb.Subscribe(stream.Context(), "market_updates_channel")
    defer pubsub.Close()

    ch := pubsub.Channel()

    for msg := range ch {
        // 1. Unmarshal directly into the struct
		var tick MarketTick
		if err := json.Unmarshal([]byte(msg.Payload), &tick); err != nil {
			log.Printf("⚠️ Failed to parse payload: %v", err)
			continue 
		}

		// 2. Map it to the gRPC Proto format
		// Since we only get one symbol per Pub/Sub message, the array only has 1 item
		metrics := []*pb.Metric{
			{
				Symbol:    tick.Symbol,
				Obi:       tick.Obi,
				Update:    tick.Update,
				ProcessTs: tick.ProcessTs,
			},
		}

        response := &pb.MarketUpdate{
            Timestamp: time.Now().UnixMilli(),
            Data:      metrics,
        }

        if err := stream.Send(response); err != nil {
            log.Printf("❌ Client Disconnected or Network Error: %v", err)
            return err 
        }
    }

    return nil
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