package main

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	uri := "mongodb+srv://spg123450_db_user:bfg7QkwpI6HcN2lI@cluster0.r5y45bf.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
	ctx := context.Background()

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf("Connection failed: %v", err)
	}
	defer client.Disconnect(ctx)

	coll := client.Database("workload_db").Collection("cdc_benchmark")

	stream, err := coll.Watch(ctx, mongo.Pipeline{})
	if err != nil {
		log.Fatalf("Watch stream error: %v", err)
	}
	defer stream.Close(ctx)

	fmt.Println("Listening for live CDC (Insert, Update, Delete)... (Press Ctrl+C to stop)")
	for stream.Next(ctx) {
		var event bson.M
		if err := stream.Decode(&event); err != nil {
			log.Printf("Decode error: %v", err)
			continue
		}

		op := event["operationType"]
		docKey := event["documentKey"]

		switch op {
		case "insert":
			fmt.Printf("[INSERT] ID: %v\n", docKey)
		case "update":
			// updateDescription reveals what changed without sending the entire document
			updDesc := event["updateDescription"]
			fmt.Printf("[UPDATE] ID: %v | Delta: %v\n", docKey, updDesc)
		case "delete":
			fmt.Printf("[DELETE] ID: %v\n", docKey)
		default:
			fmt.Printf("[%v] ID: %v\n", op, docKey)
		}
	}
}
