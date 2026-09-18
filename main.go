package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

type WorkloadDoc struct {
	ID        bson.ObjectID  `bson:"_id,omitempty"`
	TenantID  string         `bson:"tenant_id"`
	Status    string         `bson:"status"`
	Version   int            `bson:"version"`
	CreatedAt time.Time      `bson:"created_at"`
	UpdatedAt time.Time      `bson:"updated_at"`
	Payload   map[string]any `bson:"payload"`
	Padding   string         `bson:"padding,omitempty"`
}

// Thread-safe circular pool to store existing IDs for updates and deletes
type IDPool struct {
	sync.RWMutex
	ids   []bson.ObjectID
	maxSz int
}

func NewIDPool(maxSize int) *IDPool {
	return &IDPool{
		ids:   make([]bson.ObjectID, 0, maxSize),
		maxSz: maxSize,
	}
}

func (p *IDPool) Add(id bson.ObjectID) {
	p.Lock()
	defer p.Unlock()
	if len(p.ids) < p.maxSz {
		p.ids = append(p.ids, id)
	} else {
		// Overwrite a pseudo-random slot to keep the working set fresh
		idx := randomInt(0, p.maxSz-1)
		p.ids[idx] = id
	}
}

func (p *IDPool) GetRandom() (bson.ObjectID, bool) {
	p.RLock()
	defer p.RUnlock()
	if len(p.ids) == 0 {
		return bson.NilObjectID, false
	}
	idx := randomInt(0, len(p.ids)-1)
	return p.ids[idx], true
}

func (p *IDPool) PopRandom() (bson.ObjectID, bool) {
	p.Lock()
	defer p.Unlock()
	if len(p.ids) == 0 {
		return bson.NilObjectID, false
	}
	idx := randomInt(0, len(p.ids)-1)
	id := p.ids[idx]
	// Fast swap-delete
	p.ids[idx] = p.ids[len(p.ids)-1]
	p.ids = p.ids[:len(p.ids)-1]
	return id, true
}

var (
	totalInserts uint64
	totalUpdates uint64
	totalDeletes uint64
	totalErrors  uint64
)

func main() {
	mongoURI := flag.String("uri", os.Getenv("MONGO_URI"), "MongoDB Atlas or Firestore connection URI")
	dbName := flag.String("db", "workload_db", "Target database name")
	collName := flag.String("coll", "cdc_benchmark", "Target collection name")
	targetTPS := flag.Int("tps", 1500, "Target total operations per second")
	batchSize := flag.Int("batch", 50, "Batch size per BulkWrite call")
	numWorkers := flag.Int("workers", 16, "Number of concurrent worker goroutines")
	docSizeBytes := flag.Int("doc-size", 1024, "Payload padding size in bytes")

	// CRUD ratio configuration (defaults to 70% Insert, 20% Update, 10% Delete)
	insertPct := flag.Int("insert-pct", 70, "Percentage of insert operations (0-100)")
	updatePct := flag.Int("update-pct", 20, "Percentage of update operations (0-100)")
	deletePct := flag.Int("delete-pct", 10, "Percentage of delete operations (0-100)")
	flag.Parse()

	if *mongoURI == "" {
		log.Fatal("Fatal: Database URI must be set via -uri flag or MONGO_URI environment variable")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("\nShutdown signal caught. Draining active batches...")
		cancel()
	}()

	clientOpts := options.Client().
		ApplyURI(*mongoURI).
		SetMaxPoolSize(100).
		SetMinPoolSize(10).
		SetWriteConcern(writeconcern.Majority()).
		SetTimeout(5 * time.Second)

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		log.Fatalf("Database connection failed: %v", err)
	}
	defer func() {
		_ = client.Disconnect(context.Background())
	}()

	if err := client.Ping(ctx, nil); err != nil {
		log.Fatalf("Cluster ping failed: %v", err)
	}
	log.Printf("Connected successfully to cluster: %s/%s", *dbName, *collName)

	collection := client.Database(*dbName).Collection(*collName)
	paddingStr := strings.Repeat("x", *docSizeBytes)
	idPool := NewIDPool(50000) // Keeps up to 50k active document IDs in RAM

	taskChan := make(chan int, *numWorkers*4)
	var wg sync.WaitGroup

	for w := 1; w <= *numWorkers; w++ {
		wg.Add(1)
		go worker(ctx, &wg, collection, idPool, taskChan, paddingStr, *insertPct, *updatePct, *deletePct)
	}

	go metricsReporter(ctx, *targetTPS)

	batchesPerSec := *targetTPS / *batchSize
	if batchesPerSec < 1 {
		batchesPerSec = 1
	}
	ticker := time.NewTicker(time.Second / time.Duration(batchesPerSec))
	defer ticker.Stop()

	log.Printf("CRUD Generator running: %d TPS (Ratios: %d%% Insert / %d%% Update / %d%% Delete)",
		*targetTPS, *insertPct, *updatePct, *deletePct)

generatorLoop:
	for {
		select {
		case <-ctx.Done():
			break generatorLoop
		case <-ticker.C:
			select {
			case taskChan <- *batchSize:
			default:
				atomic.AddUint64(&totalErrors, 1)
			}
		}
	}

	close(taskChan)
	wg.Wait()
	log.Printf("Stopped. Completed: [Inserts: %d | Updates: %d | Deletes: %d]",
		atomic.LoadUint64(&totalInserts), atomic.LoadUint64(&totalUpdates), atomic.LoadUint64(&totalDeletes))
}

func worker(ctx context.Context, wg *sync.WaitGroup, coll *mongo.Collection, pool *IDPool, taskChan <-chan int, padding string, insertPct, updatePct, deletePct int) {
	defer wg.Done()
	bulkOpts := options.BulkWrite().SetOrdered(false)

	for count := range taskChan {
		models := make([]mongo.WriteModel, 0, count)
		newIDs := make([]bson.ObjectID, 0, count)

		for i := 0; i < count; i++ {
			roll := randomInt(1, 100)
			now := time.Now().UTC()

			switch {
			// Case 1: UPDATE
			case roll > insertPct && roll <= (insertPct+updatePct):
				if id, ok := pool.GetRandom(); ok {
					// Modifying status, incrementing version, and altering payload to test updateDescription
					updateDoc := bson.D{
						{Key: "$set", Value: bson.D{
							{Key: "status", Value: "PROCESSED"},
							{Key: "updated_at", Value: now},
							{Key: "payload.metrics", Value: []float64{99.9, 100.0}},
							{Key: "payload.last_mutation", Value: "cdc_update_event"},
						}},
						{Key: "$inc", Value: bson.D{
							{Key: "version", Value: 1},
						}},
					}
					models = append(models, mongo.NewUpdateOneModel().SetFilter(bson.D{{Key: "_id", Value: id}}).SetUpdate(updateDoc))
					continue
				}
				fallthrough // Pool empty, fallback to Insert

			// Case 2: DELETE
			case roll > (insertPct + updatePct):
				if id, ok := pool.PopRandom(); ok {
					models = append(models, mongo.NewDeleteOneModel().SetFilter(bson.D{{Key: "_id", Value: id}}))
					continue
				}
				fallthrough // Pool empty, fallback to Insert

			// Case 3: INSERT (Default)
			default:
				id := bson.NewObjectID()
				doc := WorkloadDoc{
					ID:        id,
					TenantID:  fmt.Sprintf("tenant_%d", randomInt(1, 100)),
					Status:    "PENDING",
					Version:   1,
					CreatedAt: now,
					UpdatedAt: now,
					Payload: map[string]any{
						"source":     "poc-workload-generator",
						"request_id": randomHexString(8),
						"metrics":    []float64{10.5, 42.0, 98.7},
					},
					Padding: padding,
				}
				models = append(models, mongo.NewInsertOneModel().SetDocument(doc))
				newIDs = append(newIDs, id)
			}
		}

		if len(models) == 0 {
			continue
		}

		res, err := coll.BulkWrite(ctx, models, bulkOpts)
		if err != nil {
			atomic.AddUint64(&totalErrors, uint64(len(models)))
		} else {
			// Populate the ID pool with newly confirmed inserts
			for _, id := range newIDs {
				pool.Add(id)
			}
			atomic.AddUint64(&totalInserts, uint64(res.InsertedCount))
			atomic.AddUint64(&totalUpdates, uint64(res.ModifiedCount))
			atomic.AddUint64(&totalDeletes, uint64(res.DeletedCount))
		}
	}
}

func metricsReporter(ctx context.Context, targetTPS int) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var prevInserts, prevUpdates, prevDeletes uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currInserts := atomic.LoadUint64(&totalInserts)
			currUpdates := atomic.LoadUint64(&totalUpdates)
			currDeletes := atomic.LoadUint64(&totalDeletes)
			errors := atomic.LoadUint64(&totalErrors)

			insRate := currInserts - prevInserts
			updRate := currUpdates - prevUpdates
			delRate := currDeletes - prevDeletes
			totalRate := insRate + updRate + delRate

			prevInserts, prevUpdates, prevDeletes = currInserts, currUpdates, currDeletes

			log.Printf("[CRUD METRICS] Total: %5d ops/s (Ins: %4d | Upd: %4d | Del: %3d) | Cumulative: %d | Errors: %d",
				totalRate, insRate, updRate, delRate, currInserts+currUpdates+currDeletes, errors)
		}
	}
}

func randomHexString(n int) string {
	bytes := make([]byte, n)
	_, _ = rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

func randomInt(min, max int) int {
	if min >= max {
		return min
	}
	nBig, err := rand.Int(rand.Reader, big.NewInt(int64(max-min+1)))
	if err != nil {
		return min
	}
	return int(nBig.Int64()) + min
}
