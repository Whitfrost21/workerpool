package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/Whitfrost21/workerpool/lru"
	"github.com/Whitfrost21/workerpool/workerpool"
)

// sample task to pass for workers
func makeparseline(line string) func(ctx context.Context) (any, error) {
	return func(ctx context.Context) (any, error) {
		time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
		return fmt.Sprintf("parse(%s)", line), nil
	}
}

// hash the key
func hashline(line string) string {
	sum := sha256.Sum256([]byte(line))
	return hex.EncodeToString(sum[:])
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cache := lru.New[string, any](100)
	cacheActor := workerpool.NewCacheActor(cache)
	pool := workerpool.Newpool(4, 10)
	pool.Start(ctx)

	pool.Drain(cacheActor)
	// go pool.Drain()

	logLines := []string{
		"ERROR db timeout", "INFO user login", "WARN disk 80%",
		"INFO request served", "ERROR nil pointer", "DEBUG cache miss",
		"INFO user login", "INFO user login", "INFO user login",
	}

	for _, line := range logLines {

		key := hashline(line)

		if val, found := cacheActor.Get(key); found {
			log.Printf("cache line hit for %q:%v (skipping it)", line, val)
			continue
		}
		pool.Submit(workerpool.Job{ID: key, Task: makeparseline(line)})
		time.Sleep(1 * time.Second)
	}
	pool.Shutdown()
	// time.Sleep(1 * time.Second)
	pool.WaitDrain()
	log.Println("all jobs processed ")
}
