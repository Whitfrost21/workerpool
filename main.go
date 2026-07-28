package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/Whitfrost21/workerpool/lru"
	"github.com/Whitfrost21/workerpool/workerpool"
)

func makeparseline(line string) func(ctx context.Context) (any, error) {
	return func(ctx context.Context) (any, error) {
		time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
		return fmt.Sprintf("parse(%s)", line), nil
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cache := lru.New[string, any](100)
	cacheActor := workerpool.NewCacheActor[string, any](cache)
	pool := workerpool.Newpool(4, 10)
	pool.Start(ctx)

	pool.Drain()
	// go pool.Drain()

	logLines := []string{
		"ERROR db timeout", "INFO user login", "WARN disk 80%",
		"INFO request served", "ERROR nil pointer", "DEBUG cache miss",
	}

	for i, line := range logLines {
		pool.Submit(workerpool.Job{ID: i, Task: makeparseline(line)})
	}
	pool.Shutdown()
	// time.Sleep(1 * time.Second)
	pool.WaitDrain()
	log.Println("all jobs processed ")
}
