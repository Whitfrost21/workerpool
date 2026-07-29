package workerpool

import (
	"context"
	"log"
	"sync"
)

type Job struct {
	ID   string
	Task func(ctx context.Context) (any, error)
}

type Result struct {
	JobID string
	value any
	err   error
}

type Pool struct {
	jobs       chan Job
	results    chan Result
	numworkers int
	wg         sync.WaitGroup
	drainwg    sync.WaitGroup
}

func Newpool(numworkers, queuesize int) *Pool {
	return &Pool{
		jobs:       make(chan Job, queuesize),
		results:    make(chan Result, queuesize),
		numworkers: numworkers,
	}
}

func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.numworkers; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}
}

func (p *Pool) worker(ctx context.Context) {
	defer p.wg.Done()

	for {
		select {
		case job, ok := <-p.jobs:
			if !ok {
				return // no more jobs in channel
			}
			value, err := job.Task(ctx)
			p.results <- Result{JobID: job.ID, value: value, err: err}

		case <-ctx.Done():
			return
		}
	}
}

func (p *Pool) Submit(j Job) {
	p.jobs <- j
}

func (p *Pool) Shutdown() {
	close(p.jobs)
	p.wg.Wait()
	close(p.results)
}

func (p *Pool) Drain(cacheActor *CacheActor[string, any]) {
	p.drainwg.Add(1)
	go p.drain(cacheActor)
}

func (p *Pool) drain(cacheActor *CacheActor[string, any]) {
	defer p.drainwg.Done()

	for r := range p.results {
		if r.err != nil {
			log.Printf("job %s failed %v", r.JobID, r.err)
			continue
		}
		cacheActor.Set(r.JobID, r.value)
		log.Printf("job %s done: %v", r.JobID, r.value)
	}
}

func (p *Pool) WaitDrain() {
	p.drainwg.Wait()
}
