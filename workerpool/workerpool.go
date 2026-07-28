package workerpool

import (
	"context"
	"log"
	"sync"
)

type Job struct {
	ID   int
	Task func(ctx context.Context) (any, error)
}

type Result struct {
	JobID int
	Value any
	Err   error
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
			p.results <- Result{JobID: job.ID, Value: value, Err: err}

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

func (p *Pool) Drain() {
	p.drainwg.Add(1)
	go p.drain()
}

func (p *Pool) drain() {
	defer p.drainwg.Done()

	for r := range p.results {
		if r.Err != nil {
			log.Printf("job %d failed %v", r.JobID, r.Err)
			continue
		}
		log.Printf("job %d done: %v", r.JobID, r.Value)
	}
}

func (p *Pool) WaitDrain() {
	p.drainwg.Wait()
}
