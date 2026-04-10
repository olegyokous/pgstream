package wal

import (
	"context"
	"errors"
	"sync"
)

// ErrPoolClosed is returned when operations are attempted on a closed pool.
var ErrPoolClosed = errors.New("worker pool is closed")

// Task is a unit of work submitted to the pool.
type Task func(ctx context.Context) error

// PoolConfig holds configuration for the worker pool.
type PoolConfig struct {
	Workers    int
	QueueDepth int
}

// DefaultPoolConfig returns sensible defaults.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		Workers:    4,
		QueueDepth: 64,
	}
}

// Pool manages a fixed set of goroutines consuming tasks from a shared queue.
type Pool struct {
	cfg    PoolConfig
	queue  chan Task
	wg     sync.WaitGroup
	once   sync.Once
	closed chan struct{}
	Errors chan error
}

// NewPool creates and starts a Pool with the given config.
func NewPool(cfg PoolConfig) *Pool {
	if cfg.Workers <= 0 {
		cfg.Workers = DefaultPoolConfig().Workers
	}
	if cfg.QueueDepth <= 0 {
		cfg.QueueDepth = DefaultPoolConfig().QueueDepth
	}
	p := &Pool{
		cfg:    cfg,
		queue:  make(chan Task, cfg.QueueDepth),
		closed: make(chan struct{}),
		Errors: make(chan error, cfg.QueueDepth),
	}
	for i := 0; i < cfg.Workers; i++ {
		p.wg.Add(1)
		go p.run()
	}
	return p
}

// Submit enqueues a task for execution. Returns ErrPoolClosed if the pool is stopped.
func (p *Pool) Submit(ctx context.Context, t Task) error {
	select {
	case <-p.closed:
		return ErrPoolClosed
	case <-ctx.Done():
		return ctx.Err()
	case p.queue <- t:
		return nil
	}
}

// Close stops accepting new tasks and waits for all workers to finish.
func (p *Pool) Close() {
	p.once.Do(func() {
		close(p.closed)
		close(p.queue)
		p.wg.Wait()
		close(p.Errors)
	})
}

func (p *Pool) run() {
	defer p.wg.Done()
	for task := range p.queue {
		if err := task(context.Background()); err != nil {
			select {
			case p.Errors <- err:
			default:
			}
		}
	}
}
