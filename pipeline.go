// Package pipeline provides functions and data for concurrently processing
// data.
package pipeline

import (
	"log"
	"sync"

	"github.com/grandroundtable/pipeline/errmux"
)

// WorkerPool represents the behavior for a pool of workers.
type WorkerPool interface {
	// Close frees the pool resources. Multiple calls to Close will result in
	// undefined behavior. Close is guaranteed to be called after all workers
	// in the pool are closed.
	Close() error
	// NewWorker creates a new Worker for the pool.
	NewWorker() Worker
}

// Worker represents the processing logic for a pool.
type Worker interface {
	// Close terminates the worker. Multiple calls to Close will result in
	// undefined behavior. Close is guaranteed to be called after the job has
	// been canceled or finished.
	Close() error
	// Next executes the next task for the worker. If the worker is able to
	// continue, Next returns true. Otherwise, Next returns false.
	Next() (bool, error)
}

// Job represents the logic for running and terminating a job with multiple
// workers.
type Job struct {
	pool WorkerPool
	q    chan struct{}
}

// NewJob creates a new job with the given pool.
func NewJob(p WorkerPool) *Job {
	q := make(chan struct{})

	return &Job{
		pool: p,
		q:    q,
	}
}

func (j *Job) Close() error {
	close(j.q)

	return nil
}

// Run runs the given job with n workers, using the given consumer for error
// handling logic. The returned handler will process all errors from all
// workers.
func (j *Job) Run(c errmux.Consumer, n int) *errmux.Handler {
	var wg sync.WaitGroup

	// Make n pairs of workers and error channels.
	ws := make([]Worker, n)
	errOuts := make([]<-chan error, n)
	errIns := make([]chan<- error, n)

	wg.Add(n)
	for i := 0; i < n; i++ {
		w := j.pool.NewWorker()
		ws[i] = w
		ch := make(chan error, 1)
		errOuts[i] = ch
		errIns[i] = ch
	}

	poolErrs := make(chan error, 1)
	errOuts, errIns = append(errOuts, poolErrs), append(errIns, poolErrs)

	// Create a new handler with the given consumer and error channels.
	h := errmux.NewHandler(c, errOuts...)

	// Cancel the job if the handler reports an error.
	go func() {
		if _, ok := <-h.ErrChan(); !ok {
			j.Close()
		}
	}()

	// For each worker, iterate over each task until either the handler
	// yields an error or there are no tasks left to execute.
	for i := range ws {
		go func(v Worker, e chan<- error) {
			defer func() {
				e <- v.Close()
				close(e)
				wg.Done()
			}()

			ok := true

			var err error
			for ok {
				select {
				case <-j.q:
					return
				default:
				}

				ok, err = v.Next()

				e <- err
			}
			log.Print("succcess")
		}(ws[i], errIns[i])
	}

	// Wait until all workers are finished and then close the pool.
	go func() {
		wg.Wait()
		poolErrs <- j.pool.Close()
		close(poolErrs)
	}()

	return h
}
