// Package pipeline provides functions and data for concurrently processing
// data.
package pipeline

import (
	"sync"

	"github.com/jnschaeffer/pipeline/errmux"
)

// Job represents the behavior for a job that is run with multiple workers.
type Job interface {
	// Close terminates the job early. Multiple calls to Close will result in
	// undefined behavior.
	Close()
	// NewWorker creates a new Worker and a receive-only error channel for
	// error values. The error channel must be closed to avoid deadlocks or
	// leaks.
	NewWorker() (Worker, <-chan error)
}

// Worker represents the processing logic for a job.
type Worker interface {
	// Close terminates the worker. Multiple calls to Close will result in
	// undefined behavior.
	Close()
	// Next executes the next task for the worker. If there was a task to
	// execute, Next returns true. Otherwise, it returns false.
	Next() bool
}

// Run runs the given job with n workers, using the given consumer for error
// handling logic. The returned handler will process all errors from all
// workers.
func Run(job Job, c errmux.Consumer, n int) *errmux.Handler {
	var wg sync.WaitGroup

	// Make n pairs of workers and error channels.
	ws := make([]Worker, n)
	errs := make([]<-chan error, n)

	wg.Add(n)
	for i := 0; i < n; i++ {
		w, e := job.NewWorker()
		ws[i] = w
		errs[i] = e
	}

	// Create a new handler with the given consumer and error channels.
	h := errmux.NewHandler(c, errs...)

	// For each worker, iterate over each task until either the handler
	// yields an error or there are no tasks left to execute.
	for _, w := range ws {
		go func(v Worker) {
			defer wg.Done()
			defer v.Close()

			ok := true
			e := h.ErrChan()

			for ok {
				select {
				case <-e:
					return
				default:
				}

				ok = v.Next()
			}
		}(w)
	}

	// Wait until all workers are finished and then close the job.
	go func() {
		wg.Wait()
		job.Close()
	}()

	return h
}
