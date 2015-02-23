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
	Close() error
	// NewWorker creates a new Worker for the job.
	NewWorker() Worker
}

// Worker represents the processing logic for a job.
type Worker interface {
	// Close terminates the worker. Multiple calls to Close will result in
	// undefined behavior.
	Close() error
	// Next executes the next task for the worker. If the worker is able to
	// continue, Next returns true. Otherwise, Next returns false.
	Next() (bool, error)
}

// Run runs the given job with n workers, using the given consumer for error
// handling logic. The returned handler will process all errors from all
// workers.
func Run(job Job, c errmux.Consumer, n int) *errmux.Handler {
	var wg sync.WaitGroup

	// Make n pairs of workers and error channels.
	ws := make([]Worker, n)
	errOuts := make([]<-chan error, n)
	errIns := make([]chan<- error, n)

	wg.Add(n)
	for i := 0; i < n; i++ {
		w := job.NewWorker()
		ws[i] = w
		ch := make(chan error, 1)
		errOuts[i] = ch
		errIns[i] = ch
	}

	jobErrs := make(chan error, 1)
	errOuts, errIns = append(errOuts, jobErrs), append(errIns, jobErrs)

	// Create a new handler with the given consumer and error channels.
	h := errmux.NewHandler(c, errOuts...)

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
			ch := h.ErrChan()

			var err error
			for ok {
				select {
				case <-ch:
					return
				default:
				}

				ok, err = v.Next()
				e <- err
			}
		}(ws[i], errIns[i])
	}

	// Wait until all workers are finished and then close the job.
	go func() {
		wg.Wait()
		jobErrs <- job.Close()
		close(jobErrs)
	}()

	return h
}
