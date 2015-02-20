// Package errmux provides functions and data for handling errors in a concurrent environment.
package errmux

import "sync"

// mergeErrors merges a slice of error channels into one, terminating early if q is unblocked.
func mergeErrors(q <-chan struct{}, errs []<-chan error) <-chan error {
	var wg sync.WaitGroup
	out := make(chan error, len(errs))

	wg.Add(len(errs))

	// For each error channel, start a separate error handling goroutine.
	for _, e := range errs {
		// Make sure to close over our channel.
		go func(ch <-chan error) {
			defer wg.Done()

			for err := range ch {
				select {
				case <-q:
					return
				default:
				}
				out <- err
			}
		}(e)
	}

	// Wait for all error handlers to finish and close.
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// Handler represents a handler for multiple concurrent error streams.
type Handler struct {
	c     Consumer
	errs  <-chan error
	q     chan struct{}
	t     chan struct{}
	tOnce sync.Once
}

// NewHandler creates a handler with the provided consumer and error channels.
// All values are guaranteed to be read from any given error channel until it is closed
// or the handler is canceled.
func NewHandler(c Consumer, errs ...<-chan error) *Handler {
	q := make(chan struct{})
	errOut := mergeErrors(q, errs)
	t := make(chan struct{}, 1)

	h := &Handler{
		c:    c,
		errs: errOut,
		q:    q,
		t:    t,
	}

	go h.start()

	return h
}

// start begins error processing, terminating early if the consumer's Consume
// method returns false.
func (h *Handler) start() {
	done := false
	// iterate over the entire range of errors
	for err := range h.errs {
		// ensure we only get a false result from Consume one time
		if !done {
			if ok := h.c.Consume(err); !ok {
				h.Cancel()
				done = true
			}
		}
	}

	h.Cancel()
}

// Wait blocks until error processing is finished. After Wait returns, no more
// new values passed into the handler will be consumed. They may, however,
// be read by the handler and discarded.
func (h *Handler) Wait() {
	<-h.q
}

// Err blocks until error processing is finished and then returns the consumer's
// final error before termination. All rules of Wait apply to Err.
func (h *Handler) Err() error {
	h.Wait()

	return h.c.Err()
}

// ErrChan returns a channel that returns the value of Err when it is available.
func (h *Handler) ErrChan() <-chan error {
	e := make(chan error, 1)
	go func() {
		defer close(e)
		e <- h.Err()
	}()

	return e
}

// Cancel terminates error handling. If the handler is already canceled or finished
// handling errors, Cancel returns false. Otherwise, Cancel blocks until the cancel
// operation is finished.
func (h *Handler) Cancel() bool {
	ok := false
	h.tOnce.Do(func() {
		close(h.q)
		ok = true
	})

	return ok
}
