package errmux

// Consumer represents a consumer of a single error.
type Consumer interface {
	// Consume takes an error and returns true if error reading can continue.
	// If it cannot, Consume returns false. Calling Consume after the first call
	// which returns false will result in undefined behavior.
	Consume(error) bool
	// Err returns the current error state for the consumer.
	Err() error
}

// DefaultConsumer represents the default Consumer implementation in errmux.
// With the default consumer, the first non-nil error encountered by the
// consumer will stop all error processing.
type DefaultConsumer struct {
	err  error
	done bool
}

// NewDefaultConsumer creates a new default consumer.
func NewDefaultConsumer() *DefaultConsumer {
	return &DefaultConsumer{}
}

// Consume sets the consumer error to the first non-nil error encountered and
// returns false.
func (c *DefaultConsumer) Consume(err error) (ok bool) {
	switch {
	case c.done:
		ok = false
	case err != nil:
		c.err = err
		c.done = true
		ok = false
	default:
		ok = true
	}

	return
}

// Err returns the first non-nil error consumed by DefaultConsumer, or nil if
// no error was found.
func (c *DefaultConsumer) Err() error {
	return c.err
}
