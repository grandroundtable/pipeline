package pipeline

import "log"

// ErrConsumer represents a consumer of a single error.
type ErrConsumer interface {
	// Consume takes an error and returns true if error reading can continue.
	// If it cannot, Consume returns false. Calling Consume after the first call
	// which returns false will result in undefined behavior.
	Consume(error) bool
	// Err returns the current error state for the consumer.
	Err() error
}

// DefaultErrConsumer represents the default ErrConsumer implementation in errmux.
// With the default consumer, the first non-nil error encountered by the
// consumer will stop all error processing.
type DefaultErrConsumer struct {
	err  error
	done bool
}

// NewDefaultErrConsumer creates a new default consumer.
func NewDefaultErrConsumer() *DefaultErrConsumer {
	return &DefaultErrConsumer{}
}

// Consume sets the consumer error to the first non-nil error encountered and
// returns false.
func (c *DefaultErrConsumer) Consume(err error) bool {
	var ok bool
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

	return ok
}

// Err returns the first non-nil error consumed by DefaultErrConsumer, or nil if
// no error was found.
func (c *DefaultErrConsumer) Err() error {
	return c.err
}

// NilErrConsumer consumes and ignores all errors.
type NilErrConsumer struct{}

// NewNilErrConsumer creates a new nil error consumer.
func NewNilErrConsumer() *NilErrConsumer {
	return &NilErrConsumer{}
}

// Consume ignores all errors and returns true.
func (c *NilErrConsumer) Consume(err error) bool {
	return true
}

// Err returns nil.
func (c *NilErrConsumer) Err() error {
	return nil
}

// LoggingErrConsumer consumes any error and logs it in the given logger with
// an optional prefix.
type LoggingErrConsumer struct {
	logger *log.Logger
	prefix string
}

// NewLoggingErrConsumer creates a new logging consumer.
func NewLoggingErrConsumer(logger *log.Logger, prefix string) *LoggingErrConsumer {
	return &LoggingErrConsumer{
		logger: logger,
		prefix: prefix,
	}
}

// Consume logs the consumer error with the consumer prefix and returns true.
func (c *LoggingErrConsumer) Consume(err error) bool {
	if err == nil {
		return true
	}

	if len(c.prefix) > 0 {
		c.logger.Printf("%s %s", c.prefix, err.Error())
	} else {
		c.logger.Printf("%s", err.Error())
	}

	return true
}

// Err returns nil as no errors are stored.
func (c *LoggingErrConsumer) Err() error {
	return nil
}
