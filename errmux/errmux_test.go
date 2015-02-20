package errmux

import (
	"fmt"
	"testing"
)

type mapConsumer map[error]struct{}

func (m mapConsumer) Consume(err error) bool {
	m[err] = struct{}{}

	return true
}

func (m mapConsumer) Err() error {
	return nil
}

func TestRangeAll(t *testing.T) {
	for i := 0; i < 100; i++ {
		testRangeAll(t)
	}
}

func testRangeAll(t *testing.T) {
	n := 3
	errs := make([]<-chan error, n)
	exp := map[error]bool{}
	for i := range errs {
		ch := make(chan error, 1)
		err := fmt.Errorf("%d", i)
		ch <- err
		close(ch)
		errs[i] = ch
		exp[err] = false
	}

	m := mapConsumer{}

	h := NewHandler(m, errs...)
	h.Wait()

	for k := range m {
		exp[k] = true
	}

	for k := range m {
		if ok := exp[k]; !ok {
			t.Fatalf("missing error %s", k)
		}
	}
}

func TestCancel(t *testing.T) {
	for i := 0; i < 100; i++ {
		testCancel(t)
	}
}

func testCancel(t *testing.T) {

	ch := make(chan error, 1)
	h := NewHandler(&DefaultConsumer{}, ch)
	go func() {
		ch <- nil
		h.Cancel()
	}()

	go func() {
		h.Wait()
		ch <- fmt.Errorf("non-nil")
		close(ch)
	}()

	h.Wait()

	if h.Err() != nil {
		t.Fatal("unexpected error")
	}
}

func TestMultiCancel(t *testing.T) {

	ch := make(chan error)

	h := NewHandler(&DefaultConsumer{}, ch)

	ok1 := h.Cancel()
	ok2 := h.Cancel()

	if !(ok1 == true && ok2 == false) {
		t.Fatalf("expected true, false; got %v, %v", ok1, ok2)
	}
}
