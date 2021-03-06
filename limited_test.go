package limited_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tschaub/limited"
)

func TestWithContext(t *testing.T) {
	limit := 10
	max := 0

	running := make(chan int)
	defer close(running)

	go func() {
		current := 0
		for delta := range running {
			current += delta
			if current > max {
				max = current
			}
		}
	}()

	group, _ := limited.WithContext(context.Background(), limit)
	for i := 0; i < limit*10; i++ {
		err := group.Go(func() error {
			running <- 1
			time.Sleep(10 * time.Millisecond)
			running <- -1
			return nil
		})
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	}

	if err := group.Wait(); err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if max != limit {
		t.Errorf("expected a limit of %d, got %d", limit, max)
	}
}

func TestWithContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	group, groupCtx := limited.WithContext(ctx, 1)

	expectedErr := errors.New("custom")
	err := group.Go(func() error {
		<-groupCtx.Done()
		return expectedErr
	})
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	cancel()
	if err := group.Wait(); err != expectedErr {
		t.Errorf("expected custom error, got %v", err)
	}
}

func TestWithContextLimit(t *testing.T) {
	limit := 10
	started := int64(0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	custom := errors.New("custom")
	group, _ := limited.WithContext(ctx, limit)
	var err error
	for i := 0; i < limit*10; i++ {
		err = group.Go(func() error {
			atomic.AddInt64(&started, 1)
			if atomic.LoadInt64(&started) >= int64(limit) {
				cancel()
				return custom
			}
			time.Sleep(10 * time.Millisecond)
			return nil
		})
		if err != nil {
			break
		}
	}

	waitErr := group.Wait()
	if waitErr != custom {
		t.Errorf("expected custom error, got %v", waitErr)
	}

	if err != context.Canceled {
		t.Errorf("expected context canceled error, got %v", err)
	}

	if started != int64(limit) {
		t.Errorf("expected a limit of %d, got %d", limit, started)
	}
}

func TestWithContextError(t *testing.T) {
	limit := 10
	custom := errors.New("custom")

	group, _ := limited.WithContext(context.Background(), limit)
	var err error
	for i := 0; i < limit*10; i++ {
		err = group.Go(func() error {
			time.Sleep(10 * time.Millisecond)
			if i > limit {
				return custom
			}
			return nil
		})
		if err != nil {
			break
		}
	}
	waitErr := group.Wait()
	if waitErr != custom {
		t.Errorf("expected custom error, got %v", waitErr)
	}

	if err != context.Canceled {
		t.Errorf("expected context cancelled error, got %v", err)
	}
}
