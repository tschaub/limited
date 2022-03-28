package limited

import (
	"context"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// A Group is a collection of goroutines working on subtasks that are part of
// the same overall task.
//
// (A zero Group is not valid, use the WithContext function.)
type Group struct {
	sem   *semaphore.Weighted
	group *errgroup.Group
	ctx   context.Context
}

// WithContext returns a new Group and an associated Context derived from ctx.
//
// The derived Context is canceled the first time a function passed to Go
// returns a non-nil error or the first time Wait returns, whichever occurs
// first.  No more than limit goroutines will be created at a time.
func WithContext(ctx context.Context, limit int) (*Group, context.Context) {
	group, groupCtx := errgroup.WithContext(ctx)
	sem := semaphore.NewWeighted(int64(limit))
	return &Group{
		sem:   sem,
		group: group,
		ctx:   groupCtx,
	}, groupCtx
}

// Wait blocks until all function calls from the Go method have returned, then
// returns the first non-nil error (if any) from them.
func (g *Group) Wait() error {
	return g.group.Wait()
}

// Go calls the given function in a new goroutine.
//
// The first call to return a non-nil error cancels the group; its error will be
// returned by Wait.  The function will block the number of current goroutines
// started by the group exceeds the limit.  Any returned error will typically be
// due to context cancellation.
func (g *Group) Go(f func() error) error {
	if g.ctx.Err() != nil {
		return g.ctx.Err()
	}
	if err := g.sem.Acquire(g.ctx, 1); err != nil {
		return err
	}
	if g.ctx.Err() != nil {
		return g.ctx.Err()
	}
	g.group.Go(func() error {
		defer g.sem.Release(1)
		return f()
	})
	return nil
}
