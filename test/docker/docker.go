package docker

import (
	"errors"
	"time"

	"github.com/ory/dockertest"
)

// Runtime wraps dockertest functionality into a reusable component.
type Runtime struct {
	expiration time.Duration
	pool       *dockertest.Pool
	resources  []*dockertest.Resource
}

// NewRuntime constructor.
func NewRuntime(expiration time.Duration) (*Runtime, error) {
	if expiration < 0 {
		return nil, errors.New("expiration value is negative")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, err
	}
	pool.MaxWait = expiration

	return &Runtime{expiration: expiration, pool: pool}, nil
}

// Pool getter.
func (b *Runtime) Pool() *dockertest.Pool {
	return b.pool
}

// Expiration getter.
func (b *Runtime) Expiration() time.Duration {
	return b.expiration
}

// RunWithOptions runs a resources provided with options.
func (b *Runtime) RunWithOptions(ro *dockertest.RunOptions) (*dockertest.Resource, error) {
	resource, err := b.pool.RunWithOptions(ro)
	if err != nil {
		return nil, err
	}
	b.resources = append(b.resources, resource)

	err = resource.Expire(uint(b.expiration.Seconds()))
	if err != nil {
		return nil, err
	}
	return resource, nil
}

// Teardown all resources in the opposite order of their creation.
func (b *Runtime) Teardown() []error {
	ee := make([]error, 0)

	for i := len(b.resources) - 1; i >= 0; i-- {
		err := b.pool.Purge(b.resources[i])
		if err != nil {
			ee = append(ee, err)
		}
	}

	return ee
}
