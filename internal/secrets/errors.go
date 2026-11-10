package secrets

import (
	"errors"
	"fmt"

	"github.com/xavidop/mamori"
)

// ConfigError is a secret that cannot be resolved because of the
// configuration: an unset ref var, an invalid ref or a chain with no value
// (exit code 2).
type ConfigError struct {
	Name string
	Msg  string
	Err  error
}

func (e *ConfigError) Error() string { return fmt.Sprintf("secret %s: %s", e.Name, e.Msg) }

// Unwrap returns the mamori error, if any.
func (e *ConfigError) Unwrap() error { return e.Err }

// InfraError is a provider that failed: unreachable, denied, throttled
// (exit code 3).
type InfraError struct {
	Name   string
	Scheme string
	Ref    string
	Kind   mamori.Kind
	Err    error
}

func (e *InfraError) Error() string {
	return fmt.Sprintf("secret %s: provider %s failed (%s): %v", e.Name, e.Scheme, e.Kind, e.Err)
}

// Unwrap returns the provider error.
func (e *InfraError) Unwrap() error { return e.Err }

// IsConfig reports whether err is (or wraps) a ConfigError.
func IsConfig(err error) bool {
	var ce *ConfigError
	return errors.As(err, &ce)
}

// IsInfra reports whether err is (or wraps) an InfraError.
func IsInfra(err error) bool {
	var ie *InfraError
	return errors.As(err, &ie)
}
