//go:build lite

package app

// registerFull is empty in the lite build: no broker or cloud SDKs.
func registerFull() {}
