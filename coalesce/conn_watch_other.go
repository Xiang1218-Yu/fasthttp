//go:build !unix

package coalesce

import (
	"net"
	"time"
)

// watchConnClose is not supported on this platform; it returns nil so followers
// fall back to the wait timeout and application-level cancellation.
func watchConnClose(conn net.Conn, deadline time.Time, stop <-chan struct{}) <-chan struct{} {
	return nil
}
