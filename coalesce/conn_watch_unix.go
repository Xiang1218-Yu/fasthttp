//go:build unix

package coalesce

import (
	"crypto/tls"
	"errors"
	"net"
	"syscall"
	"time"
)

// connWatchPoll bounds the time a watcher blocks on one peeking read; it also
// bounds how quickly a watcher notices stop after the follower finished.
const connWatchPoll = 200 * time.Millisecond

// watchConnClose starts a best-effort watcher that closes the returned channel
// when the peer closes conn (orderly FIN or RST). It peeks at the raw socket
// with MSG_PEEK, so buffered request bytes (e.g. pipelined requests) are never
// consumed. The watcher stops when stop is closed or when deadline elapses.
//
// It returns nil when the connection does not expose a raw socket (for example
// in-memory listeners); callers then rely on wait timeout and application-level
// cancellation.
func watchConnClose(conn net.Conn, deadline time.Time, stop <-chan struct{}) <-chan struct{} {
	if conn == nil {
		return nil
	}
	if tlsConn, ok := conn.(*tls.Conn); ok {
		conn = tlsConn.NetConn()
	}
	rawConn, ok := conn.(syscall.Conn)
	if !ok {
		return nil
	}
	rc, err := rawConn.SyscallConn()
	if err != nil || rc == nil {
		return nil
	}

	closed := make(chan struct{})
	go func() {
		var buf [1]byte
		for {
			pollDeadline := time.Now().Add(connWatchPoll)
			if !deadline.IsZero() && deadline.Before(pollDeadline) {
				pollDeadline = deadline
			}
			// fasthttp only reads the next request after the handler returns;
			// it resets the read deadline itself then, so arming a deadline
			// here does not interfere with request serving.
			_ = conn.SetReadDeadline(pollDeadline)

			var (
				called bool
				n      int
				rerr   error
			)
			// The callback is invoked only when the socket is readable; the
			// peeking recv therefore returns immediately and never steals
			// data from the server's own reader.
			_ = rc.Read(func(fd uintptr) bool {
				called = true
				n, _, rerr = syscall.Recvfrom(int(fd), buf[:], syscall.MSG_PEEK)
				return true
			})

			if called {
				switch {
				case rerr == nil && n == 0:
					// Orderly connection close (EOF).
					close(closed)
					return
				case rerr != nil && isPeerGone(rerr):
					// Aborted connection (RST / shutdown).
					close(closed)
					return
				case rerr == nil && n > 0:
					// Pipelined request data: the peer is alive. Re-check
					// infrequently; the peeked bytes remain buffered.
					timer := time.NewTimer(time.Second)
					select {
					case <-stop:
						timer.Stop()
						return
					case <-timer.C:
					}
				default:
					// Transient condition (EAGAIN/EINTR): poll again.
				}
			}

			if !deadline.IsZero() && !time.Now().Before(deadline) {
				return
			}
			select {
			case <-stop:
				return
			default:
			}
		}
	}()
	return closed
}

func isPeerGone(err error) bool {
	return errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ENOTCONN) ||
		errors.Is(err, syscall.ESHUTDOWN)
}
