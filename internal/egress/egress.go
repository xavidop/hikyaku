// Package egress is the SSRF guard for requests whose host comes from a
// message: an http destination with a templated URL, an enrich transform.
package egress

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"syscall"

	"github.com/xavidop/hikyaku/plugin"
)

// Guard is the SSRF guard of a request whose host is a per-message
// template (design §12): the host must match egress.allow (when set), and,
// unless egress.allow_private, the address dialed must be public. The check
// runs on the address actually dialed, so DNS rebinding cannot slip a
// private address past it.
type Guard struct {
	hosts        []string // exact hosts and *.suffix patterns
	nets         []*net.IPNet
	allowPrivate bool
}

// New builds the guard of an egress: block.
func New(e plugin.Egress) (*Guard, error) {
	g := &Guard{allowPrivate: e.AllowPrivate}
	for _, a := range e.Allow {
		a = strings.ToLower(strings.TrimSpace(a))
		if strings.Contains(a, "/") {
			_, n, err := net.ParseCIDR(a)
			if err != nil {
				return nil, fmt.Errorf("egress.allow %q: %w", a, err)
			}
			g.nets = append(g.nets, n)
			continue
		}
		if ip := net.ParseIP(a); ip != nil {
			bits := 32
			if ip.To4() == nil {
				bits = 128
			}
			g.nets = append(g.nets, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
			continue
		}
		g.hosts = append(g.hosts, a)
	}
	return g, nil
}

// ErrDenied marks a request the guard refused: permanent, a fallback or
// the DLQ takes the message.
var ErrDenied = errors.New("egress denied")

// CheckHost runs before a request: the host must be allowed by name.
func (g *Guard) CheckHost(host string) error {
	host = strings.ToLower(host)
	if len(g.hosts) == 0 && len(g.nets) == 0 {
		return nil
	}
	if g.byName(host) {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil {
		for _, n := range g.nets {
			if n.Contains(ip) {
				return nil
			}
		}
	}
	if len(g.hosts) > 0 || len(g.nets) > 0 {
		// A name not in the list may still resolve into an allowed CIDR:
		// control() decides with the dialed address.
		if len(g.nets) > 0 && net.ParseIP(host) == nil {
			return nil
		}
		return plugin.Wrap(plugin.Permanent, []string{"egress"}, fmt.Errorf("%w: host %s is not in egress.allow", ErrDenied, host))
	}
	return nil
}

// byName reports whether host is allowed by an egress.allow host entry.
func (g *Guard) byName(host string) bool {
	host = strings.ToLower(host)
	for _, h := range g.hosts {
		if h == host || (strings.HasPrefix(h, "*.") && strings.HasSuffix(host, h[1:])) {
			return true
		}
	}
	return false
}

// control runs for every dial, on the address dialed. With egress.allow
// set, the address must be in one of its CIDRs unless the host dialed was
// allowed by name. Private, loopback, link-local and unspecified addresses
// are refused unless allowed (allow_private, or a CIDR that holds them).
func (g *Guard) control(address string, byName bool) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("%w: cannot parse %s", ErrDenied, address)
	}
	for _, n := range g.nets {
		if n.Contains(ip) {
			return nil
		}
	}
	if (len(g.hosts) > 0 || len(g.nets) > 0) && !byName {
		return fmt.Errorf("%w: %s is not in egress.allow", ErrDenied, ip)
	}
	if !g.allowPrivate && (ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsInterfaceLocalMulticast()) {
		return fmt.Errorf("%w: %s is a private address (set egress.allow_private to allow it)", ErrDenied, ip)
	}
	return nil
}

// Dialer returns a dial function with the guard's control. The transport
// dials host:port with the host name, so each dial (a redirect's too)
// knows whether its host was allowed by name.
func (g *Guard) Dialer(base *net.Dialer) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		named := g.byName(host)
		d := *base
		d.Control = func(_, address string, _ syscall.RawConn) error { return g.control(address, named) }
		return d.DialContext(ctx, network, addr)
	}
}
