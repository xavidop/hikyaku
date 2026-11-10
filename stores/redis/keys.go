package redis

import "strings"

// keys names a store's Redis keys (design §6.5). Every key starts with the
// key prefix and the relay name. The variable part of a history list or a
// message index is wrapped in braces, a Redis Cluster hash tag, so each lands
// on the slot of its own key or message ID:
//
//	{prefix}:{relay}:{route}:hist:{<key>}   LIST, newest record first
//	{prefix}:{relay}:msg:{<msg_id>}         HASH route, key; same TTL as the record
//	{prefix}:{relay}:kv:<k>                 the KV surface
type keys struct {
	base string // "{prefix}:{relay}:"
}

func newKeys(prefix, relay string) keys {
	return keys{base: prefix + ":" + relay + ":"}
}

// hist is the history list of a route's key.
func (k keys) hist(route, key string) string {
	return k.base + route + ":hist:{" + key + "}"
}

// msg is the index hash of a message ID.
func (k keys) msg(id string) string {
	return k.base + "msg:{" + id + "}"
}

// kv is the key of a KV entry.
func (k keys) kv(name string) string {
	return k.base + "kv:" + name
}

// histPattern is the SCAN MATCH pattern of the history lists of a route, or
// of every route when route is empty.
func (k keys) histPattern(route string) string {
	if route == "" {
		return globEscape(k.base) + "*:hist:{*}"
	}
	return globEscape(k.base+route+":hist:") + "{*}"
}

// globEscape escapes the glob metacharacters of a SCAN MATCH pattern. It works
// on bytes, so a key that is not valid UTF-8 survives.
func globEscape(s string) string {
	if !strings.ContainsAny(s, `*?[]\`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '*', '?', '[', ']', '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}
