package redis

import (
	"encoding/json"

	goredis "github.com/redis/go-redis/v9"

	"github.com/xavidop/hikyaku/store"
)

// feedbackScript attaches a feedback to the record of a message.
//
//	KEYS[1]  the history list the message index points to
//	ARGV[1]  the message ID
//	ARGV[2]  the feedback, as JSON
//
// It finds the record whose msg_id matches, sets its feedback, writes it back
// with LSET and returns it; it returns nil when the list holds no such record.
//
// Records are the JSON objects encodeRecord writes, whose feedback, when there
// is one, is their last member. The script replaces the feedback by cutting
// the record at that member, so the rest of the record is kept byte for byte:
// re-encoding it with cjson would turn empty arrays into objects and round
// floats to 14 digits. The member can be found by its text: a quote inside a
// JSON string is always escaped, so `,"feedback":` can only open a "feedback"
// member, and no object nested in a record has one.
var feedbackScript = goredis.NewScript(`
local items = redis.call('LRANGE', KEYS[1], 0, -1)
for i, raw in ipairs(items) do
  local ok, rec = pcall(cjson.decode, raw)
  if ok and type(rec) == 'table' and rec['msg_id'] == ARGV[1] then
    local base = string.sub(raw, 1, -2)
    if rec['feedback'] ~= nil then
      local cut, from = nil, 1
      while true do
        local at = string.find(raw, ',"feedback":', from, true)
        if at == nil then break end
        cut, from = at, at + 1
      end
      if cut == nil then
        return {err = 'hikyaku: the record of ' .. ARGV[1] .. ' has no trailing feedback member'}
      end
      base = string.sub(raw, 1, cut - 1)
    end
    local updated = base .. ',"feedback":' .. ARGV[2] .. '}'
    redis.call('LSET', KEYS[1], i - 1, updated)
    return updated
  end
end
return nil
`)

// incrScript adds one to a counter and gives a counter it just created its
// TTL, atomically.
//
//	KEYS[1]  the counter
//	ARGV[1]  the TTL in milliseconds; 0 means none
var incrScript = goredis.NewScript(`
local n = redis.call('INCR', KEYS[1])
local ttl = tonumber(ARGV[1])
if n == 1 and ttl > 0 then
  redis.call('PEXPIRE', KEYS[1], ttl)
end
return n
`)

// feedbackMember opens the feedback member of a stored record.
const feedbackMember = `,"feedback":`

// encodeRecord marshals a record for its history list, with its feedback, if
// any, as the last member: feedbackScript relies on that, whatever the field
// order of store.CallRecord becomes.
func encodeRecord(rec store.CallRecord) ([]byte, error) {
	fb := rec.Feedback
	rec.Feedback = nil
	b, err := json.Marshal(rec)
	if err != nil || fb == nil {
		return b, err
	}
	f, err := json.Marshal(fb)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(b)+len(feedbackMember)+len(f))
	out = append(out, b[:len(b)-1]...) // b is an object with members: it ends in '}'
	out = append(out, feedbackMember...)
	out = append(out, f...)
	return append(out, '}'), nil
}

// decodeRecord unmarshals a stored record.
func decodeRecord(raw string) (store.CallRecord, error) {
	var rec store.CallRecord
	err := json.Unmarshal([]byte(raw), &rec)
	return rec, err
}
