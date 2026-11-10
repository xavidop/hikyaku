package store

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// Turn is one role-tagged message of a history-aware state.
type Turn struct {
	Role    string `json:"role"` // user | model
	Content string `json:"content"`
}

// Turn roles.
const (
	RoleUser  = "user"
	RoleModel = "model"
)

// HistoryPreamble is the line added to the system message when a state
// carries history, so the model knows how to read the turns.
const HistoryPreamble = "The state is the routing history of one key, oldest first: each user turn is a past message, and the model turn after it says where it was routed and what happened. The last user turn is the message to route now."

// ModelTurnText says what happened to a past call, for its model turn:
// "routed to support (p=0.91), outcome: delivered", plus the correction
// when one was recorded.
func ModelTurnText(r CallRecord) string {
	var parts []string
	for _, a := range r.Decision {
		if a.Chosen {
			parts = append(parts, fmt.Sprintf("%s (p=%.2f)", a.Candidate, a.Probability))
		}
	}
	dest := strings.Join(parts, ", ")
	if dest == "" {
		dest = strings.Join(r.DeliveredTo, ", ")
	}
	if dest == "" {
		dest = "no destination"
	}
	s := "routed to " + dest + ", outcome: " + r.Outcome
	if r.Feedback != nil && len(r.Feedback.Correct) > 0 {
		s += ", correct destination: " + strings.Join(r.Feedback.Correct, ", ")
	}
	return s
}

// UserTurnText is a past call's user turn: its stored state or summary.
func UserTurnText(r CallRecord) string {
	if r.State == "" {
		return "(message not recorded)"
	}
	return r.State
}

// Turns is a built state: the turns, and what budgeting did to fit them.
type Turns struct {
	Turns []Turn
	// Dropped is how many past calls were left out to fit max bytes.
	Dropped int
	// Truncated reports that the current message itself was cut.
	Truncated bool
}

// Bytes is the total size of the turns' contents.
func (t Turns) Bytes() int {
	n := 0
	for _, x := range t.Turns {
		n += len(x.Content)
	}
	return n
}

// BuildTurns turns past calls (oldest first) and the current message into
// role-tagged turns: a user turn and a model turn per past call, then the
// current message as the last user turn. maxBytes (0 = no cap) caps the
// total: the oldest pairs are dropped first, and the current message is cut
// only once no history is left.
func BuildTurns(history []CallRecord, current string, maxBytes int) Turns {
	pairs := make([][2]Turn, len(history))
	for i, r := range history {
		pairs[i] = [2]Turn{{Role: RoleUser, Content: UserTurnText(r)}, {Role: RoleModel, Content: ModelTurnText(r)}}
	}
	size := len(current)
	for _, p := range pairs {
		size += len(p[0].Content) + len(p[1].Content)
	}
	dropped := 0
	for maxBytes > 0 && size > maxBytes && dropped < len(pairs) {
		size -= len(pairs[dropped][0].Content) + len(pairs[dropped][1].Content)
		dropped++
	}
	out := Turns{Dropped: dropped}
	for _, p := range pairs[dropped:] {
		out.Turns = append(out.Turns, p[0], p[1])
	}
	if maxBytes > 0 && len(current) > maxBytes {
		current = TruncateUTF8(current, maxBytes)
		out.Truncated = true
	}
	out.Turns = append(out.Turns, Turn{Role: RoleUser, Content: current})
	return out
}

// TruncateUTF8 cuts s to at most n bytes without splitting a character.
func TruncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
