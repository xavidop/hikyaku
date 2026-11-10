// Package prompts holds the text hikyaku sends to models, in one place to
// read and change: the generative router's system prompt (a template) and
// the System One questions the decision engine adds to a request.
package prompts

import (
	_ "embed"
	"strings"
	"text/template"
)

//go:embed generative_system.tmpl
var generativeSystemText string

var generativeSystem = template.Must(template.New("generative_system").Parse(generativeSystemText))

// Candidate is a destination the generative router may pick.
type Candidate struct {
	Name        string
	Description string
}

// Generative is what the generative router's system prompt is built from.
type Generative struct {
	// Instructions are the route's own (router.instructions).
	Instructions string
	Candidates   []Candidate
	// Multi lets the model pick several destinations.
	Multi bool
	// Open and Close mark where the message starts and ends.
	Open, Close string
	// History, when set, explains the history turns that come with the message.
	History string
}

// GenerativeSystem renders the generative router's system prompt.
func GenerativeSystem(g Generative) string {
	g.Instructions = strings.TrimSpace(g.Instructions)
	var sb strings.Builder
	if err := generativeSystem.Execute(&sb, g); err != nil {
		panic("prompts: generative_system.tmpl: " + err.Error()) // a broken embedded template
	}
	return sb.String()
}

// The follow-up question sticky routing (sticky.mode: model) asks a
// decision model: is the latest message a follow-up of the history?
const (
	FollowUpInstructions = "Is the latest message a follow-up to a previous call in this history?"
	FollowUpYes          = "The latest message continues, repeats or refers to an earlier message of the history"
	FollowUpNo           = "The latest message starts something new"
)

// HealthCheckInstructions is the question of a decision model's health
// ping.
const HealthCheckInstructions = "Is this a health check?"
