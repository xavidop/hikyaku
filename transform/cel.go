package transform

import (
	"fmt"
	"sync"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"

	"github.com/xavidop/hikyaku/envelope"
)

// Expr is a compiled CEL expression over `msg`: msg.json, msg.headers,
// msg.bodyText, msg.attributes, msg.source, msg.subject, msg.id,
// msg.protocol, msg.contentType.
type Expr struct {
	src string
	prg cel.Program
}

var (
	celOnce sync.Once
	celEnv  *cel.Env
	celErr  error
)

func env() (*cel.Env, error) {
	celOnce.Do(func() {
		celEnv, celErr = cel.NewEnv(cel.Variable("msg", cel.MapType(cel.StringType, cel.DynType)))
	})
	return celEnv, celErr
}

// CompileCEL compiles a boolean CEL expression.
func CompileCEL(src string) (*Expr, error) {
	e, err := env()
	if err != nil {
		return nil, err
	}
	ast, iss := e.Compile(src)
	if iss != nil && iss.Err() != nil {
		return nil, fmt.Errorf("cel %q: %w", src, iss.Err())
	}
	if t := ast.OutputType(); t != cel.BoolType && t != cel.DynType {
		return nil, fmt.Errorf("cel %q: evaluates to %s, want a bool", src, t)
	}
	prg, err := e.Program(ast, cel.EvalOptions(cel.OptOptimize))
	if err != nil {
		return nil, fmt.Errorf("cel %q: %w", src, err)
	}
	return &Expr{src: src, prg: prg}, nil
}

// String returns the expression's source.
func (x *Expr) String() string { return x.src }

// Bool evaluates the expression against an envelope. A missing key or a
// type mismatch is an error the caller decides about (rules treat it as no
// match; filters let the message through).
func (x *Expr) Bool(env *envelope.Envelope) (bool, error) {
	return x.BoolData(Data(env))
}

// BoolData evaluates the expression against a prepared Data map.
func (x *Expr) BoolData(data map[string]any) (bool, error) {
	out, _, err := x.prg.Eval(map[string]any{"msg": data})
	if err != nil {
		return false, fmt.Errorf("cel %q: %w", x.src, err)
	}
	b, ok := out.(types.Bool)
	if !ok {
		return false, fmt.Errorf("cel %q: evaluated to %v, want a bool", x.src, out)
	}
	return bool(b), nil
}
