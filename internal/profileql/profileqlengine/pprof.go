package profileqlengine

import (
	"bytes"

	"github.com/go-faster/errors"
	"github.com/google/pprof/profile"

	"github.com/oteldb/oteldb/internal/profilestorage"
)

// Pprof renders the result into a gzip-compressed Google pprof profile.
//
// Each tree node with a non-zero self value becomes a sample whose stack is the
// path from that node up to the root (leaf-first, as pprof expects). The
// synthetic "total" root is omitted.
func (r *Result) Pprof() ([]byte, error) {
	p := &profile.Profile{
		SampleType: []*profile.ValueType{
			{Type: r.Type.SampleType, Unit: r.Type.SampleUnit},
		},
		PeriodType: &profile.ValueType{Type: r.Type.PeriodType, Unit: r.Type.PeriodUnit},
	}

	b := pprofBuilder{
		profile: p,
		locs:    map[string]*profile.Location{},
		funcs:   map[string]*profile.Function{},
	}
	if r.Tree != nil && r.Tree.Root != nil {
		for _, child := range r.Tree.Root.Children {
			b.walk(child, nil)
		}
	}

	var buf bytes.Buffer
	if err := p.Write(&buf); err != nil {
		return nil, errors.Wrap(err, "write pprof")
	}
	return buf.Bytes(), nil
}

type pprofBuilder struct {
	profile *profile.Profile
	locs    map[string]*profile.Location
	funcs   map[string]*profile.Function
}

func (b *pprofBuilder) walk(n *profilestorage.FlameNode, parents []*profile.Location) {
	// pprof stacks are leaf-first, so the current node's location precedes its
	// ancestors.
	stack := append([]*profile.Location{b.location(n.Name)}, parents...)
	if n.Self > 0 {
		b.profile.Sample = append(b.profile.Sample, &profile.Sample{
			Location: stack,
			Value:    []int64{n.Self},
		})
	}
	for _, child := range n.Children {
		b.walk(child, stack)
	}
}

func (b *pprofBuilder) location(name string) *profile.Location {
	if loc, ok := b.locs[name]; ok {
		return loc
	}
	loc := &profile.Location{
		ID:   uint64(len(b.profile.Location) + 1),
		Line: []profile.Line{{Function: b.function(name)}},
	}
	b.locs[name] = loc
	b.profile.Location = append(b.profile.Location, loc)
	return loc
}

func (b *pprofBuilder) function(name string) *profile.Function {
	if fn, ok := b.funcs[name]; ok {
		return fn
	}
	fn := &profile.Function{
		ID:         uint64(len(b.profile.Function) + 1),
		Name:       name,
		SystemName: name,
	}
	b.funcs[name] = fn
	b.profile.Function = append(b.profile.Function, fn)
	return fn
}
