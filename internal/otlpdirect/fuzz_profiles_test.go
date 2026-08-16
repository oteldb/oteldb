package otlpdirect_test

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pprofile"

	"github.com/oteldb/oteldb/internal/otlpdirect"
)

// FuzzConvertProfiles drives the profiles decoder with arbitrary bytes. Everything here is
// index-based — samples name stacks, stacks name locations, locations name functions and mappings,
// and all of them bottom out in a string table the same request supplies — so a malformed request
// has plenty of room to point an index somewhere the table does not reach.
func FuzzConvertProfiles(f *testing.F) {
	seed := func(build func(pprofile.Profiles)) {
		pd := pprofile.NewProfiles()
		build(pd)

		raw, err := (&pprofile.ProtoMarshaler{}).MarshalProfiles(pd)
		if err != nil {
			f.Fatal(err)
		}

		f.Add(raw)
	}

	seed(func(pprofile.Profiles) {})

	seed(func(pd pprofile.Profiles) {
		pd.Dictionary().StringTable().Append("")

		pr := pd.ResourceProfiles().AppendEmpty().ScopeProfiles().AppendEmpty().Profiles().AppendEmpty()
		pr.SetTime(1)
		pr.Samples().AppendEmpty().Values().Append(1)
	})

	seed(func(pd pprofile.Profiles) {
		full := fullProfiles(f)
		full.CopyTo(pd)
	})

	// Many samples in one profile, so the reused scratch is exercised under mutation.
	seed(func(pd pprofile.Profiles) {
		pd.Dictionary().StringTable().Append("")

		pr := pd.ResourceProfiles().AppendEmpty().ScopeProfiles().AppendEmpty().Profiles().AppendEmpty()
		for i := range 8 {
			s := pr.Samples().AppendEmpty()
			s.SetStackIndex(int32(i))
			s.Values().Append(int64(i))

			if i%2 == 0 {
				s.TimestampsUnixNano().Append(uint64(i))
			}

			if i%3 == 0 {
				s.AttributeIndices().Append(int32(i))
			}
		}
	})

	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		var c otlpdirect.ProfilesConverter

		got, err := c.Convert(data)
		if err != nil {
			return
		}

		dict := got.Dictionary
		for i := range dict.Strings {
			_ = len(dict.Strings[i])
		}

		for i := range dict.Locations {
			_ = len(dict.Locations[i].Lines) + len(dict.Locations[i].AttributeIndices)
		}

		for i := range dict.Stacks {
			_ = len(dict.Stacks[i].LocationIndices)
		}

		for i := range dict.Links {
			_ = len(dict.Links[i].TraceID) + len(dict.Links[i].SpanID)
		}

		for i := range got.Resources {
			rp := &got.Resources[i]
			_ = len(rp.Resource.SchemaURL) + len(rp.Resource.Attributes)

			for j := range rp.Scopes {
				sp := &rp.Scopes[j]
				_ = len(sp.Scope.Name) + len(sp.Scope.Attributes)

				for k := range sp.Profiles {
					pr := &sp.Profiles[k]
					_ = len(pr.ProfileID) + len(pr.AttributeIndices)

					for s := range pr.Samples {
						_ = len(pr.Samples[s].Values) + len(pr.Samples[s].TimestampsUnixNano) +
							len(pr.Samples[s].AttributeIndices)
					}
				}
			}
		}

		// The scratch is reused across elements and across calls; a second pass over the same
		// bytes must produce the same batch, not one contaminated by the first.
		again, err := c.Convert(data)
		if err != nil {
			t.Fatalf("second convert of the same input failed: %v", err)
		}

		if len(again.Resources) != len(got.Resources) {
			t.Fatalf("reuse changed the batch: %d resources then %d", len(got.Resources), len(again.Resources))
		}

		if len(again.Dictionary.Strings) != len(got.Dictionary.Strings) {
			t.Fatalf("reuse changed the dictionary: %d strings then %d",
				len(got.Dictionary.Strings), len(again.Dictionary.Strings))
		}
	})
}
