package otlpdirect_test

import (
	"github.com/VictoriaMetrics/easyproto"

	"github.com/oteldb/storage/signal/profile"
)

// encodeProfiles builds one ExportProfilesServiceRequest carrying a fully-populated dictionary and
// a single profile, writing each message's fields ascending or descending.
//
// Profiles has one ordering hazard the other signals do not: the shared dictionary is field 2 of
// the request while the resource profiles are field 1, and a resource attribute's key and string
// value are indices into that dictionary. Ascending order therefore puts the table the resources
// need after them on the wire.
func encodeProfiles(ascending bool) []byte {
	var m easyproto.Marshaler

	req := m.MessageMarshaler()

	writeDictionary := func() {
		dict := req.AppendMessage(2)

		writeMapping := func() {
			mp := dict.AppendMessage(1)
			mp.AppendUint64(1, 0x400000)
			mp.AppendUint64(2, 0x500000)
			mp.AppendUint64(3, 0x1000)
			mp.AppendInt32(4, 6)
			mp.AppendInt32s(5, []int32{0})
		}

		writeLocation := func() {
			loc := dict.AppendMessage(2)

			writeLines := func() {
				ln := loc.AppendMessage(3)
				ln.AppendInt32(1, 0)
				ln.AppendInt64(2, 17)
				ln.AppendInt64(3, 3)
			}

			if ascending {
				loc.AppendInt32(1, 0)
				loc.AppendUint64(2, 0x4010a0)
				writeLines()
				loc.AppendInt32s(4, []int32{0})
			} else {
				loc.AppendInt32s(4, []int32{0})
				writeLines()
				loc.AppendUint64(2, 0x4010a0)
				loc.AppendInt32(1, 0)
			}
		}

		writeFunction := func() {
			fn := dict.AppendMessage(3)
			fn.AppendInt32(1, 5)
			fn.AppendInt32(2, 5)
			fn.AppendInt32(3, 6)
			fn.AppendInt64(4, 42)
		}

		writeLink := func() {
			lk := dict.AppendMessage(4)
			lk.AppendBytes(1, []byte("0123456789abcdef"))
			lk.AppendBytes(2, []byte("01234567"))
		}

		writeStrings := func() {
			for _, s := range []string{"", "service.name", "api", "samples", "count", "main", "/src/main.go"} {
				dict.AppendString(5, s)
			}
		}

		writeAttribute := func() {
			at := dict.AppendMessage(6)
			at.AppendInt32(1, 1)
			at.AppendMessage(2).AppendString(1, "api")
			at.AppendInt32(3, 4)
		}

		writeStack := func() { dict.AppendMessage(7).AppendInt32s(1, []int32{0}) }

		if ascending {
			writeMapping()
			writeLocation()
			writeFunction()
			writeLink()
			writeStrings()
			writeAttribute()
			writeStack()
		} else {
			writeStack()
			writeAttribute()
			writeStrings()
			writeLink()
			writeFunction()
			writeLocation()
			writeMapping()
		}
	}

	writeResource := func() {
		rp := req.AppendMessage(1)

		writeRes := func() {
			res := rp.AppendMessage(1)

			// key_strindex and string_value_strindex, the form pdata puts on the wire.
			kv := res.AppendMessage(1)
			kv.AppendInt32(3, 1)
			kv.AppendMessage(2).AppendInt32(8, 2)
		}

		writeScopes := func() {
			sp := rp.AppendMessage(2)

			writeScope := func() {
				sc := sp.AppendMessage(1)
				sc.AppendString(1, "scope-name")
				sc.AppendString(2, "1.0.0")
			}

			writeProfiles := func() {
				pr := sp.AppendMessage(2)

				writeTypes := func() {
					st := pr.AppendMessage(1)
					st.AppendInt32(1, 3)
					st.AppendInt32(2, 4)

					pt := pr.AppendMessage(5)
					pt.AppendInt32(1, 3)
					pt.AppendInt32(2, 4)
				}

				writeSample := func() {
					s := pr.AppendMessage(2)
					s.AppendInt32(1, 0)
					s.AppendInt32s(2, []int32{0})
					s.AppendInt32(3, 0)
					s.AppendInt64s(4, []int64{10, 20})
					s.AppendFixed64s(5, []uint64{1, 2})
				}

				writeScalars := func() {
					pr.AppendFixed64(3, 1_700_000_000_000_000_000)
					pr.AppendUint64(4, 30_000_000_000)
					pr.AppendInt64(6, 10_000_000)
					pr.AppendBytes(7, []byte("profile-id-0000a"))
					pr.AppendUint32(8, 3)
					pr.AppendInt32s(11, []int32{0})
				}

				if ascending {
					writeTypes()
					writeSample()
					writeScalars()
				} else {
					writeScalars()
					writeSample()
					writeTypes()
				}
			}

			if ascending {
				writeScope()
				writeProfiles()
				sp.AppendString(3, "https://schema.example/scope")
			} else {
				sp.AppendString(3, "https://schema.example/scope")
				writeProfiles()
				writeScope()
			}
		}

		if ascending {
			writeRes()
			writeScopes()
			rp.AppendString(3, "https://schema.example/resource")
		} else {
			rp.AppendString(3, "https://schema.example/resource")
			writeScopes()
			writeRes()
		}
	}

	if ascending {
		writeResource()
		writeDictionary()
	} else {
		writeDictionary()
		writeResource()
	}

	return m.Marshal(nil)
}

// canonicalProfiles is [canonical] for the profiles model: it rewrites zero-length byte slices to
// nil so the two paths' spellings of an absent field compare equal. See canonical_test.go.
func canonicalProfiles(p *profile.Profiles) *profile.Profiles {
	canonicalDictionary(&p.Dictionary)

	for i := range p.Resources {
		rp := &p.Resources[i]
		rp.Resource.SchemaURL = canonBytes(rp.Resource.SchemaURL)
		rp.Resource.Attributes = canonAttrs(rp.Resource.Attributes)

		for j := range rp.Scopes {
			sp := &rp.Scopes[j]
			sp.Scope.Name = canonBytes(sp.Scope.Name)
			sp.Scope.Version = canonBytes(sp.Scope.Version)
			sp.Scope.SchemaURL = canonBytes(sp.Scope.SchemaURL)
			sp.Scope.Attributes = canonAttrs(sp.Scope.Attributes)

			for k := range sp.Profiles {
				canonicalProfile(&sp.Profiles[k])
			}

			if len(sp.Profiles) == 0 {
				sp.Profiles = nil
			}
		}

		if len(rp.Scopes) == 0 {
			rp.Scopes = nil
		}
	}

	if len(p.Resources) == 0 {
		p.Resources = nil
	}

	return p
}

func canonicalDictionary(d *profile.Dictionary) {
	for i := range d.Strings {
		d.Strings[i] = canonBytes(d.Strings[i])
	}

	for i := range d.Stacks {
		d.Stacks[i].LocationIndices = canonIndices(d.Stacks[i].LocationIndices)
	}

	for i := range d.Locations {
		l := &d.Locations[i]
		l.AttributeIndices = canonIndices(l.AttributeIndices)

		if len(l.Lines) == 0 {
			l.Lines = nil
		}
	}

	for i := range d.Mappings {
		d.Mappings[i].AttributeIndices = canonIndices(d.Mappings[i].AttributeIndices)
	}

	for i := range d.Attributes {
		d.Attributes[i].Value = canonValue(d.Attributes[i].Value)
	}

	for i := range d.Links {
		d.Links[i].TraceID = canonBytes(d.Links[i].TraceID)
		d.Links[i].SpanID = canonBytes(d.Links[i].SpanID)
	}

	canonEmpty(&d.Strings)
	canonEmpty(&d.Stacks)
	canonEmpty(&d.Locations)
	canonEmpty(&d.Functions)
	canonEmpty(&d.Mappings)
	canonEmpty(&d.Attributes)
	canonEmpty(&d.Links)
}

func canonicalProfile(pr *profile.Profile) {
	pr.ProfileID = canonBytes(pr.ProfileID)
	pr.AttributeIndices = canonIndices(pr.AttributeIndices)

	for i := range pr.Samples {
		s := &pr.Samples[i]
		s.AttributeIndices = canonIndices(s.AttributeIndices)

		if len(s.Values) == 0 {
			s.Values = nil
		}

		if len(s.TimestampsUnixNano) == 0 {
			s.TimestampsUnixNano = nil
		}
	}

	canonEmpty(&pr.Samples)
}

func canonIndices(idx []int32) []int32 {
	if len(idx) == 0 {
		return nil
	}

	return idx
}

func canonEmpty[T any](s *[]T) {
	if len(*s) == 0 {
		*s = nil
	}
}
