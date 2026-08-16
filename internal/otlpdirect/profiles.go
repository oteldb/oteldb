package otlpdirect

import (
	"github.com/VictoriaMetrics/easyproto"
	"github.com/go-faster/errors"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/profile"

	"github.com/oteldb/oteldb/internal/xarena"
)

// Field numbers of profiles/v1development/profiles.proto and its collector service.
const (
	// opentelemetry.proto.collector.profiles.v1development.ExportProfilesServiceRequest
	fieldExportResourceProfiles = 1
	fieldExportDictionary       = 2

	// opentelemetry.proto.profiles.v1development.ProfilesDictionary
	fieldDictMappings   = 1
	fieldDictLocations  = 2
	fieldDictFunctions  = 3
	fieldDictLinks      = 4
	fieldDictStrings    = 5
	fieldDictAttributes = 6
	fieldDictStacks     = 7

	// opentelemetry.proto.profiles.v1development.ResourceProfiles
	fieldResourceProfilesResource  = 1
	fieldResourceProfilesScope     = 2
	fieldResourceProfilesSchemaURL = 3

	// opentelemetry.proto.profiles.v1development.ScopeProfiles
	fieldScopeProfilesScope     = 1
	fieldScopeProfilesProfiles  = 2
	fieldScopeProfilesSchemaURL = 3

	// opentelemetry.proto.profiles.v1development.Profile
	fieldProfileSampleType = 1
	fieldProfileSamples    = 2
	fieldProfileTime       = 3
	fieldProfileDuration   = 4
	fieldProfilePeriodType = 5
	fieldProfilePeriod     = 6
	fieldProfileID         = 7
	fieldProfileDropped    = 8
	fieldProfileAttributes = 11

	// opentelemetry.proto.profiles.v1development.Sample
	fieldSampleStack      = 1
	fieldSampleAttributes = 2
	fieldSampleLink       = 3
	fieldSampleValues     = 4
	fieldSampleTimestamps = 5

	// opentelemetry.proto.profiles.v1development.ValueType
	fieldValueTypeType = 1
	fieldValueTypeUnit = 2

	// opentelemetry.proto.profiles.v1development.Mapping
	fieldMappingMemoryStart = 1
	fieldMappingMemoryLimit = 2
	fieldMappingFileOffset  = 3
	fieldMappingFilename    = 4
	fieldMappingAttributes  = 5

	// opentelemetry.proto.profiles.v1development.Location
	fieldLocationMapping    = 1
	fieldLocationAddress    = 2
	fieldLocationLines      = 3
	fieldLocationAttributes = 4

	// opentelemetry.proto.profiles.v1development.Line
	fieldLineFunction = 1
	fieldLineLine     = 2
	fieldLineColumn   = 3

	// opentelemetry.proto.profiles.v1development.Function
	fieldFunctionName       = 1
	fieldFunctionSystemName = 2
	fieldFunctionFilename   = 3
	fieldFunctionStartLine  = 4

	// opentelemetry.proto.profiles.v1development.Stack
	fieldStackLocations = 1

	// opentelemetry.proto.profiles.v1development.KeyValueAndUnit
	fieldAttributeKey   = 1
	fieldAttributeValue = 2
	fieldAttributeUnit  = 3

	// opentelemetry.proto.profiles.v1development.Link
	fieldProfileLinkTraceID = 1
	fieldProfileLinkSpanID  = 2
)

// ProfilesConverter decodes an OTLP ExportProfilesServiceRequest into [profile.Profiles]. It
// retains the batch and the scratch it is built from, so a converter reused across requests
// allocates nothing in steady state. It is not safe for concurrent use; pool one per in-flight
// request.
type ProfilesConverter struct {
	batch profile.Profiles
	dec   decoder

	lines   xarena.Arena[profile.Line]
	indices xarena.Arena[int32]
	values  xarena.Arena[int64]
	stamps  xarena.Arena[uint64]

	// Submessage collectors. Each is consumed before the next element of its kind reaches it, and
	// no walk that reads one re-enters the walk that fills it, so a single buffer per kind serves
	// the whole request.
	dictTables [7][][]byte
	resources  [][]byte
	scopes     [][]byte
	profiles   [][]byte
	samples    [][]byte
	locLines   [][]byte

	// Repeated scalar scratch, copied into the arenas at the end of the element that read it.
	i32 []int32
	i64 []int64
	u64 []uint64
}

// Convert decodes a serialized ExportProfilesServiceRequest.
//
// The returned batch aliases src: every string-table entry, id and attribute key is a sub-slice of
// it. It stays valid until the next Convert on this converter, and src must not be recycled until
// the write consuming the batch has returned.
func (c *ProfilesConverter) Convert(src []byte) (*profile.Profiles, error) {
	c.batch.Reset()
	c.dec.reset()
	c.lines.Reset()
	c.indices.Reset()
	c.values.Reset()
	c.stamps.Reset()

	var (
		fc        easyproto.FieldContext
		dictData  []byte
		resources = c.resources[:0]
		err       error
	)

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return nil, errors.Wrap(err, "read profiles request field")
		}

		switch fc.FieldNum {
		case fieldExportResourceProfiles:
			data, ok := fc.MessageData()
			if !ok {
				return nil, errors.New("read resource profiles")
			}

			resources = append(resources, data)
		case fieldExportDictionary:
			data, ok := fc.MessageData()
			if !ok {
				return nil, errors.New("read profiles dictionary")
			}

			dictData = data
		}
	}

	c.resources = resources

	// The dictionary is decoded first whatever order it arrived in: a resource or scope attribute
	// may reference its string table instead of carrying the string inline.
	if err := c.dictionary(dictData); err != nil {
		return nil, err
	}

	c.dec.strings = c.batch.Dictionary.Strings

	for _, data := range resources {
		if err := c.resourceProfiles(data); err != nil {
			return nil, err
		}
	}

	return &c.batch, nil
}

func (c *ProfilesConverter) dictionary(src []byte) error {
	var (
		fc  easyproto.FieldContext
		err error
	)

	dict := &c.batch.Dictionary

	tables := c.dictTables
	for i := range tables {
		tables[i] = tables[i][:0]
	}

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read dictionary field")
		}

		if fc.FieldNum == fieldDictStrings {
			v, ok := fc.Bytes()
			if !ok {
				return errors.New("read dictionary string")
			}

			dict.Strings = append(dict.Strings, v)

			continue
		}

		if fc.FieldNum < 1 || int(fc.FieldNum) > len(tables) {
			continue
		}

		data, ok := fc.MessageData()
		if !ok {
			return errors.Errorf("read dictionary table %d entry", fc.FieldNum)
		}

		tables[fc.FieldNum-1] = append(tables[fc.FieldNum-1], data)
	}

	c.dictTables = tables

	for _, data := range tables[fieldDictMappings-1] {
		if err := c.mapping(dict, data); err != nil {
			return err
		}
	}

	for _, data := range tables[fieldDictLocations-1] {
		if err := c.location(dict, data); err != nil {
			return err
		}
	}

	for _, data := range tables[fieldDictFunctions-1] {
		if err := c.function(dict, data); err != nil {
			return err
		}
	}

	for _, data := range tables[fieldDictLinks-1] {
		if err := c.link(dict, data); err != nil {
			return err
		}
	}

	for _, data := range tables[fieldDictStacks-1] {
		if err := c.stack(dict, data); err != nil {
			return err
		}
	}

	// Attribute values may themselves reference the string table, so they are decoded once it is
	// complete.
	c.dec.strings = dict.Strings

	for _, data := range tables[fieldDictAttributes-1] {
		if err := c.attribute(dict, data); err != nil {
			return err
		}
	}

	return nil
}

func (c *ProfilesConverter) mapping(dict *profile.Dictionary, src []byte) error {
	var (
		fc  easyproto.FieldContext
		m   profile.Mapping
		err error
	)

	idx := c.i32[:0]

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read mapping field")
		}

		switch fc.FieldNum {
		case fieldMappingMemoryStart:
			if m.MemoryStart, err = takeUint64(&fc, "mapping memory start"); err != nil {
				return err
			}
		case fieldMappingMemoryLimit:
			if m.MemoryLimit, err = takeUint64(&fc, "mapping memory limit"); err != nil {
				return err
			}
		case fieldMappingFileOffset:
			if m.FileOffset, err = takeUint64(&fc, "mapping file offset"); err != nil {
				return err
			}
		case fieldMappingFilename:
			if m.FilenameStrindex, err = takeInt32(&fc, "mapping filename index"); err != nil {
				return err
			}
		case fieldMappingAttributes:
			if idx, err = takeInt32s(&fc, idx, "mapping attribute indices"); err != nil {
				return err
			}
		}
	}

	c.i32 = idx
	m.AttributeIndices = c.ownIndices(idx)

	dict.AddMapping(m)

	return nil
}

func (c *ProfilesConverter) location(dict *profile.Dictionary, src []byte) error {
	var (
		fc  easyproto.FieldContext
		l   profile.Location
		err error
	)

	idx := c.i32[:0]
	lines := c.locLines[:0]

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read location field")
		}

		switch fc.FieldNum {
		case fieldLocationMapping:
			if l.MappingIndex, err = takeInt32(&fc, "location mapping index"); err != nil {
				return err
			}
		case fieldLocationAddress:
			if l.Address, err = takeUint64(&fc, "location address"); err != nil {
				return err
			}
		case fieldLocationLines:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read location line")
			}

			lines = append(lines, data)
		case fieldLocationAttributes:
			if idx, err = takeInt32s(&fc, idx, "location attribute indices"); err != nil {
				return err
			}
		}
	}

	c.i32, c.locLines = idx, lines
	l.AttributeIndices = c.ownIndices(idx)

	if len(lines) > 0 {
		out := c.lines.Alloc(len(lines))

		for _, data := range lines {
			ln, err := line(data)
			if err != nil {
				return err
			}

			out = append(out, ln)
		}

		l.Lines = out
	}

	dict.AddLocation(l)

	return nil
}

func line(src []byte) (profile.Line, error) {
	var (
		fc  easyproto.FieldContext
		ln  profile.Line
		err error
	)

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return ln, errors.Wrap(err, "read line field")
		}

		switch fc.FieldNum {
		case fieldLineFunction:
			if ln.FunctionIndex, err = takeInt32(&fc, "line function index"); err != nil {
				return ln, err
			}
		case fieldLineLine:
			if ln.Line, err = takeInt64(&fc, "line number"); err != nil {
				return ln, err
			}
		case fieldLineColumn:
			if ln.Column, err = takeInt64(&fc, "line column"); err != nil {
				return ln, err
			}
		}
	}

	return ln, nil
}

func (c *ProfilesConverter) function(dict *profile.Dictionary, src []byte) error {
	var (
		fc  easyproto.FieldContext
		f   profile.Function
		err error
	)

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read function field")
		}

		switch fc.FieldNum {
		case fieldFunctionName:
			if f.NameStrindex, err = takeInt32(&fc, "function name index"); err != nil {
				return err
			}
		case fieldFunctionSystemName:
			if f.SystemNameStrindex, err = takeInt32(&fc, "function system name index"); err != nil {
				return err
			}
		case fieldFunctionFilename:
			if f.FilenameStrindex, err = takeInt32(&fc, "function filename index"); err != nil {
				return err
			}
		case fieldFunctionStartLine:
			if f.StartLine, err = takeInt64(&fc, "function start line"); err != nil {
				return err
			}
		}
	}

	dict.AddFunction(f)

	return nil
}

func (c *ProfilesConverter) stack(dict *profile.Dictionary, src []byte) error {
	var (
		fc  easyproto.FieldContext
		err error
	)

	idx := c.i32[:0]

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read stack field")
		}

		if fc.FieldNum == fieldStackLocations {
			if idx, err = takeInt32s(&fc, idx, "stack location indices"); err != nil {
				return err
			}
		}
	}

	c.i32 = idx

	dict.AddStack(c.ownIndices(idx)...)

	return nil
}

func (c *ProfilesConverter) attribute(dict *profile.Dictionary, src []byte) error {
	var (
		fc  easyproto.FieldContext
		a   profile.KeyValueAndUnit
		err error
	)

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read dictionary attribute field")
		}

		switch fc.FieldNum {
		case fieldAttributeKey:
			if a.KeyStrindex, err = takeInt32(&fc, "attribute key index"); err != nil {
				return err
			}
		case fieldAttributeUnit:
			if a.UnitStrindex, err = takeInt32(&fc, "attribute unit index"); err != nil {
				return err
			}
		case fieldAttributeValue:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read attribute value")
			}

			if a.Value, err = c.dec.anyValue(data); err != nil {
				return err
			}
		}
	}

	dict.AddAttribute(a)

	return nil
}

func (c *ProfilesConverter) link(dict *profile.Dictionary, src []byte) error {
	var (
		fc  easyproto.FieldContext
		l   profile.Link
		err error
	)

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read link field")
		}

		switch fc.FieldNum {
		case fieldProfileLinkTraceID:
			if l.TraceID, err = takeBytes(&fc, "link trace id"); err != nil {
				return err
			}
		case fieldProfileLinkSpanID:
			if l.SpanID, err = takeBytes(&fc, "link span id"); err != nil {
				return err
			}
		}
	}

	dict.AddLink(l)

	return nil
}

func (c *ProfilesConverter) resourceProfiles(src []byte) error {
	var (
		fc  easyproto.FieldContext
		res signal.Resource
		err error
	)

	scopes := c.scopes[:0]

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read resource profiles field")
		}

		switch fc.FieldNum {
		case fieldResourceProfilesResource:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read resource")
			}

			if res.Attributes, err = c.dec.resource(data); err != nil {
				return err
			}
		case fieldResourceProfilesScope:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read scope profiles")
			}

			scopes = append(scopes, data)
		case fieldResourceProfilesSchemaURL:
			if res.SchemaURL, err = takeBytes(&fc, "resource schema url"); err != nil {
				return err
			}
		}
	}

	c.scopes = scopes

	rp := c.batch.AddResource()
	rp.Resource = res

	for _, data := range scopes {
		if err := c.scopeProfiles(rp, data); err != nil {
			return err
		}
	}

	return nil
}

func (c *ProfilesConverter) scopeProfiles(rp *profile.ResourceProfiles, src []byte) error {
	var (
		fc        easyproto.FieldContext
		scopeData []byte
		schemaURL []byte
		err       error
	)

	profiles := c.profiles[:0]

	// Field order is the producer's choice — pdata writes them descending, so schema_url arrives
	// before scope. The scope submessage is therefore decoded after the walk, never during it:
	// decoding in place would overwrite a schema_url already read.
	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read scope profiles field")
		}

		switch fc.FieldNum {
		case fieldScopeProfilesScope:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read scope")
			}

			scopeData = data
		case fieldScopeProfilesProfiles:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read profile")
			}

			profiles = append(profiles, data)
		case fieldScopeProfilesSchemaURL:
			if schemaURL, err = takeBytes(&fc, "scope schema url"); err != nil {
				return err
			}
		}
	}

	c.profiles = profiles

	sc, err := c.dec.scope(scopeData)
	if err != nil {
		return err
	}

	sc.SchemaURL = schemaURL

	sp := rp.AddScope()
	sp.Scope = sc

	for _, data := range profiles {
		if err := c.profile(sp, data); err != nil {
			return err
		}
	}

	return nil
}

func (c *ProfilesConverter) profile(sp *profile.ScopeProfiles, src []byte) error {
	var (
		fc         easyproto.FieldContext
		sampleType []byte
		periodType []byte
		err        error
	)

	pr := sp.AddProfile()

	idx := c.i32[:0]
	samples := c.samples[:0]

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read profile field")
		}

		switch fc.FieldNum {
		case fieldProfileSampleType:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read sample type")
			}

			sampleType = data
		case fieldProfilePeriodType:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read period type")
			}

			periodType = data
		case fieldProfileSamples:
			data, ok := fc.MessageData()
			if !ok {
				return errors.New("read sample")
			}

			samples = append(samples, data)
		case fieldProfileTime:
			v, ok := fc.Fixed64()
			if !ok {
				return errors.New("read profile time")
			}

			pr.TimeNanos = int64(v)
		case fieldProfileDuration:
			v, ok := fc.Uint64()
			if !ok {
				return errors.New("read profile duration")
			}

			pr.DurationNanos = int64(v)
		case fieldProfilePeriod:
			if pr.Period, err = takeInt64(&fc, "profile period"); err != nil {
				return err
			}
		case fieldProfileID:
			if pr.ProfileID, err = takeBytes(&fc, "profile id"); err != nil {
				return err
			}
		case fieldProfileDropped:
			v, ok := fc.Uint32()
			if !ok {
				return errors.New("read profile dropped count")
			}

			pr.Dropped = v
		case fieldProfileAttributes:
			if idx, err = takeInt32s(&fc, idx, "profile attribute indices"); err != nil {
				return err
			}
		}
	}

	c.i32, c.samples = idx, samples
	pr.AttributeIndices = c.ownIndices(idx)

	if pr.SampleType, err = valueType(sampleType); err != nil {
		return err
	}

	if pr.PeriodType, err = valueType(periodType); err != nil {
		return err
	}

	for _, data := range samples {
		if err := c.sample(pr, data); err != nil {
			return err
		}
	}

	return nil
}

func valueType(src []byte) (profile.ValueType, error) {
	var (
		fc  easyproto.FieldContext
		vt  profile.ValueType
		err error
	)

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return vt, errors.Wrap(err, "read value type field")
		}

		switch fc.FieldNum {
		case fieldValueTypeType:
			if vt.TypeStrindex, err = takeInt32(&fc, "value type index"); err != nil {
				return vt, err
			}
		case fieldValueTypeUnit:
			if vt.UnitStrindex, err = takeInt32(&fc, "value unit index"); err != nil {
				return vt, err
			}
		}
	}

	return vt, nil
}

func (c *ProfilesConverter) sample(pr *profile.Profile, src []byte) error {
	var (
		fc  easyproto.FieldContext
		err error
	)

	s := pr.AddSample()

	idx := c.i32[:0]
	values := c.i64[:0]
	stamps := c.u64[:0]

	for len(src) > 0 {
		if src, err = fc.NextField(src); err != nil {
			return errors.Wrap(err, "read sample field")
		}

		switch fc.FieldNum {
		case fieldSampleStack:
			if s.StackIndex, err = takeInt32(&fc, "sample stack index"); err != nil {
				return err
			}
		case fieldSampleLink:
			if s.LinkIndex, err = takeInt32(&fc, "sample link index"); err != nil {
				return err
			}
		case fieldSampleAttributes:
			if idx, err = takeInt32s(&fc, idx, "sample attribute indices"); err != nil {
				return err
			}
		case fieldSampleValues:
			v, ok := fc.UnpackInt64s(values)
			if !ok {
				return errors.New("read sample values")
			}

			values = v
		case fieldSampleTimestamps:
			v, ok := fc.UnpackFixed64s(stamps)
			if !ok {
				return errors.New("read sample timestamps")
			}

			stamps = v
		}
	}

	c.i32, c.i64, c.u64 = idx, values, stamps

	s.AttributeIndices = c.ownIndices(idx)

	if len(values) > 0 {
		s.Values = append(c.values.Alloc(len(values)), values...)
	}

	if len(stamps) > 0 {
		s.TimestampsUnixNano = append(c.stamps.Alloc(len(stamps)), stamps...)
	}

	return nil
}

// ownIndices copies index scratch into the arena, so the batch keeps a slice the next element's
// scratch reuse cannot overwrite.
func (c *ProfilesConverter) ownIndices(idx []int32) []int32 {
	if len(idx) == 0 {
		return nil
	}

	return append(c.indices.Alloc(len(idx)), idx...)
}

func takeInt32(fc *easyproto.FieldContext, what string) (int32, error) {
	v, ok := fc.Int32()
	if !ok {
		return 0, errors.Errorf("read %s", what)
	}

	return v, nil
}

func takeInt64(fc *easyproto.FieldContext, what string) (int64, error) {
	v, ok := fc.Int64()
	if !ok {
		return 0, errors.Errorf("read %s", what)
	}

	return v, nil
}

func takeUint64(fc *easyproto.FieldContext, what string) (uint64, error) {
	v, ok := fc.Uint64()
	if !ok {
		return 0, errors.Errorf("read %s", what)
	}

	return v, nil
}

func takeInt32s(fc *easyproto.FieldContext, dst []int32, what string) ([]int32, error) {
	v, ok := fc.UnpackInt32s(dst)
	if !ok {
		return dst, errors.Errorf("read %s", what)
	}

	return v, nil
}
