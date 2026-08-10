package hareceiver

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/extension/xextension/storage"
)

// cursorState locates the next journal entry to read.
//
// Home Assistant returns only the cursor of the first entry of a response, in
// the X-First-Cursor header, so the position cannot be expressed as a single
// cursor. Instead it is an anchor cursor plus the number of entries already
// consumed after it: the next request skips Skip entries past Anchor. Every
// response re-anchors on its own first entry, so Skip stays bounded by the
// batch size.
type cursorState struct {
	Anchor string `json:"anchor"`
	Skip   int    `json:"skip"`
}

// rangeHeader renders the state as a systemd-journal-gatewayd Range header.
func (s cursorState) rangeHeader(batch int) string {
	return "entries=" + s.Anchor + ":" + strconv.Itoa(s.Skip) + ":" + strconv.Itoa(batch)
}

// tailRangeHeader anchors a source that has no stored cursor at the end of the
// journal.
//
// "entries=:-1:N" starts one entry before the last and returns N, so N must be
// 2 to cover the last entry: "entries=:-1:1" would anchor on the second-to-last
// one and the first poll would re-emit the last as if it were new. Supervisor
// avoids the same trap by clamping its own "lines" parameter to 2.
const tailRangeHeader = "entries=:-1:2"

// advance moves the state past the n entries of a response whose first entry
// had the given cursor.
func (s cursorState) advance(firstCursor string, n int) cursorState {
	if firstCursor == "" {
		// Should not happen while a response has entries, but staying on the
		// old anchor is still correct.
		return cursorState{Anchor: s.Anchor, Skip: s.Skip + n}
	}
	return cursorState{Anchor: firstCursor, Skip: n}
}

func loadCursor(ctx context.Context, client storage.Client, key string) (cursorState, error) {
	data, err := client.Get(ctx, key)
	if err != nil {
		return cursorState{}, errors.Wrap(err, "get")
	}
	if len(data) == 0 {
		return cursorState{}, nil
	}
	var s cursorState
	if err := json.Unmarshal(data, &s); err != nil {
		return cursorState{}, errors.Wrap(err, "unmarshal")
	}
	return s, nil
}

func saveCursor(ctx context.Context, client storage.Client, key string, s cursorState) error {
	data, err := json.Marshal(s)
	if err != nil {
		return errors.Wrap(err, "marshal")
	}
	if err := client.Set(ctx, key, data); err != nil {
		return errors.Wrap(err, "set")
	}
	return nil
}
