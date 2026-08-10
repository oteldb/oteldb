package hareceiver

import "time"

// recombineEntries joins entries that are fragments of one logical event.
//
// journald splits a multi-line message at every newline, so a Python traceback
// arrives as one entry per line, each with a full envelope, the same identifier
// and PID, and near-identical timestamps. Emitted as-is that is one severity-less
// record per stack frame.
//
// Only an application log line opens a block that can absorb fragments. Without
// that restriction every source that never uses the format — systemd, kernel,
// CoreDNS — would have entire runs of unrelated entries merged, since none of
// them starts a new application log line.
//
// A fragment must also come from the same process and follow within window. The
// exact window barely matters: measured over 11.5k captured entries the gap
// between fragments was 58ms at p99, and anything from 100ms to 1s produced
// byte-identical output. Only 4 of 491 fragments exceeded 1s, all of them lines
// that merely followed an unrelated entry from the same process, so ending the
// block there is the wanted outcome rather than a loss.
//
// Fragments are only ever appended to an entry already in the batch, so the
// journal entry count — which the cursor arithmetic depends on — is unchanged.
func recombineEntries(entries []Entry, window time.Duration) []Entry {
	if len(entries) < 2 {
		return entries
	}

	var (
		out = entries[:0:0]
		// open indexes the entry fragments are being appended to, or -1 when
		// the previous entry cannot absorb any.
		open = -1
		last time.Time
	)
	for _, e := range entries {
		_, isApp := parseAppMessage(e.Message)
		if !isApp && open >= 0 && continuesEntry(out[open], e, last, window) {
			out[open].Message += "\n" + e.Message
			last = e.Timestamp
			continue
		}

		out = append(out, e)
		if isApp {
			open, last = len(out)-1, e.Timestamp
		} else {
			open = -1
		}
	}
	return out
}

// continuesEntry reports whether cur is a fragment of the block opened by prev,
// whose newest fragment arrived at last.
func continuesEntry(prev, cur Entry, last time.Time, window time.Duration) bool {
	if prev.Identifier != cur.Identifier || prev.HasPID != cur.HasPID || prev.PID != cur.PID {
		return false
	}
	d := cur.Timestamp.Sub(last)
	return d >= 0 && d <= window
}
