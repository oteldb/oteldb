package globalmetric

import (
	"context"

	"github.com/ClickHouse/ch-go"
)

var _ Tracker = (*noopTracker)(nil)

type noopTracker struct{}

var _ Track = (*noopTrack)(nil)

type noopTrack struct{}

func (n noopTrack) End() {}

func (n noopTrack) OnProfiles(ctx context.Context, events []ch.ProfileEvent) error { return nil }

func (n noopTracker) Start(ctx context.Context, opts ...TrackOption) (context.Context, Track) {
	return ctx, noopTrack{}
}

func NewNoopTracker() Tracker {
	return noopTracker{}
}
