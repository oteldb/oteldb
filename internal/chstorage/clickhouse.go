package chstorage

import (
	"context"

	"github.com/ClickHouse/ch-go"
)

type ClickHouseClient interface {
	Do(ctx context.Context, q ch.Query) error
	Ping(ctx context.Context) error
}

type dialingClickhouseClient struct {
	options ch.Options
}

func (c *dialingClickhouseClient) Ping(ctx context.Context) error {
	db, err := ch.Dial(ctx, c.options)
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()
	return db.Ping(ctx)
}

var _ ClickHouseClient = (*dialingClickhouseClient)(nil)

func NewDialingClickhouseClient(options ch.Options) ClickHouseClient {
	return &dialingClickhouseClient{
		options: options,
	}
}

func (c *dialingClickhouseClient) Do(ctx context.Context, q ch.Query) error {
	db, err := ch.Dial(ctx, c.options)
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()
	return db.Do(ctx, q)
}

// TODO: rewrite or disable
var _ = NewDialingClickhouseClient
