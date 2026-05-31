package main

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/oteldb/internal/chstorage"
	"github.com/go-faster/sdk/zctx"
)

func getRowCount(ctx context.Context, client chstorage.ClickHouseClient, table string) (uint64, error) {
	var count proto.ColUInt64
	err := client.Do(ctx, ch.Query{
		Logger: zctx.From(ctx).Named("ch"),
		Body:   fmt.Sprintf("SELECT count() FROM `%s`", table),
		Result: proto.Results{
			{Name: "count()", Data: &count},
		},
	})
	if err != nil {
		return 0, err
	}
	if count.Rows() == 0 {
		return 0, nil
	}
	return count.Row(0), nil
}
