package chstorage

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func (i *Inserter) totals(ctx context.Context, tableName string) (int64, error) {
	var totals proto.ColInt64
	q := ch.Query{
		Body: fmt.Sprintf("SELECT count() as count FROM `%s`", tableName),
		Result: proto.Results{
			{
				Name: "count",
				Data: &totals,
			},
		},
	}
	if err := i.ch.Do(ctx, q); err != nil {
		return 0, err
	}
	if totals.Rows() == 0 {
		return 0, nil
	}

	return totals.Row(0), nil
}
