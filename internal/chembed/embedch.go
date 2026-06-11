// Package embedch implements embedded clickhouse.
//
// Parts of https://github.com/ClickHouse/ch-go/tree/main/cht were used.
package chembed

import (
	"bytes"
	"encoding/xml"
	"os"

	"github.com/go-faster/errors"
)

func writeXML(name string, v any) error {
	buf := new(bytes.Buffer)
	e := xml.NewEncoder(buf)
	e.Indent("", "  ")
	if err := e.Encode(v); err != nil {
		return errors.Wrap(err, "encode")
	}
	if err := os.WriteFile(name, buf.Bytes(), 0o600); err != nil {
		return errors.Wrap(err, "write")
	}

	return nil
}
