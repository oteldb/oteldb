// Copyright  The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package prometheusremotewritereceiver implements Prometheus Remote Write API for OTLP collector.
package prometheusremotewritereceiver

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"

	"github.com/golang/snappy"
	"github.com/valyala/bytebufferpool"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/otelreceiver/prometheusremotewrite"
	"github.com/go-faster/oteldb/internal/prompb"
	"github.com/go-faster/oteldb/internal/xsync"
)

const receiverFormat = "protobuf"

// Receiver is a Prometheus remote write receiver.
type Receiver struct {
	params       receiver.Settings
	host         component.Host
	nextConsumer consumer.Metrics

	mu         sync.Mutex
	startOnce  sync.Once
	stopOnce   sync.Once
	shutdownWG sync.WaitGroup

	server        *http.Server
	config        *Config
	timeThreshold *int64
	logger        *zap.Logger
	obsrecv       *receiverhelper.ObsReport
}

// NewReceiver creates new [Start].
func NewReceiver(params receiver.Settings, config *Config, mconsumer consumer.Metrics) (*Receiver, error) {
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             params.ID,
		Transport:              "http",
		ReceiverCreateSettings: params,
	})
	zr := &Receiver{
		params:        params,
		nextConsumer:  mconsumer,
		config:        config,
		logger:        params.Logger,
		obsrecv:       obsrecv,
		timeThreshold: &config.TimeThreshold,
	}
	return zr, err
}

var _ component.Component = (*Receiver)(nil)

// Start implements [component.Component].
func (rec *Receiver) Start(ctx context.Context, host component.Host) (err error) {
	if host == nil {
		return errors.New("nil host")
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()

	rec.startOnce.Do(func() {
		rec.host = host
		rec.server, err = rec.config.ServerConfig.ToServer(ctx,
			host.GetExtensions(),
			rec.params.TelemetrySettings,
			rec,
			confighttp.WithDecoder("snappy", snappyDecoder),
		)
		var listener net.Listener
		listener, err = rec.config.ServerConfig.ToListener(ctx)
		if err != nil {
			return
		}
		rec.shutdownWG.Add(1)
		go func() {
			defer rec.shutdownWG.Done()
			if errHTTP := rec.server.Serve(listener); !errors.Is(errHTTP, http.ErrServerClosed) {
				componentstatus.ReportStatus(rec.host, componentstatus.NewFatalErrorEvent(errHTTP))
			}
		}()
	})
	return err
}

func snappyDecoder(body io.ReadCloser) (io.ReadCloser, error) {
	compressed := bytebufferpool.Get()
	defer bytebufferpool.Put(compressed)

	if _, err := io.Copy(compressed, body); err != nil {
		return nil, err
	}

	decompressed, err := snappy.Decode(nil, compressed.Bytes())
	if err != nil {
		return nil, err
	}

	return &closerReader{
		data:   decompressed,
		Reader: *bytes.NewReader(decompressed),
	}, nil
}

func decodeRequest(r io.Reader, bb *bytebufferpool.ByteBuffer, rw *prompb.WriteRequest) error {
	switch r := r.(type) {
	case *closerReader:
		// Do not make an unnecessary copy of data.
		bb.Set(r.data)
	default:
		if _, err := bb.ReadFrom(r); err != nil {
			return err
		}
	}
	return rw.Unmarshal(bb.B)
}

func (rec *Receiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := rec.obsrecv.StartMetricsOp(r.Context())

	bb := bytebufferpool.Get()
	defer bytebufferpool.Put(bb)

	wr := xsync.GetReset(writeRequestPool)
	defer writeRequestPool.Put(wr)

	if err := decodeRequest(r.Body, bb, wr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	pms, err := prometheusremotewrite.FromTimeSeries(wr.Timeseries, prometheusremotewrite.Settings{
		TimeThreshold: *rec.timeThreshold,
		Logger:        *rec.logger,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	metricCount := pms.ResourceMetrics().Len()
	dataPointCount := pms.DataPointCount()
	if metricCount != 0 {
		err = rec.nextConsumer.ConsumeMetrics(ctx, pms)
	}
	rec.obsrecv.EndMetricsOp(ctx, receiverFormat, dataPointCount, err)
	w.WriteHeader(http.StatusAccepted)
}

// Shutdown implements [component.Component].
func (rec *Receiver) Shutdown(context.Context) (err error) {
	rec.stopOnce.Do(func() {
		err = rec.server.Close()
		rec.shutdownWG.Wait()
	})
	return err
}
