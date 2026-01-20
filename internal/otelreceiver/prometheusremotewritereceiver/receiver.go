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
	"context"
	"errors"
	"net"
	"net/http"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/prometheusremotewrite"
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
			confighttp.WithDecoder("snappy", prometheusremotewrite.SnappyDecoder),
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

func (rec *Receiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := rec.obsrecv.StartMetricsOp(r.Context())

	pms, err := prometheusremotewrite.DecodeRequest(r.Body, prometheusremotewrite.Settings{
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
