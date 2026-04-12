module github.com/go-faster/oteldb

go 1.26.1

tool (
	github.com/bufbuild/buf/cmd/buf
	github.com/ogen-go/ogen/cmd/jschemagen
	github.com/ogen-go/ogen/cmd/ogen
	golang.org/x/tools/cmd/stringer
	google.golang.org/protobuf/cmd/protoc-gen-go
)

require (
	github.com/ClickHouse/ch-go v0.71.0
	github.com/MakeNowJust/heredoc/v2 v2.0.1
	github.com/Masterminds/sprig/v3 v3.3.0
	github.com/VictoriaMetrics/VictoriaMetrics v1.139.0
	github.com/VictoriaMetrics/easyproto v1.2.0
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/cheggaaa/pb/v3 v3.1.7
	github.com/docker/cli v29.4.0+incompatible
	github.com/dustin/go-humanize v1.0.1
	github.com/fatih/color v1.19.0
	github.com/go-faster/errors v0.7.1
	github.com/go-faster/jx v1.2.0
	github.com/go-faster/sdk v0.34.0
	github.com/go-faster/yaml v0.4.6
	github.com/go-logfmt/logfmt v0.6.1
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v1.0.0
	github.com/google/go-cmp v0.7.0
	github.com/google/uuid v1.6.0
	github.com/grafana/loki/pkg/push v0.0.0-20241223090937-bf7301470453
	github.com/grafana/pyroscope-go v1.2.8
	github.com/klauspost/compress v1.18.5
	github.com/kr/logfmt v0.0.0-20210122060352-19f9bcb100e6
	github.com/mattn/go-isatty v0.0.21
	github.com/moby/moby/api v1.54.1
	github.com/moby/moby/client v0.4.0
	github.com/ogen-go/ogen v1.20.3
	github.com/open-telemetry/opentelemetry-collector-contrib/connector/countconnector v0.149.0
	github.com/open-telemetry/opentelemetry-collector-contrib/connector/exceptionsconnector v0.149.0
	github.com/open-telemetry/opentelemetry-collector-contrib/connector/failoverconnector v0.149.0
	github.com/open-telemetry/opentelemetry-collector-contrib/connector/roundrobinconnector v0.149.0
	github.com/open-telemetry/opentelemetry-collector-contrib/connector/servicegraphconnector v0.149.0
	github.com/open-telemetry/opentelemetry-collector-contrib/connector/signaltometricsconnector v0.149.0
	github.com/open-telemetry/opentelemetry-collector-contrib/connector/sumconnector v0.149.0
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/basicauthextension v0.149.0
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/bearertokenauthextension v0.149.0
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/oidcauthextension v0.149.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil v0.149.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributesprocessor v0.149.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor v0.149.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/metricstransformprocessor v0.149.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourceprocessor v0.149.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor v0.149.0
	github.com/oteldb/promql-engine v0.6.0-alpha.0
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/common v0.67.5
	github.com/prometheus/prometheus v0.311.1
	github.com/schollz/progressbar/v3 v3.19.0
	github.com/sergi/go-diff v1.4.0
	github.com/spf13/cobra v1.10.2
	github.com/spf13/pflag v1.0.10
	github.com/stretchr/testify v1.11.1
	github.com/testcontainers/testcontainers-go v0.42.0
	github.com/valyala/bytebufferpool v1.0.0
	github.com/zeebo/xxh3 v1.1.0
	go.opentelemetry.io/collector/component v1.55.0
	go.opentelemetry.io/collector/component/componentstatus v0.149.0
	go.opentelemetry.io/collector/component/componenttest v0.149.0
	go.opentelemetry.io/collector/config/confighttp v0.149.0
	go.opentelemetry.io/collector/config/confignet v1.55.0
	go.opentelemetry.io/collector/config/configoptional v1.55.0
	go.opentelemetry.io/collector/config/configtls v1.55.0
	go.opentelemetry.io/collector/confmap v1.55.0
	go.opentelemetry.io/collector/confmap/provider/envprovider v1.55.0
	go.opentelemetry.io/collector/connector v0.149.0
	go.opentelemetry.io/collector/consumer v1.55.0
	go.opentelemetry.io/collector/consumer/consumertest v0.149.0
	go.opentelemetry.io/collector/exporter v1.55.0
	go.opentelemetry.io/collector/exporter/exporterhelper v0.149.0
	go.opentelemetry.io/collector/extension v1.55.0
	go.opentelemetry.io/collector/otelcol v0.149.0
	go.opentelemetry.io/collector/pdata v1.55.0
	go.opentelemetry.io/collector/pipeline v1.55.0
	go.opentelemetry.io/collector/processor v1.55.0
	go.opentelemetry.io/collector/processor/batchprocessor v0.149.0
	go.opentelemetry.io/collector/receiver v1.55.0
	go.opentelemetry.io/collector/receiver/otlpreceiver v0.149.0
	go.opentelemetry.io/collector/receiver/receiverhelper v0.149.0
	go.opentelemetry.io/collector/receiver/receivertest v0.149.0
	go.opentelemetry.io/collector/service v0.149.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.68.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.68.0
	go.opentelemetry.io/contrib/zpages v0.68.0
	go.opentelemetry.io/otel v1.43.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.43.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.43.0
	go.opentelemetry.io/otel/log v0.19.0
	go.opentelemetry.io/otel/metric v1.43.0
	go.opentelemetry.io/otel/sdk v1.43.0
	go.opentelemetry.io/otel/sdk/metric v1.43.0
	go.opentelemetry.io/otel/trace v1.43.0
	go.opentelemetry.io/proto/otlp v1.10.0
	go.uber.org/atomic v1.11.0
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.27.1
	go4.org/netipx v0.0.0-20231129151722-fdeea329fbba
	golang.org/x/exp v0.0.0-20260312153236-7ab1446f8b90
	golang.org/x/perf v0.0.0-20241204221936-711ff2ab7231
	golang.org/x/sync v0.20.0
	google.golang.org/grpc v1.80.0
	google.golang.org/protobuf v1.36.11
	gopkg.in/yaml.v2 v2.4.0
	sigs.k8s.io/yaml v1.6.0
)

require (
	buf.build/gen/go/bufbuild/bufplugin/protocolbuffers/go v1.36.11-20250718181942-e35f9b667443.1 // indirect
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.11-20260209202127-80ab13bee0bf.1 // indirect
	buf.build/gen/go/bufbuild/registry/connectrpc/go v1.19.1-20260126144947-819582968857.2 // indirect
	buf.build/gen/go/bufbuild/registry/protocolbuffers/go v1.36.11-20260126144947-819582968857.1 // indirect
	buf.build/gen/go/pluginrpc/pluginrpc/protocolbuffers/go v1.36.11-20241007202033-cf42259fcbfc.1 // indirect
	buf.build/go/app v0.2.0 // indirect
	buf.build/go/bufplugin v0.9.0 // indirect
	buf.build/go/bufprivateusage v0.1.0 // indirect
	buf.build/go/interrupt v1.1.0 // indirect
	buf.build/go/protovalidate v1.1.3 // indirect
	buf.build/go/protoyaml v0.6.0 // indirect
	buf.build/go/spdx v0.2.0 // indirect
	buf.build/go/standard v0.1.1-0.20260325175353-2b287e071df5 // indirect
	cel.dev/expr v0.25.1 // indirect
	connectrpc.com/connect v1.19.1 // indirect
	connectrpc.com/otelconnect v0.9.0 // indirect
	dario.cat/mergo v1.0.2 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20250102033503-faa5f7b0171c // indirect
	github.com/GehirnInc/crypt v0.0.0-20230320061759-8cc1b52080c5 // indirect
	github.com/KimMachineGun/automemlimit v0.7.5 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.4.0 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/VictoriaMetrics/metrics v1.42.0 // indirect
	github.com/VictoriaMetrics/metricsql v0.85.0 // indirect
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/aclements/go-moremath v0.0.0-20210112150236-f10218a38794 // indirect
	github.com/alecthomas/participle/v2 v2.1.4 // indirect
	github.com/antchfx/xmlquery v1.5.1 // indirect
	github.com/antchfx/xpath v1.3.6 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bufbuild/buf v1.67.0 // indirect
	github.com/bufbuild/protocompile v0.14.2-0.20260319203231-019757e4c592 // indirect
	github.com/bufbuild/protoplugin v0.0.0-20250218205857-750e09ce93e1 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cli/browser v1.3.0 // indirect
	github.com/clipperhouse/uax29/v2 v2.7.0 // indirect
	github.com/containerd/errdefs v1.0.0 // indirect
	github.com/containerd/errdefs/pkg v0.3.0 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/containerd/platforms v0.2.1 // indirect
	github.com/containerd/stargz-snapshotter/estargz v0.18.2 // indirect
	github.com/coreos/go-oidc/v3 v3.17.0 // indirect
	github.com/cpuguy83/dockercfg v0.3.2 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.7 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dennwc/varint v1.0.0 // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/dlclark/regexp2 v1.11.5 // indirect
	github.com/dmarkham/enumer v1.6.3 // indirect
	github.com/docker/distribution v2.8.3+incompatible // indirect
	github.com/docker/docker-credential-helpers v0.9.5 // indirect
	github.com/docker/go-connections v0.6.0 // indirect
	github.com/docker/go-metrics v0.0.1 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/ebitengine/purego v0.10.0 // indirect
	github.com/edsrzf/mmap-go v1.2.0 // indirect
	github.com/efficientgo/core v1.0.0-rc.2 // indirect
	github.com/elastic/go-grok v0.3.1 // indirect
	github.com/elastic/lunes v0.2.0 // indirect
	github.com/expr-lang/expr v1.17.8 // indirect
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/foxboron/go-tpm-keyfiles v0.0.0-20251226215517-609e4778396f // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/fvbommel/sortorder v1.1.0 // indirect
	github.com/fxamacker/cbor/v2 v2.9.0 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-jose/go-jose/v4 v4.1.4 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-logr/zapr v1.3.0 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/gofrs/flock v0.13.0 // indirect
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/google/cel-go v0.27.0 // indirect
	github.com/google/go-containerregistry v0.21.3 // indirect
	github.com/google/go-tpm v0.9.8 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/grafana/otel-profiling-go v0.5.1 // indirect
	github.com/grafana/pyroscope-go/godeltaprof v0.1.9 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.28.0 // indirect
	github.com/hashicorp/go-version v1.8.0 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/huandu/xstrings v1.5.0 // indirect
	github.com/iancoleman/strcase v0.3.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jdx/go-netrc v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/pgzip v1.2.6 // indirect
	github.com/knadh/koanf v1.5.0 // indirect
	github.com/knadh/koanf/v2 v2.3.4 // indirect
	github.com/lightstep/go-expohisto v1.0.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20251013123823-9fd1530e3ec3 // indirect
	github.com/magefile/mage v1.15.0 // indirect
	github.com/magiconair/properties v1.8.10 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-runewidth v0.0.20 // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/go-archive v0.2.0 // indirect
	github.com/moby/patternmatcher v0.6.1 // indirect
	github.com/moby/sys/atomicwriter v0.1.0 // indirect
	github.com/moby/sys/sequential v0.6.0 // indirect
	github.com/moby/sys/user v0.4.0 // indirect
	github.com/moby/sys/userns v0.1.0 // indirect
	github.com/moby/term v0.5.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/morikuni/aec v1.1.0 // indirect
	github.com/mostynb/go-grpc-compression v1.2.3 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/internal/credentialsfile v0.149.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal v0.149.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter v0.149.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/pdatautil v0.149.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl v0.149.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/sampling v0.149.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/pascaldekloe/name v1.0.1 // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/petermattis/goid v0.0.0-20260226131333-17d1149c6ac6 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/otlptranslator v1.0.0 // indirect
	github.com/prometheus/procfs v0.20.1 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.59.0 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/rs/cors v1.11.1 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/samber/lo v1.52.0 // indirect
	github.com/samber/slog-common v0.20.0 // indirect
	github.com/samber/slog-zap/v2 v2.6.3 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/segmentio/encoding v0.5.4 // indirect
	github.com/shirou/gopsutil/v4 v4.26.3 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/sirupsen/logrus v1.9.4 // indirect
	github.com/spf13/cast v1.7.0 // indirect
	github.com/tetratelabs/wazero v1.11.0 // indirect
	github.com/tg123/go-htpasswd v1.2.4 // indirect
	github.com/tidwall/btree v1.8.1 // indirect
	github.com/tklauser/go-sysconf v0.3.16 // indirect
	github.com/tklauser/numcpus v0.11.0 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/ua-parser/uap-go v0.0.0-20251207011819-db9adb27a0b8 // indirect
	github.com/valyala/fastjson v1.6.10 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	github.com/valyala/quicktemplate v1.8.0 // indirect
	github.com/vbatts/tar-split v0.12.2 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.lsp.dev/jsonrpc2 v0.10.0 // indirect
	go.lsp.dev/pkg v0.0.0-20210717090340-384b27a52fb2 // indirect
	go.lsp.dev/protocol v0.12.0 // indirect
	go.lsp.dev/uri v0.3.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/collector v0.149.0 // indirect
	go.opentelemetry.io/collector/client v1.55.0 // indirect
	go.opentelemetry.io/collector/config/configauth v1.55.0 // indirect
	go.opentelemetry.io/collector/config/configcompression v1.55.0 // indirect
	go.opentelemetry.io/collector/config/configgrpc v0.149.0 // indirect
	go.opentelemetry.io/collector/config/configmiddleware v1.55.0 // indirect
	go.opentelemetry.io/collector/config/configopaque v1.55.0 // indirect
	go.opentelemetry.io/collector/config/configretry v1.55.0 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.149.0 // indirect
	go.opentelemetry.io/collector/confmap/xconfmap v0.149.0 // indirect
	go.opentelemetry.io/collector/connector/connectortest v0.149.0 // indirect
	go.opentelemetry.io/collector/connector/xconnector v0.149.0 // indirect
	go.opentelemetry.io/collector/consumer/consumererror v0.149.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.149.0 // indirect
	go.opentelemetry.io/collector/exporter/exportertest v0.149.0 // indirect
	go.opentelemetry.io/collector/exporter/xexporter v0.149.0 // indirect
	go.opentelemetry.io/collector/extension/extensionauth v1.55.0 // indirect
	go.opentelemetry.io/collector/extension/extensioncapabilities v0.149.0 // indirect
	go.opentelemetry.io/collector/extension/extensionmiddleware v0.149.0 // indirect
	go.opentelemetry.io/collector/extension/extensiontest v0.149.0 // indirect
	go.opentelemetry.io/collector/extension/xextension v0.149.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.55.0 // indirect
	go.opentelemetry.io/collector/internal/componentalias v0.149.0 // indirect
	go.opentelemetry.io/collector/internal/fanoutconsumer v0.149.0 // indirect
	go.opentelemetry.io/collector/internal/sharedcomponent v0.149.0 // indirect
	go.opentelemetry.io/collector/internal/telemetry v0.149.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.149.0 // indirect
	go.opentelemetry.io/collector/pdata/testdata v0.149.0 // indirect
	go.opentelemetry.io/collector/pdata/xpdata v0.149.0 // indirect
	go.opentelemetry.io/collector/pipeline/xpipeline v0.149.0 // indirect
	go.opentelemetry.io/collector/processor/processorhelper v0.149.0 // indirect
	go.opentelemetry.io/collector/processor/processorhelper/xprocessorhelper v0.149.0 // indirect
	go.opentelemetry.io/collector/processor/processortest v0.149.0 // indirect
	go.opentelemetry.io/collector/processor/xprocessor v0.149.0 // indirect
	go.opentelemetry.io/collector/receiver/xreceiver v0.149.0 // indirect
	go.opentelemetry.io/collector/service/hostcapabilities v0.149.0 // indirect
	go.opentelemetry.io/contrib/bridges/otelzap v0.17.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/runtime v0.65.0 // indirect
	go.opentelemetry.io/contrib/otelconf v0.22.0 // indirect
	go.opentelemetry.io/contrib/propagators/autoprop v0.67.0 // indirect
	go.opentelemetry.io/contrib/propagators/aws v1.42.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.42.0 // indirect
	go.opentelemetry.io/contrib/propagators/jaeger v1.42.0 // indirect
	go.opentelemetry.io/contrib/propagators/ot v1.42.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc v0.18.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp v0.19.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.42.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.43.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.43.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.64.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutlog v0.18.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.42.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.42.0 // indirect
	go.opentelemetry.io/otel/sdk/log v0.19.0 // indirect
	go.uber.org/automaxprocs v1.6.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/crypto v0.50.0 // indirect
	golang.org/x/mod v0.35.0 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/term v0.42.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	golang.org/x/tools v0.44.0 // indirect
	gonum.org/v1/gonum v0.17.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260401024825-9d38bb4040a9 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260406210006-6f92a3bedf2d // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/apimachinery v0.35.3 // indirect
	k8s.io/client-go v0.35.3 // indirect
	k8s.io/klog/v2 v2.140.0 // indirect
	k8s.io/kube-openapi v0.0.0-20250910181357-589584f1c912 // indirect
	k8s.io/utils v0.0.0-20260210185600-b8788abfbbc2 // indirect
	mvdan.cc/xurls/v2 v2.6.0 // indirect
	pluginrpc.com/pluginrpc v0.5.0 // indirect
	sigs.k8s.io/json v0.0.0-20250730193827-2d320260d730 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
	sigs.k8s.io/structured-merge-diff/v6 v6.3.0 // indirect
)
