module github.com/oteldb/oteldb

go 1.26.2

tool (
	github.com/bufbuild/buf/cmd/buf
	github.com/ogen-go/ogen/cmd/jschemagen
	github.com/ogen-go/ogen/cmd/ogen
	go.opentelemetry.io/collector/cmd/builder
	golang.org/x/tools/cmd/stringer
	google.golang.org/protobuf/cmd/protoc-gen-go
)

require (
	github.com/ClickHouse/ch-go v0.72.0
	github.com/MakeNowJust/heredoc/v2 v2.0.1
	github.com/Masterminds/sprig/v3 v3.3.0
	github.com/VictoriaMetrics/VictoriaMetrics v1.141.0
	github.com/VictoriaMetrics/easyproto v1.2.0
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/cheggaaa/pb/v3 v3.1.7
	github.com/docker/cli v29.5.3+incompatible
	github.com/dustin/go-humanize v1.0.1
	github.com/fatih/color v1.19.0
	github.com/go-faster/errors v0.7.1
	github.com/go-faster/jx v1.2.0
	github.com/go-faster/sdk v0.35.0
	github.com/go-faster/yaml v0.4.6
	github.com/go-logfmt/logfmt v0.6.1
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v1.0.0
	github.com/google/go-cmp v0.7.0
	github.com/google/uuid v1.6.0
	github.com/grafana/loki/pkg/push v0.0.0-20241223090937-bf7301470453
	github.com/grafana/pyroscope-go v1.3.1
	github.com/klauspost/compress v1.18.6
	github.com/kr/logfmt v0.0.0-20210122060352-19f9bcb100e6
	github.com/mattn/go-isatty v0.0.22
	github.com/maypok86/otter v1.2.4
	github.com/moby/moby/api v1.55.0
	github.com/moby/moby/client v0.4.1
	github.com/ogen-go/ogen v1.22.0
	github.com/open-telemetry/opentelemetry-collector-contrib/connector/countconnector v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/connector/exceptionsconnector v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/connector/failoverconnector v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/connector/roundrobinconnector v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/connector/servicegraphconnector v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/connector/signaltometricsconnector v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/connector/sumconnector v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/basicauthextension v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/bearertokenauthextension v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckextension v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/oidcauthextension v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/pprofextension v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributesprocessor v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/groupbyattrsprocessor v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/k8sattributesprocessor v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/metricstransformprocessor v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourcedetectionprocessor v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourceprocessor v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/receiver/dockerstatsreceiver v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/receiver/filelogreceiver v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/receiver/journaldreceiver v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8seventsreceiver v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkareceiver v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/receiver/syslogreceiver v0.154.0
	github.com/open-telemetry/opentelemetry-collector-contrib/receiver/webhookeventreceiver v0.154.0
	github.com/oteldb/promql-engine v0.7.0-alpha.0
	github.com/pierrec/lz4/v4 v4.1.27
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/common v0.69.0
	github.com/prometheus/prometheus v0.312.0
	github.com/schollz/progressbar/v3 v3.19.0
	github.com/sergi/go-diff v1.4.0
	github.com/spf13/cobra v1.10.2
	github.com/spf13/pflag v1.0.10
	github.com/stretchr/testify v1.11.1
	github.com/testcontainers/testcontainers-go v0.42.0
	github.com/valyala/bytebufferpool v1.0.0
	github.com/zeebo/xxh3 v1.1.0
	go.opentelemetry.io/collector/component v1.60.0
	go.opentelemetry.io/collector/component/componentstatus v0.154.0
	go.opentelemetry.io/collector/component/componenttest v0.154.0
	go.opentelemetry.io/collector/config/confighttp v0.154.0
	go.opentelemetry.io/collector/config/confignet v1.60.0
	go.opentelemetry.io/collector/config/configoptional v1.60.0
	go.opentelemetry.io/collector/config/configtls v1.60.0
	go.opentelemetry.io/collector/confmap v1.60.0
	go.opentelemetry.io/collector/confmap/provider/envprovider v1.60.0
	go.opentelemetry.io/collector/confmap/provider/fileprovider v1.60.0
	go.opentelemetry.io/collector/confmap/provider/httpprovider v1.60.0
	go.opentelemetry.io/collector/confmap/provider/httpsprovider v1.60.0
	go.opentelemetry.io/collector/confmap/provider/yamlprovider v1.60.0
	go.opentelemetry.io/collector/connector v0.154.0
	go.opentelemetry.io/collector/consumer v1.60.0
	go.opentelemetry.io/collector/consumer/consumertest v0.154.0
	go.opentelemetry.io/collector/exporter v1.60.0
	go.opentelemetry.io/collector/exporter/debugexporter v0.154.0
	go.opentelemetry.io/collector/exporter/exporterhelper v0.154.0
	go.opentelemetry.io/collector/exporter/nopexporter v0.154.0
	go.opentelemetry.io/collector/exporter/otlpexporter v0.154.0
	go.opentelemetry.io/collector/exporter/otlphttpexporter v0.154.0
	go.opentelemetry.io/collector/extension v1.60.0
	go.opentelemetry.io/collector/extension/zpagesextension v0.154.0
	go.opentelemetry.io/collector/otelcol v0.154.0
	go.opentelemetry.io/collector/pdata v1.60.0
	go.opentelemetry.io/collector/pipeline v1.60.0
	go.opentelemetry.io/collector/processor v1.60.0
	go.opentelemetry.io/collector/processor/batchprocessor v0.154.0
	go.opentelemetry.io/collector/processor/memorylimiterprocessor v0.154.0
	go.opentelemetry.io/collector/receiver v1.60.0
	go.opentelemetry.io/collector/receiver/otlpreceiver v0.154.0
	go.opentelemetry.io/collector/receiver/receiverhelper v0.154.0
	go.opentelemetry.io/collector/receiver/receivertest v0.154.0
	go.opentelemetry.io/collector/service v0.154.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.69.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.69.0
	go.opentelemetry.io/contrib/zpages v0.69.0
	go.opentelemetry.io/otel v1.44.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.44.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.44.0
	go.opentelemetry.io/otel/log v0.20.0
	go.opentelemetry.io/otel/metric v1.44.0
	go.opentelemetry.io/otel/sdk v1.44.0
	go.opentelemetry.io/otel/sdk/metric v1.44.0
	go.opentelemetry.io/otel/trace v1.44.0
	go.opentelemetry.io/proto/otlp v1.10.0
	go.uber.org/atomic v1.11.0
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.28.0
	go4.org/netipx v0.0.0-20231129151722-fdeea329fbba
	golang.org/x/exp v0.0.0-20260603202125-055de637280b
	golang.org/x/perf v0.0.0-20260512194132-3cf34090a3db
	golang.org/x/sync v0.21.0
	google.golang.org/grpc v1.81.1
	google.golang.org/protobuf v1.36.12-0.20260120151049-f2248ac996af
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
	cloud.google.com/go/auth v0.20.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute v1.60.0 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	connectrpc.com/connect v1.19.1 // indirect
	connectrpc.com/otelconnect v0.9.0 // indirect
	dario.cat/mergo v1.0.2 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.21.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.13.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.12.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v5 v5.7.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v4 v4.3.0 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20250102033503-faa5f7b0171c // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.7.1 // indirect
	github.com/Code-Hex/go-generics-cache v1.5.1 // indirect
	github.com/DeRuina/timberjack v1.4.5 // indirect
	github.com/GehirnInc/crypt v0.0.0-20230320061759-8cc1b52080c5 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.33.0 // indirect
	github.com/KimMachineGun/automemlimit v0.7.5 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.5.0 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/Showmax/go-fqdn v1.0.0 // indirect
	github.com/VictoriaMetrics/metrics v1.43.2 // indirect
	github.com/VictoriaMetrics/metricsql v0.87.0 // indirect
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/aclements/go-moremath v0.0.0-20241023150245-c8bbc672ef66 // indirect
	github.com/alecthomas/participle/v2 v2.1.4 // indirect
	github.com/alecthomas/units v0.0.0-20240927000941-0f3dac36c52b // indirect
	github.com/antchfx/xmlquery v1.5.1 // indirect
	github.com/antchfx/xpath v1.3.6 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/apache/thrift v0.23.1-0.20260429145742-d2acd3c49e58 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/aws/aws-msk-iam-sasl-signer-go v1.0.4 // indirect
	github.com/aws/aws-sdk-go v1.55.8 // indirect
	github.com/aws/aws-sdk-go-v2 v1.41.10 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.32.21 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.20 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.26 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.26 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.26 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.27 // indirect
	github.com/aws/aws-sdk-go-v2/service/ec2 v1.305.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ecs v1.81.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/elasticache v1.52.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.26 // indirect
	github.com/aws/aws-sdk-go-v2/service/kafka v1.52.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/lightsail v1.54.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/rds v1.118.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.1.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.31.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.36.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.43.0 // indirect
	github.com/aws/smithy-go v1.26.0 // indirect
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/bboreham/go-loser v0.0.0-20230920113527-fcc2c21820a3 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bmatcuk/doublestar/v4 v4.10.0 // indirect
	github.com/bufbuild/buf v1.67.0 // indirect
	github.com/bufbuild/protocompile v0.14.2-0.20260319203231-019757e4c592 // indirect
	github.com/bufbuild/protoplugin v0.0.0-20250218205857-750e09ce93e1 // indirect
	github.com/buger/jsonparser v1.1.2 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cli/browser v1.3.0 // indirect
	github.com/clipperhouse/uax29/v2 v2.7.0 // indirect
	github.com/cncf/xds/go v0.0.0-20260202195803-dba9d589def2 // indirect
	github.com/containerd/errdefs v1.0.0 // indirect
	github.com/containerd/errdefs/pkg v0.3.0 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/containerd/platforms v0.2.1 // indirect
	github.com/containerd/stargz-snapshotter/estargz v0.18.2 // indirect
	github.com/coreos/go-oidc/v3 v3.18.0 // indirect
	github.com/coreos/go-systemd/v22 v22.7.0 // indirect
	github.com/cpuguy83/dockercfg v0.3.2 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.7 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dennwc/varint v1.0.0 // indirect
	github.com/digitalocean/go-metadata v0.0.0-20250129100319-e3650a3df44b // indirect
	github.com/digitalocean/godo v1.193.0 // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/dlclark/regexp2 v1.12.0 // indirect
	github.com/dmarkham/enumer v1.6.3 // indirect
	github.com/docker/distribution v2.8.3+incompatible // indirect
	github.com/docker/docker-credential-helpers v0.9.8 // indirect
	github.com/docker/go-connections v0.7.0 // indirect
	github.com/docker/go-metrics v0.0.1 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dolthub/maphash v0.1.0 // indirect
	github.com/ebitengine/purego v0.10.1 // indirect
	github.com/edsrzf/mmap-go v1.2.1-0.20241212181136-fad1cd13edbd // indirect
	github.com/efficientgo/core v1.0.0-rc.3 // indirect
	github.com/elastic/go-grok v0.3.1 // indirect
	github.com/elastic/lunes v0.2.2 // indirect
	github.com/emicklei/go-restful/v3 v3.13.0 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.37.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.3 // indirect
	github.com/expr-lang/expr v1.17.8 // indirect
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb // indirect
	github.com/felixge/fgprof v0.9.5 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/foxboron/go-tpm-keyfiles v0.0.0-20260427185012-515ba073c4c1 // indirect
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/fvbommel/sortorder v1.1.0 // indirect
	github.com/fxamacker/cbor/v2 v2.9.1 // indirect
	github.com/gammazero/deque v1.2.1 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-jose/go-jose/v4 v4.1.4 // indirect
	github.com/go-kit/log v0.2.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-logr/zapr v1.3.0 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-openapi/analysis v0.25.0 // indirect
	github.com/go-openapi/errors v0.22.7 // indirect
	github.com/go-openapi/jsonpointer v0.23.1 // indirect
	github.com/go-openapi/jsonreference v0.21.5 // indirect
	github.com/go-openapi/loads v0.23.3 // indirect
	github.com/go-openapi/spec v0.22.4 // indirect
	github.com/go-openapi/strfmt v0.26.2 // indirect
	github.com/go-openapi/swag v0.25.5 // indirect
	github.com/go-openapi/swag/cmdutils v0.25.5 // indirect
	github.com/go-openapi/swag/conv v0.25.5 // indirect
	github.com/go-openapi/swag/fileutils v0.25.5 // indirect
	github.com/go-openapi/swag/jsonname v0.26.0 // indirect
	github.com/go-openapi/swag/jsonutils v0.25.5 // indirect
	github.com/go-openapi/swag/loading v0.25.5 // indirect
	github.com/go-openapi/swag/mangling v0.25.5 // indirect
	github.com/go-openapi/swag/netutils v0.25.5 // indirect
	github.com/go-openapi/swag/stringutils v0.25.5 // indirect
	github.com/go-openapi/swag/typeutils v0.25.5 // indirect
	github.com/go-openapi/swag/yamlutils v0.25.5 // indirect
	github.com/go-openapi/validate v0.25.2 // indirect
	github.com/go-resty/resty/v2 v2.17.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/go-zookeeper/zk v1.0.4 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/gofrs/flock v0.13.0 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/google/cel-go v0.27.0 // indirect
	github.com/google/gnostic-models v0.7.1 // indirect
	github.com/google/go-containerregistry v0.21.3 // indirect
	github.com/google/go-querystring v1.2.0 // indirect
	github.com/google/go-tpm v0.9.9-0.20260124013517-8f8f42cba0de // indirect
	github.com/google/pprof v0.0.0-20260507013755-92041b743c96 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.15 // indirect
	github.com/googleapis/gax-go/v2 v2.22.0 // indirect
	github.com/gophercloud/gophercloud/v2 v2.12.0 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/gorilla/websocket v1.5.4-0.20250319132907-e064f32e3674 // indirect
	github.com/grafana/otel-profiling-go v0.5.3 // indirect
	github.com/grafana/pyroscope-go/godeltaprof v0.1.11 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0 // indirect
	github.com/hashicorp/consul/api v1.32.1 // indirect
	github.com/hashicorp/cronexpr v1.1.3 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-hclog v1.6.3 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-retryablehttp v0.7.8 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/go-version v1.9.0 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/hashicorp/nomad/api v0.0.0-20260528135333-5b027732945f // indirect
	github.com/hashicorp/serf v0.10.1 // indirect
	github.com/hetznercloud/hcloud-go/v2 v2.42.0 // indirect
	github.com/huandu/xstrings v1.5.0 // indirect
	github.com/iancoleman/strcase v0.3.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/ionos-cloud/sdk-go/v6 v6.3.7 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jaegertracing/jaeger-idl v0.9.0 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jdx/go-netrc v1.0.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jonboulle/clockwork v0.5.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/julienschmidt/httprouter v1.3.0 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/pgzip v1.2.6 // indirect
	github.com/knadh/koanf v1.5.0 // indirect
	github.com/knadh/koanf/providers/env/v2 v2.0.0 // indirect
	github.com/knadh/koanf/v2 v2.3.5 // indirect
	github.com/kolo/xmlrpc v0.0.0-20220921171641-a4b6fa1dd06b // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/leodido/go-syslog/v4 v4.5.0 // indirect
	github.com/leodido/ragel-machinery v0.0.0-20190525184631-5f46317e436b // indirect
	github.com/lightstep/go-expohisto v1.0.0 // indirect
	github.com/linode/go-metadata v0.2.4 // indirect
	github.com/linode/linodego v1.69.1 // indirect
	github.com/lufia/plan9stats v0.0.0-20260330125221-c963978e514e // indirect
	github.com/magefile/mage v1.17.2 // indirect
	github.com/magiconair/properties v1.8.10 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-runewidth v0.0.24 // indirect
	github.com/mdlayher/socket v0.4.1 // indirect
	github.com/mdlayher/vsock v1.2.1 // indirect
	github.com/miekg/dns v1.1.72 // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.1-0.20231216201459-8508981c8b6c // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/go-archive v0.2.0 // indirect
	github.com/moby/patternmatcher v0.6.1 // indirect
	github.com/moby/sys/atomicwriter v0.1.0 // indirect
	github.com/moby/sys/sequential v0.7.0 // indirect
	github.com/moby/sys/user v0.4.0 // indirect
	github.com/moby/sys/userns v0.1.0 // indirect
	github.com/moby/term v0.5.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/morikuni/aec v1.1.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f // indirect
	github.com/oklog/ulid/v2 v2.1.1 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/internal/basicauth v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/internal/credentialsfile v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/k8sleaderelector v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/ecsutil v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/common v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/docker v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/exp/metrics v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/healthcheck v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/k8sconfig v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/k8sinventory v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/kafka v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/metadataproviders v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/pdatautil v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/sharedcomponent v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/batchpersignal v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/core/xidutils v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/experimentalmetricmetadata v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/kafka/configkafka v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/kafka/topic v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/sampling v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/status v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/azure v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheus v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/zipkin v0.154.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/deltatocumulativeprocessor v0.154.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/openshift/api v0.0.0-20251015095338-264e80a2b6e7 // indirect
	github.com/openshift/client-go v0.0.0-20251015124057-db0dee36e235 // indirect
	github.com/openzipkin/zipkin-go v0.4.3 // indirect
	github.com/outscale/osc-sdk-go/v2 v2.34.0 // indirect
	github.com/ovh/go-ovh v1.9.0 // indirect
	github.com/pascaldekloe/name v1.0.1 // indirect
	github.com/pb33f/jsonpath v0.8.2 // indirect
	github.com/pb33f/libopenapi v0.37.2 // indirect
	github.com/pb33f/ordered-map/v2 v2.3.1 // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/petermattis/goid v0.0.0-20260226131333-17d1149c6ac6 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/alertmanager v0.32.1 // indirect
	github.com/prometheus/client_golang/exp v0.0.0-20260518105423-c9d5bc4c50a9 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common/assets v0.2.0 // indirect
	github.com/prometheus/exporter-toolkit v0.16.0 // indirect
	github.com/prometheus/otlptranslator v1.0.0 // indirect
	github.com/prometheus/procfs v0.20.1 // indirect
	github.com/prometheus/sigv4 v0.4.1 // indirect
	github.com/puzpuzpuz/xsync/v4 v4.5.0 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.59.1 // indirect
	github.com/relvacode/iso8601 v1.7.0 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/rs/cors v1.11.1 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/samber/lo v1.53.0 // indirect
	github.com/samber/slog-common v0.21.0 // indirect
	github.com/samber/slog-zap/v2 v2.7.0 // indirect
	github.com/scaleway/scaleway-sdk-go v1.0.0-beta.36 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/segmentio/encoding v0.5.4 // indirect
	github.com/shirou/gopsutil/v4 v4.26.5 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/shurcooL/httpfs v0.0.0-20230704072500-f1e31cf0ba5c // indirect
	github.com/sirupsen/logrus v1.9.4 // indirect
	github.com/spf13/cast v1.10.0 // indirect
	github.com/stackitcloud/stackit-sdk-go/core v0.26.0 // indirect
	github.com/stretchr/objx v0.5.3 // indirect
	github.com/tetratelabs/wazero v1.11.0 // indirect
	github.com/tg123/go-htpasswd v1.2.4 // indirect
	github.com/tidwall/btree v1.8.1 // indirect
	github.com/tklauser/go-sysconf v0.4.0 // indirect
	github.com/tklauser/numcpus v0.12.0 // indirect
	github.com/twmb/franz-go v1.21.2 // indirect
	github.com/twmb/franz-go/pkg/kadm v1.18.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	github.com/twmb/franz-go/pkg/sasl/kerberos v1.1.0 // indirect
	github.com/twmb/franz-go/plugin/kzap v1.1.2 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/ua-parser/uap-go v0.0.0-20260529044130-17c35e68e58c // indirect
	github.com/valyala/fastjson v1.6.10 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	github.com/valyala/quicktemplate v1.8.0 // indirect
	github.com/vbatts/tar-split v0.12.2 // indirect
	github.com/vultr/govultr/v3 v3.31.2 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/yuin/goldmark v1.8.2 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.etcd.io/bbolt v1.4.3 // indirect
	go.lsp.dev/jsonrpc2 v0.10.0 // indirect
	go.lsp.dev/pkg v0.0.0-20210717090340-384b27a52fb2 // indirect
	go.lsp.dev/protocol v0.12.0 // indirect
	go.lsp.dev/uri v0.3.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/collector v0.154.0 // indirect
	go.opentelemetry.io/collector/client v1.60.0 // indirect
	go.opentelemetry.io/collector/cmd/builder v0.154.0 // indirect
	go.opentelemetry.io/collector/config/configauth v1.60.0 // indirect
	go.opentelemetry.io/collector/config/configcompression v1.60.0 // indirect
	go.opentelemetry.io/collector/config/configgrpc v0.154.0 // indirect
	go.opentelemetry.io/collector/config/configmiddleware v1.60.0 // indirect
	go.opentelemetry.io/collector/config/configopaque v1.60.0 // indirect
	go.opentelemetry.io/collector/config/configretry v1.60.0 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.154.0 // indirect
	go.opentelemetry.io/collector/confmap/xconfmap v0.154.0 // indirect
	go.opentelemetry.io/collector/connector/connectortest v0.154.0 // indirect
	go.opentelemetry.io/collector/connector/xconnector v0.154.0 // indirect
	go.opentelemetry.io/collector/consumer/consumererror v0.154.0 // indirect
	go.opentelemetry.io/collector/consumer/consumererror/xconsumererror v0.154.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.154.0 // indirect
	go.opentelemetry.io/collector/exporter/exporterhelper/xexporterhelper v0.154.0 // indirect
	go.opentelemetry.io/collector/exporter/exportertest v0.154.0 // indirect
	go.opentelemetry.io/collector/exporter/xexporter v0.154.0 // indirect
	go.opentelemetry.io/collector/extension/extensionauth v1.60.0 // indirect
	go.opentelemetry.io/collector/extension/extensioncapabilities v0.154.0 // indirect
	go.opentelemetry.io/collector/extension/extensionmiddleware v0.154.0 // indirect
	go.opentelemetry.io/collector/extension/extensiontest v0.154.0 // indirect
	go.opentelemetry.io/collector/extension/xextension v0.154.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.60.0 // indirect
	go.opentelemetry.io/collector/filter v0.154.0 // indirect
	go.opentelemetry.io/collector/internal/componentalias v0.154.0 // indirect
	go.opentelemetry.io/collector/internal/fanoutconsumer v0.154.0 // indirect
	go.opentelemetry.io/collector/internal/memorylimiter v0.154.0 // indirect
	go.opentelemetry.io/collector/internal/sharedcomponent v0.154.0 // indirect
	go.opentelemetry.io/collector/internal/telemetry v0.154.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.154.0 // indirect
	go.opentelemetry.io/collector/pdata/testdata v0.154.0 // indirect
	go.opentelemetry.io/collector/pdata/xpdata v0.154.0 // indirect
	go.opentelemetry.io/collector/pipeline/xpipeline v0.154.0 // indirect
	go.opentelemetry.io/collector/processor/processorhelper v0.154.0 // indirect
	go.opentelemetry.io/collector/processor/processorhelper/xprocessorhelper v0.154.0 // indirect
	go.opentelemetry.io/collector/processor/processortest v0.154.0 // indirect
	go.opentelemetry.io/collector/processor/xprocessor v0.154.0 // indirect
	go.opentelemetry.io/collector/receiver/xreceiver v0.154.0 // indirect
	go.opentelemetry.io/collector/scraper v0.154.0 // indirect
	go.opentelemetry.io/collector/scraper/scraperhelper v0.154.0 // indirect
	go.opentelemetry.io/collector/service/hostcapabilities v0.154.0 // indirect
	go.opentelemetry.io/contrib/bridges/otelzap v0.19.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace v0.69.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/runtime v0.69.0 // indirect
	go.opentelemetry.io/contrib/otelconf v0.24.0 // indirect
	go.opentelemetry.io/contrib/propagators/autoprop v0.69.0 // indirect
	go.opentelemetry.io/contrib/propagators/aws v1.44.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.44.0 // indirect
	go.opentelemetry.io/contrib/propagators/jaeger v1.44.0 // indirect
	go.opentelemetry.io/contrib/propagators/ot v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc v0.20.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp v0.20.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.66.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutlog v0.20.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.44.0 // indirect
	go.opentelemetry.io/otel/sdk/log v0.20.0 // indirect
	go.uber.org/automaxprocs v1.6.0 // indirect
	go.uber.org/goleak v1.3.0 // indirect
	go.uber.org/zap/exp v0.3.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	go.yaml.in/yaml/v4 v4.0.0-rc.4 // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/mod v0.37.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/term v0.44.0 // indirect
	golang.org/x/text v0.38.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	golang.org/x/tools v0.46.0 // indirect
	gonum.org/v1/gonum v0.17.0 // indirect
	google.golang.org/api v0.278.0 // indirect
	google.golang.org/genproto v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260608224507-4308a22a1bab // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260608224507-4308a22a1bab // indirect
	gopkg.in/evanphx/json-patch.v4 v4.13.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/ini.v1 v1.67.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/api v0.35.4 // indirect
	k8s.io/apimachinery v0.35.4 // indirect
	k8s.io/client-go v0.35.4 // indirect
	k8s.io/klog/v2 v2.140.0 // indirect
	k8s.io/kube-openapi v0.0.0-20260414162039-ec9c827d403f // indirect
	k8s.io/utils v0.0.0-20260507154919-ff6756f316d2 // indirect
	mvdan.cc/xurls/v2 v2.6.0 // indirect
	pluginrpc.com/pluginrpc v0.5.0 // indirect
	sigs.k8s.io/json v0.0.0-20250730193827-2d320260d730 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
	sigs.k8s.io/structured-merge-diff/v6 v6.3.2 // indirect
)
