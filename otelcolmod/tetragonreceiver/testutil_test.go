package tetragonreceiver

import "go.opentelemetry.io/collector/config/configgrpc"

func createTestClientConfig(endpoint string) configgrpc.ClientConfig {
	return configgrpc.ClientConfig{
		Endpoint: endpoint,
	}
}
