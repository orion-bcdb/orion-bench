module orion-bench

go 1.16

replace github.com/hyperledger-labs/orion-server => ../orion-server

replace github.com/hyperledger-labs/orion-sdk-go => ../orion-sdk-go

require (
	github.com/cenkalti/backoff v2.1.1+incompatible
	github.com/creasty/defaults v1.6.0
	github.com/hyperledger-labs/orion-sdk-go v0.2.5
	github.com/hyperledger-labs/orion-server v0.2.6-0.20220828084308-c4591bc4f62e
	github.com/mroth/weightedrand v1.0.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.7.1
	github.com/spf13/viper v1.10.1
	//github.com/hyperledger-labs/orion-sdk-go v0.0.0-20220628135226-31c863fdb78d
	//github.com/hyperledger-labs/orion-server v0.2.4-0.20220621134147-6a9aeaf38f9a
	go.uber.org/zap v1.18.1
	google.golang.org/protobuf v1.27.1
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
)
