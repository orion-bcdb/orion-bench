# Orion Benchmark Tool

This tool helps generate the required material to create an Orion cluster,
and generate a workload to evaluate it.

## Usage of orion-bench
```
  -cwd string
    	benchmark configuration working directory
  -config string
    	benchmark configuration YAML file path
  -clear
    	[action]: clear all the material and data
  -list
    	[action]: list all the available material
  -material
    	[action]: generate all crypto material and configurations
  -rank value
    	worker/node rank (starting from 0) (default main)
  -init
    	[action]: initialize the data for the benchmark
  -warmup
    	[action]: runs a workload generator (client) for warmup
  -benchmark
    	[action]: runs a workload generator (client) for benchmark
  -node
    	[action]: runs an orion node
  -prometheus
    	[action]: runs a prometheus server to collect the data
```

## Benchmark Flow
The benchmark tool flow is as follows:

### Configuration
Create the configuration files that will define the experiment: cluster and workload.
See example [config.yaml](examples/config.yaml) for more details.

### Generate and Synchronize Material
On one of the hosts, run
`orion-bench -config <config-path> -material`.

This will generate all the configuration and crypto material for the experiment 
in the directories that are specified in the config file.

Then, synchronize the generated content to all hosts that participate in the experiment (clients and servers).

### Start Prometheus server
On one of the hosts, run: `orion-bench -config <config-path> -prometheus`.
This will start a prometheus server that will collect metrics from the cluster and the workload clients.

### Start Cluster
On each host that is configured as node, run: `orion-bench -config config/config.yaml -rank <rank> -node`.
For each host, use the index of the host in the config file cluster list as its rank.

### Initialize the Experiment
On one of the hosts, run: `orion-bench -config <config-path> -init`.
This will call the workload initialization. A common initialization is creating the DB tables and adding all the users.

### Warmup
On each host that is configured as client, run: `orion-bench -config <config-path> -rank <rank> -warmup`.
For each host, use the index of the host in the config file worker list as its rank.

### Benchmark Workload
On each host that is configured as client, run: `orion-bench -config <config-path> -rank <rank> -benchmark`.
For each host, use the index of the host in the config file worker list as its rank.

### Analysis
Analyze the metrics collected from the prometheus server.


## Implementing New Workloads
The benchmark tool include an independent workload generator.
That is, each user data is independent of the other users (no inherit conflicts).
Its implementation is available at [loads/independent/workload.go](pkg/workload/loads/independent/workload.go).

To implement additional workloads, you need to implement the `Worker` and `UserWorker` interfaces.
Their documentation is available at [workload/workload.go](pkg/workload/workload.go).

In addition, you need to implement an instantiation method that receives a `Workload` as an input.
Then, add it to the workload list at [config/config.go](pkg/config/config.go) named `workloads`.

To activate your new workload implementation, change the workload name in the configuration file.