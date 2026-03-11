# Minerva
Artifact Repository for Machine Learning Models *Epoch-based Optimistic Concurrency Control in Geo-replicated Databases*.

## Dependencies
1. Python 3.10+
2. DotNet 9.0+
3. Ubuntu 24.04

## Setup Instructions
1. Clone the repository
2. Build the StableSolver submodule, check readme in `Minerva/StableSolver/README.md` for instructions. There are additional dependencies for StableSolver. Pre-built binaries for Ubuntu 24.04 are provided. (Tested on Ubuntu 24.04 and 22.04)

Minerva consists of database server application and client application. The database server runs in a cluster and must have > 3 nodes for fault tolerance. The client application can be run on any machine and connect to each server through TPC sockets.

### One-Click Benchmarking Script

See "scripts/README.md" for instructions on running the one-click benchmarking script. Or if you want to run the server and client applications separately, follow the instructions below.

### Running the Database Server With Start/Stop Script

Under the `scripts` directory, create `configs` directory and place your `logger_config.json` and `minerva_config.json` files there, see `DB-Server/minerva_config_example.json` and `DB-Server/logger_config_example.json` for example configuration files.


Minerva config explanation:
- `ReadStorage`: Path to the directory where the database files are stored, you have to pre-generate database files, see below(optional, set empty string to disable).
- `DatabaseToLoad`: List of database names to load on startup, only enabled when the ReadStorage option is set. Can be "YCSB" (key-value store) or "TPCC".
- `SolverExact`: Boolean flag to indicate whether to use exact solver or approximate solver.
- `ReplicaPriority`: List of replica id indicating the priority of each replica using the order of the list, must equal to the number of replicas in the cluster.
- `LocalEpochInterval`: Interval (in milliseconds) for local epoch advancement.
- `CoordinatorGlobalEpochInterval`: Interval (in milliseconds) for global epoch advancement by the coordinator


```
$cd scripts
$./start_servers.py rstart 1 [iplist.txt]
```
This will start the database server cluster with the specified number of replicas. Each server will be started on the specified IP addresses, each ip address should correspond to a different machine (must be unique). Server id is assigned based on the order of the ip addresses provided, starting from 0.

Ip list file example:
Example:

```text
10.0.0.1
10.0.0.2
10.0.0.3
```

See `scripts/start_servers.py --help` to change port numbers and other configurations. Port is default to 5000.


### Running the Database Server Manually
To run the database server application manually, use the following command:
```
$cd src/DB-Server
dotnet run -c Release [minerva_config.json] [cluster_config.json] [logger_config.json]
```

See `src/DB-Server/minerva_config_example.json`, `src/DB-Server/cluster_config_example.json`, and `src/DB-Server/logger_config_example.json` for example configuration files.
Note that you need to run each server in the cluster separately with the same `minerva_config.json` and `logger_config.json` but different `cluster_config.json` that corresponds to the server id and ip address.

### Running the Client
```
$cd Client
```

To run the client in benchmark mode:
```
 dotnet run -c Release [benchmark_config.json]
```
See `Client/benchmark_config_example.json` for an example configuration file.

Set `pre_load_db` to true if the server database is set to read from Storage otherwise the client will populate the database on startup.


### Store Database For Pre-loading

To store the database, under `src/Client/BenchmarkInterface.cs`, uncomment `//runner.SaveDatabase().Wait();` and comment out `BenchmarkResult result = runner.RunBenchmark();` lines. You can change `persistentStorage.SaveStorageToDisk("/home/ubuntu/minerva_data/");` line in `src\DB-Server\Interface\QueryHandler.cs` to store the database files to a different location,

