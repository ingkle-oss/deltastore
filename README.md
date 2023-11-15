# DeltaStore

DeltaStore is an open-source data sharing framework using [DeltaSharing](https://delta.io/sharing) that enables building a Data CleanRoom with compute engines including Spark and Trino and APIs for Scala, Rust, and Python.

## Key features

Our main goals are near real-time query against the up-to-date regardless of the size of deltalogs, and access controls for data governance.

### Caching DeltaLogs

We support deltalogs caching using a key-value storage like RocksDB. We create a cache database per each deltatable, and update newly appended deltalogs to the cache database incrementally and periodically to keep the up-to-date.

We support partition filterings using range queries if you are using RocksDB for deltalogs caching. A key in a cache database is a AddFile's path which contains partition columns and values as a prefix. So, we can scan deltalogs very fast using a prefix which is generated from predicate hints.

We support versioning efficiently. A value in a cache database contains a 'from' field that is a version number when a AddFile is added, and a 'to' field that is a version number when a AddFile is removed. So, we can check if a AddFile is valid using 'from' and 'to' fields easily.

### Configuration and Authorization

We support REST protocols to help you develop your own [configuration](https://github.com/dataplatform-lab/deltastore/blob/main/docs/ConfigServer.md) and [authorization](https://github.com/dataplatform-lab/deltastore/blob/main/docs/AuthZServer.md) servers.

## Reporting issues

We use [GitHub Issues](https://github.com/dataplatform-lab/deltastore/issues) to track community reported issues. You can also [contact](#community) the community for getting answers.

## Contributing

We welcome contributions to DeltaStore. See our [CONTRIBUTING.md](https://github.com/dataplatform-lab/deltastore/blob/main/CONTRIBUTING.md) for more details.

## Building

DeltaStore is compiled using [SBT](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html).

To compile, run

```bash
sbt compile
```

To generate artifacts, run

```bash
sbt package
```

To execute server, run

```bash
sbt "server/runMain io.delta.store.DeltaStoreServer -c {configfile}"
```

To execute tests, run

```bash
sbt test
```

To execute a single test suite, run

```bash
sbt 'testOnly io.delta.store.PartitionPrefixSuite'
```

To execute a single test within and a single test suite, run

```bash
sbt 'testOnly io.delta.store.PartitionPrefixSuite -- -z "equal"'
```

To execute python tests, run

```bash
python3 -m pytest
```

Refer to [SBT docs](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html) for more commands.

## License

Apache License 2.0, see [LICENSE](https://github.com/dataplatform-lab/deltastore/blob/main/LICENSE.txt).
