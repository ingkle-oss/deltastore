First, you should launch a s3 compatible object storage using [minio](https://min.io/) as following, and upload samples to minio using mc.

```bash
docker run -p 9000:9000 -p 9001:9001 quay.io/minio/minio server /data --console-address ":9001"

mc config host add minio http://localhost:9000 minioadmin minioadmin
mc mb minio/testset100
mc cp --recursive samples/testset100/ minio/testset100/
```

And, you should launch a [sample server for configuration and authorization](https://github.com/dataplatform-lab/deltastore/blob/main/examples/server.py).

```bash
python3 ./server.py
```

Then, you can launch a server with [sample configuration file](https://github.com/dataplatform-lab/deltastore/blob/main/examples/deltastore.yaml).

```bash
sbt "server/runMain io.delta.store.DeltaStoreServer -c ${PWD}/examples/deltastore.yaml"
```

Finally, you can query to the server with [sample profile file](https://github.com/dataplatform-lab/deltastore/blob/main/examples/profile.json) like this.

```bash
python3 ./query_duckdb.py --profile profile.json --share deltastore --schema testsets --table testset100
```
