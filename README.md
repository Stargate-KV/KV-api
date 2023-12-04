### Running Cassandra + Coordinator + KVAPI

**Initial Setup**:

- Build the Docker image first (refer to the 'build image' section below).
- Navigate to `~/rest-key-value-store/docker-compose/cassandra-4.0`.
- Run `./start_cas_40.sh`.

**Docker Components**:

- Cassandra nodes:
  - `cass40-stargate_cassandra-1_1`
  - `cass40-stargate_cassandra-2_1`
  - `cass40-stargate_cassandra-3_1`
- Dynamo DB API: `cass40-stargate_dynamoapi_1` (port: 8082)
- Stargate Coordinator node: `cass40-stargate_coordinator_1` (port: 8081 for authentication, port: 9042 for clash)

**To Shut Down**:

- Run `docker-compose down` to stop all services.

### Restarting kvAPI After Modifications

After modifying files in `~/rest-jet-value-store/`, execute the following:

### Build KV API Image

- Format: `./mvnw com.spotify.fmt:fmt-maven-plugin:format`.

- Build image: `sudo ./mvnw clean package -Dquarkus.container-image.build=true -DskipTestsls=true -Dquarkus.http.port=8083`.

- To rebuild kvstoreapi in 

  ```
  ~/rest-key-value-store/docker-compose/cassandra-4.0
  ```

  - Stop the service: `docker stop kvstoreapi`.
  - Set environment variables:
    - `export SGTAG=v2`
    - `export PROJTAG=v1.0.0-SNAPSHOT`.
  - Restart the service: `docker-compose up kvstoreapi`.

### HTTP API

#### Get authentication token

```json
 curl -X 'POST' \
  'http://localhost:8081/v1/auth/token/generate' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "key": "cassandra",
  "secret": "cassandra"
}'
```

#### database (key space) creation & deletion & list

```json
// Create database
curl -X 'POST' \
  'http://{{host_url}}:8083/kvstore/v1/databases' \
  -H 'accept: application/json' \
  -H 'content-type: application/json' \
  -H 'X-Cassandra-Token: {{token}}' \
  -d '{
  "db_name": "mydb"
}'

201: The database {db_name} has been created successfully.
409: The database {db_name} already exists.
400: The database name is invalid.

// Delete database
curl -X 'DELETE' \
  'http://{{host_url}}:8083/kvstore/v1/mydb' \
  -H 'X-Cassandra-Token: {{token}}' \
  -H 'accept: application/json' \
  -H 'content-type: application/json'

204: The database {db_name} has been deleted successfully.
404: The database name is invalid or the database {db_name} does not exist.


// List databases
curl -X 'GET' \
  'http://{{host_url}}:8083/kvstore/v1/databases' \
  -H 'accept: application/json' \
  -H 'X-Cassandra-Token: {{token}}'
```

#### table creation & deletion & list

```json
// Create table
curl -X 'POST' \
  'http://{{host_url}}:8083/kvstore/v1/databases/mydb/tables' \
  -H 'accept: application/json' \
  -H 'content-type: application/json' \
  -H 'X-Cassandra-Token: {{token}}' \
  -d '{
  "table_name": "mytable"
}'

// Delete table
curl -X 'POST' \
  'http://{{host_url}}:8083/kvstore/v1/mydb/mytable' \
  -H 'accept: application/json' \
  -H 'content-type: application/json' \
  -H 'X-Cassandra-Token: {{token}}'

// List tables
curl -X 'GET' \
  'http://localhost:8083/kvstore/v1/mydb/tables' \
  -H 'accept: application/json' \
  -H 'X-Cassandra-Token: {{token}}'

```

#### Operations on KV

```json
// Put key-value
curl -X 'PUT' \
  'http://{{host_url}}:8083/kvstore/v1/mydb/mytable' \
  -H 'accept: application/json' \
  -H 'content-type: application/json' \
  -H 'X-Cassandra-Token: {{token}}' \
  -d '{
  "key": "cassandra",
  "value": "cassandra"
}'

// Get value
curl -X 'GET' \
  'http://{{host_url}}:8083/kvstore/v1/mydb/mytable' \
  -H 'accept: application/json' \
  -H 'content-type: application/json' \
  -H 'X-Cassandra-Token: {{token}}' \
  -d '{
    "key": "cassandra"
}'

// Update key-value
curl -X 'PATCH' \
  'http://{{host_url}}:8083/kvstore/v1/mydb/mytable' \
  -H 'accept: application/json' \
  -H 'X-Cassandra-Token: {{token}}' \
  -H 'content-type: application/json' \
  -d '{
    "key": "cassandra",
    "value":  "abcde"
}'

// Delete key
curl -X 'DELETE' \
  'http://{{host_url}}:8083/kvstore/v1/mydb/key' \
  -H 'accept: application/json' \
  -H 'content-type: application/json' \
  -H 'X-Cassandra-Token: {{token}}' \
  -d '{
  "key": "cassandra"
}'
```

#### Get and Set Cache Status

```json
// get cache size, hit rate, cache max_size and current eviction policy
curl -X 'GET' \
	'http://{{host_url}}:8083/kvstore/v1/getcachestatus' \
  -H 'accept: application/json' \
  -H 'X-Cassandra-Token: {{token}}'

// reseat cache and set cache size (-1=nochange) and eviction policy (policy = NOCHANGE, LRU, FIFO, RANDOM)
curl -X 'PUT' \
	'http://{{host_url}}:8083/kvstore/v1/resetcache' \
	 -H 'accept: application/json' \
   -H 'X-Cassandra-Token: {{token}}'
	-d '{
	 "max_size": "-1",
   "eviction_policy": "NOCHANGE"
}'

// use micrometer for the data
curl -X 'GET' \
	'http://{{host_url}}:8083/q/metrics/' \
	 -H 'accept: application/json' \
   -H 'X-Cassandra-Token: {{token}}'
```
Use `benchmark/nosqlbenchv5/metrics_analysis_tool.py` to analyze the metrics.

### Metrics API 

To export metrics, 
endpoint:
GET kvstore/v1/metrics
params:
dir optional directory to store the results of metrics in

### Eviction Policy

We have four eviction policy for our cache layer:

+ No cache
+ LRU
+ FIFO
+ Random
