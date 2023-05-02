### 运行cassandra + coordinator + kvapi

第一次要先build docker image(参考下面的build image)

进入`~/rest-key-value-store/docker-compose/cassandra-4.0`

运行`./start_cass_40.sh `

docker 开启：

+ Cassandra node: cass40-stargate_cassandra-1_1(name)
+ Cassandra node: cass40-stargate_cassandra-2_1
+ Cassandra node: cass40-stargate_cassandra-3_1
+ Dynamo DB API: cass40-stargate_dynamoapi_1(port: 8082)
+ Stargate Coordinator node: cass40-stargate_coordinator_1(port: 8081 authentication, port: 9042 clash)

全部关闭：docker-compose down

start up kvstoreapi
sudo PROJTAG=v1.0.0-SNAPSHOT SGTAG=v2 docker-compose up kvstoreapi


### 仅修改kvapi如何重启

修改文件后在`~/rest-jet-value-store/` 下运行:

+ format：`./mvnw com.spotify.fmt:fmt-maven-plugin:format`

+ build image: `sudo ./mvnw clean package -Dquarkus.container-image.build=true -DskipTestsls=true -Dquarkus.http.port=8083`

+ 更改kvstoreapi，在`~/rest-key-value-store/docker-compose/cassandra-4.0`重新build：

  + `docker stop kvstoreapi`

  + `export SGTAG=v2`

    `export PROJTAG=v1.0.0-SNAPSHOT`

  + `docker-compose up kvstoreapi`

### CQL 测试语法

+ 开启cqlsh：`docker exec -it cass40-stargate_cassandra-1_1 cqlsh`
+ 看keyspaces: `describe keyspaces`
+ 看table: `SELECT * FROM "kvdb0"."kvtable"`

### Docker 语法

+ 查看docker image: `docker images / docker image ls`
+ 查看正在运行的docker containe：`docker container ls`
+ 查看某container的port: `docker port <container-name>` 

### HTTP 测试

```
//get token 
curl -X 'POST' \
  'http://localhost:8081/v1/auth/token/generate' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "key": "cassandra",
  "secret": "cassandra"
}'

// create db
curl -X 'POST' \
  'http://localhost:8083/kvstore/v1/databases' \
  -H 'accept: application/json' \
  -H 'content-type: application/json' \
  -H 'X-Cassandra-Token: 15e6ec23-4e03-4139-9d42-7baeb5d1f959' \
  -d '{
  "db_name": "mydb"
}'
201: The database {db_name}  has been created successfully.
409: The database {db_name} already exists.
400: The database name is invalid.

// delete db
curl -X 'DELETE' \
  'http://localhost:8083/kvstore/v1/mydb' \
  -H 'X-Cassandra-Token: 0628ea4b-86ea-4277-a7ef-b288fb991ee0' \
  -H 'accept: application/json' \
  -H 'content-type: application/json'
204: The database {db_name} has been deleted successfully.
404: The database name is invalid. or The database {db_name} does not exist.

// create table
curl -X 'POST' \
  'http://localhost:8083/kvstore/v1/databases/mydb/tables' \
  -H 'accept: application/json' \
  -H 'content-type: application/json' \
  -H 'X-Cassandra-Token: 15e6ec23-4e03-4139-9d42-7baeb5d1f959' \
  -d '{
  "table_name": "mytable"
}'

// delete table
curl -X 'POST' \
  'http://localhost:8083/kvstore/v1/mydb/mytable' \
  -H 'accept: application/json' \
  -H 'content-type: application/json' \
  -H 'X-Cassandra-Token: 0628ea4b-86ea-4277-a7ef-b288fb991ee0'

// list databases
curl -X 'GET' \
  'http://localhost:8083/kvstore/v1/databases' \
  -H 'accept: application/json' \
  -H 'X-Cassandra-Token: 0628ea4b-86ea-4277-a7ef-b288fb991ee0'

// list tables
curl -X 'GET' \
  'http://localhost:8083/kvstore/v1/mydb/tables' \
  -H 'accept: application/json' \
  -H 'X-Cassandra-Token: 0628ea4b-86ea-4277-a7ef-b288fb991ee0'


// put kv
curl -X 'PUT' \
  'http://localhost:8083/kvstore/v1/mydb/mytable' \
  -H 'accept: application/json' \
  -H 'content-type: application/json' \
  -H 'X-Cassandra-Token: 0628ea4b-86ea-4277-a7ef-b288fb991ee0' \
  -d '{
  "key": "cassandra",
  "value": "cassandra"
}'

// get v
curl -X 'GET' \
  'http://localhost:8083/kvstore/v1/mydb/mytable' \
  -H 'accept: application/json' \
  -H 'content-type: application/json' \
  -H 'X-Cassandra-Token: 0628ea4b-86ea-4277-a7ef-b288fb991ee0' \
  -d '{
    "key": "cassandra"
}'

// update kv
curl -X 'PATCH' \
  'http://localhost:8083/kvstore/v1/mydb/mytable' \
  -H 'accept: application/json' \
  -H 'X-Cassandra-Token: 0628ea4b-86ea-4277-a7ef-b288fb991ee0' \
  -H 'content-type: application/json' \
  -d '{
    "key": "cassandra",
    "value":  "abcde"
}'

curl -X 'DELETE' \
  'http://localhost:8083/kvstore/v1/mydb/key' \
  -H 'accept: application/json' \
  -H 'content-type: application/json' \
  -H 'X-Cassandra-Token: 0628ea4b-86ea-4277-a7ef-b288fb991ee0' \
  -d '{
  "key": "cassandra"
}'
```

+ cassandra存储原理(简)：
  + user create DB: new keyspace named **kvdb{db_id}** (db_id start from 0)
  + Only one table in each keyspace named **kvtable**
  + Two text columns: key(partition key), value
+ Done:
  + Authentication, API implementation
+ Todo:
  + Response and Exception Format
  + Caching layer implementation
  + Replication Model implementation
