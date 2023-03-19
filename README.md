Quarkus guide: https://quarkus.io/guides/rest-json

+ 添加文件以后：`./mvnw com.spotify.fmt:fmt-maven-plugin:format`

+ build image: `sudo ./mvnw clean package -Dquarkus.container-image.build=true -DskipTestsls=true -Dquarkus.http.port=8083`

+ 开启Cassandra，cordinator node，和kvstoreapi: `./start_cass_40.sh`

+ 更改kvstoreapi，重新build：

  + `docker down kvstoreapi`

  + `export SGTAG=v2`

    `export PROJTAG=v1.0.0-SNAPSHOT`

  +  `docker-compose up kvstoreapi`

+ 全部关闭`docker-compose down`

只运行dynomoDB：

+ `docker run -p 8083:8083 stargate-cmu/key-value-store:v1.0.0-SNAPSHOT`

+ Cql command: 
  + 删除某column：`ALTER TABLE "0".kvstore DROP cassandra;`
  + 查看某keyspace: `SELECT * FROM system_schema.columns WHERE keyspace_name = '0' AND table_name = 'kvstore';`
  + 查看所有keyspcae: `describe keyspaces`

http command: 

```
authentication:
curl -X 'POST' \
  'http://localhost:8081/v1/auth/token/generate' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "key": "cassandra",
  "secret": "cassandra"
}'

curl -X 'POST' \
  'http://localhost:8083/kvstore/v1/databases' \
  -H 'accept: application/json' \
  -H 'content-type: application/json' \
  -H 'X-Cassandra-Token: 557c7929-da7b-40ea-8d41-93adf6ec85a5'

curl -X 'PUT' \
  'http://localhost:8083/kvstore/v1/0' \
  -H 'accept: application/json' \
  -H 'content-type: application/json' \
  -H 'X-Cassandra-Token: 557c7929-da7b-40ea-8d41-93adf6ec85a5' \
  -d '{
  "key": "cassandra",
  "value": "cassandra"
}'

curl -X 'PATCH' \
  'http://localhost:8083/kvstore/v1/2' \
  -H 'accept: application/json' \
  -H 'content-type: application/json' \
  -d '{
    "key": "cassandra",
    "value":  "abcde"
}'

curl -X 'GET' \
  'http://localhost:8083/kvstore/v1/0' \
  -H 'accept: application/json' \
  -H 'content-type: application/json' \
  -H 'X-Cassandra-Token: 557c7929-da7b-40ea-8d41-93adf6ec85a5' \
  -d '{
    "key": "cassandra2"
}'

// not ok
curl -X 'DELETE' \
  'http://localhost:8083/kvstore/v1/3/key' \
  -H 'accept: application/json' \
  -H 'content-type: application/json' \
  -d '{
  "key": "cassandra",
  "value": "cassandra"
}'
```

+ cassandra存储原理(简)：
  + database = keyspace -> only one table named "kvstore", column "partition_key"
  + Each key = column name
  + each value = only one row in the corresponding column
+ Done:
  + docker-compose shell完成
  + authentication认证完成
  + create, put, get完成
+ Todo:
  + in-memory和cassandra不同步
  + Exception
  + Response type db_response, 需要关联cassandra query response
  + delete key, delete db, update feature
