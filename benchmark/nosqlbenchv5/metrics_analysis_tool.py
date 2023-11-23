# use this script to analyze the metrics got from `curl -H "Accept: text/plain" localhost:8083/q/metrics/`

# Open the file in read mode
with open("sample_data/metrics.txt", "r") as file:
    # Read all the content from the file
    content = file.read()

raw_dic = {}
# find the index of last space for each line
for line in content.splitlines():
    # if line start with #, skip it
    if line.startswith("#"):
        continue
    index = line.rfind(" ")
    print(line[:index], line[index + 1:])
    raw_dic[line[:index]] = float(line[index + 1:])


result_dic = {}
result_dic["get_counts"] = raw_dic["http_server_requests_seconds_count{method=\"GET\",outcome=\"SUCCESS\",status=\"200\",uri=\"/kvstore/v1/{db_name}/{table_name}\",}"]
result_dic["get_sum"] = raw_dic["http_server_requests_seconds_sum{method=\"GET\",outcome=\"SUCCESS\",status=\"200\",uri=\"/kvstore/v1/{db_name}/{table_name}\",}"]
result_dic["get_latency"] = result_dic["get_sum"] / result_dic["get_counts"]
result_dic["put_counts"] = raw_dic["http_server_requests_seconds_count{method=\"PUT\",outcome=\"SUCCESS\",status=\"200\",uri=\"/kvstore/v1/{db_name}/{table_name}\",}"]
result_dic["put_sum"] = raw_dic["http_server_requests_seconds_sum{method=\"PUT\",outcome=\"SUCCESS\",status=\"200\",uri=\"/kvstore/v1/{db_name}/{table_name}\",}"]
result_dic["put_latency"] = result_dic["put_sum"] / result_dic["put_counts"]

print("get_counts: ", result_dic["get_counts"])
print("get_sum: ", result_dic["get_sum"])
print("get_latency: ", result_dic["get_latency"])
print("put_counts: ", result_dic["put_counts"])
print("put_sum: ", result_dic["put_sum"])
print("put_latency: ", result_dic["put_latency"])

# read write ratio
result_dic["read_write_ratio"] = result_dic["get_counts"] / result_dic["put_counts"]
# overall average latency
result_dic["average_latency"] = (result_dic["get_sum"] + result_dic["put_sum"]) / (result_dic["get_counts"] + result_dic["put_counts"])

print("read_write_ratio: ", result_dic["read_write_ratio"])
print("average_latency: ", result_dic["average_latency"])

# network I/O
# network access count grpc_client_processing_duration_seconds_count{method="ExecuteQuery",methodType="UNARY",service="stargate.StargateBridge",statusCode="OK",} 54625.0
result_dic["network_io_count"] = raw_dic["grpc_client_processing_duration_seconds_count{method=\"ExecuteQuery\",methodType=\"UNARY\",service=\"stargate.StargateBridge\",statusCode=\"OK\",}"]
result_dic["network_io_sum"] = raw_dic["grpc_client_processing_duration_seconds_sum{method=\"ExecuteQuery\",methodType=\"UNARY\",service=\"stargate.StargateBridge\",statusCode=\"OK\",}"]
result_dic["network_io_latency"] = result_dic["network_io_sum"] / result_dic["network_io_count"]
print("network_io_count: ", result_dic["network_io_count"])
print("network_io_sum: ", result_dic["network_io_sum"])
print("network_io_latency: ", result_dic["network_io_latency"])

# environment config
# find all the key where it starts with jvm_info_total, and set it as key xxx{here}
# jvm_info_total{runtime="OpenJDK Runtime Environment",vendor="Red Hat, Inc.",version="17.0.2+8-LTS",} 1.0
for key in raw_dic.keys():
    if key.startswith("jvm_info_total"):
        result_dic["jvm_info"] = raw_dic[key]
result_dic["system_cpu_count"] = raw_dic["system_cpu_count"]
result_dic["jvm_threads_peak_threads"] = raw_dic["jvm_threads_peak_threads"]

print("jvm_info: ", result_dic["jvm_info"])
print("system_cpu_count: ", result_dic["system_cpu_count"])
print("jvm_threads_peak_threads: ", result_dic["jvm_threads_peak_threads"])
