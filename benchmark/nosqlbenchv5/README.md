## Run NoSQLBench v5

### Download the latest nb5 jar
Download: `curl -L -O https://github.com/nosqlbench/nosqlbench/releases/latest/download/nb5.jar`

Check version: `java -jar nb5.jar --version`

### More info for downloading
https://docs.nosqlbench.io/getting-started/00-get-nosqlbench/

### Running .yaml
`java -jar nb5.jar ./http-rest-starter.yaml`

### Option --report-csv-to
`java -jar nb5.jar ./http-rest-starter.yaml --report-csv-to my_metrics_dir`

### Docs for running with http-rest with Stargate
https://builddocs.nosqlbench.io/blog/http-rest/

### Processed Real-world dataset CSV files
https://drive.google.com/drive/folders/195TPsD9RbM9ynRhQPojvhARRnTA3SB2X?usp=sharing

To use the real-world values as inputs, simply change rw_key definition to this:
rw_key: CSVFrequencySampler('ID-GEO.csv','Value')

Documents: https://docs.nosqlbench.io/reference/bindings/funcref-general/#csvfrequencysampler

Everything else is the same
