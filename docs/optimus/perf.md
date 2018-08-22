---
title: Performance Tests
weight: 30
exclude: true
---

# Performance Tests

## Get Performance

Results of the performance tests conducted for optimus are documented here:


### Configuration


| Optimus API | |
| --- | --- |
| EC2 Instance type | m4.xlarge [4 vCPU, 16GiB] |
| Java Version | 1.8.0_91 |
| JVM OPTS | -XX:+UseG1GC -Xms2G -Xmx4G |
| AWS Client Config | max-connections = 200 (kv-store only) |
| Optimus git commit tag | 79106ec275231a981c5d2f71c1e898c95944e971 |


| DynamoDB Config | |
| --- | --- |
| KVStore Table | Provisioned Read Throughput: 1000, Provisioned Write Throughput : 10 |
| | Version Index : (N/A as it is not used in the test) |
| MetadataStore Table | Provisioned Read Throughput: 10, Provisioned Write Throughput : 10 |
| | MetaTypeIndex : Provisioned Read Throughput: 10, Provisioned Write Throughput: 10 |
| DurableQ Table | Provisioned Read Throughput: 10, Provisioned Write Throughput : 10 |


| Gatling Client | |
| --- | --- |
| EC2 Instance type | m4.xlarge [4 vCPU, 16GiB] |
| Java Version | 1.8.0_91 |
| Gatling version | 2.2.4 |
| Gatling - Ramp Time  | 600s |
| Gatling - Full Time | 900s |


#### Datasets

| Name | Num of kvpairs | Size per value |
| --- | --- | --- | --- |
| perf256  | 1000000 | 256B |
| perf5000 | 1000000 | 5KB  |

* Values are kv pairs of UIDs.

#### Application configuration

The full application configuration used during tests is as follows:

``` clojure
{:server {:port 80, :auto-reload false, :context-root "/optimus"},
 :async-task
 {:poll-interval 5000,
  :handler-fn "optimus.service.async-task/handle-message",
  :operations-topic "versions"},
 :model-store {:operations-topic "versions"},
 :kv-store
 {:name "entries.dynamodb",
  :type :dynamodb,
  :kv-store-table "OptimusKV-Perf",
  :aws-client-config {:max-connections 200}},
 :meta-data-store
 {:name "dyanamodb",
  :type :dynamodb,
  :meta-store-table "OptimusMetadataStore-Perf"},
 :queue
 {:name "asynctasks.dynamodb",
  :type :dynamodb,
  :lease-time 60000,
  :task-queue-table "OptimusTasksQ-Perf",
  :reservation-index "reservation-index"},
 :metrics
 {:type :newrelic,
  :reporting-frequency-seconds 30,
  :reporter-name "optimus-api",
  :jvm-metrics :none,
  :metrics-attribute-filter
  (fn [name type]
      (#{:timer-min
         :timer-max
         :timer-median
         :timer95th-percentile
         :timer99th-percentile
         :timer999th-percentile
         :timer1-minute-rate
         :histogram-min
         :histogram-max
         :histogram-median
         :histogram75th-percentile
         :histogram95th-percentile
         :histogram99th-percentile
         :histogram999th-percentile
         :meter-count
         :meter1-minute-rate
         :counter-count
         :gauge-value} type))
  :prefix "perf/"},
 :logging {:level :info}}
```


### Get One

**Endpoint**: POST /datasets/:dataset/tables/:table/entries/:key

| Req Throughput (rps) | Payload size  | Min | Median | 99 Percentile | Max | Std Dev |
| --- | --- | ------ | --- | --- | --- | --- |
|100|256B|6ms|9ms|24ms|1012ms|5ms|
|250|256B|6ms|9ms|31ms|1019ms|6ms|
|100|5KB|8ms|11ms|28ms|334ms|5ms|
|250|5KB|8ms|13ms|49ms|1057ms|9ms|


### Get Many

**Endpoint**: POST /datasets/:dataset/tables/:table/entries, **Content-Type**: application/json

| Req Throughput (rps) | Batch size | Payload size | Min | Median | 99 percentile | Max | Std Dev |
| --- | --- | --- | ------ | --- | --- | --- | --- |
|100|5|256B|7ms|10ms|26ms|1054ms|7ms|
|250|5|256B|7ms|11ms|43ms|1063ms|6ms|
|100|5|5KB|16ms|21ms|64ms|762ms|14ms|
|250|5|5KB|15ms|123ms|1021ms|2139ms|224ms|
