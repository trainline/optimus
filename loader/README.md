# Optimus Loader

Data loader for Optimus based on Apache Spark that loads kv pairs from delimited input files in the request to the corresponding dataset, version and table of Optimus using the Optimus REST API.

## Usage

To build the project:

```
lein do clean, uberjar
```

## Running the loader
To run using spark-submit:
```
spark-submit --deploy-mode cluster --master yarn loader.jar         \
             --dataset <DATASET-NAME> --api-base-url <API-URL>      \
             [--label <VERSION-LABEL> ] [--version <VERSION-ID>]    \
             --table <TABLE> --file <FILE> [--content-type <TYPE>]  \
             [--table <TABLE> --file <FILE> [--content-type <TYPE>]]\
             [OPTIONS]
```

To run the loader as a standalone client:
```
lein run --local
         --dataset <DATASET-NAME> --api-base-url <API-URL>      \
         [--label <VERSION-LABEL> ] [--version <VERSION-ID>]    \
         --table <TABLE> --file <FILE> [--content-type <TYPE>]  \
         [--table <TABLE> --file <FILE> [--content-type <TYPE>]]\
         [OPTIONS]
```

The following is the list of all options supported by the loader cli.
```
  -d, --dataset DATASET-NAME                            Dataset Name
  -v, --version VERSION-ID                              Version ID. Creates a new version if not supplied.
  -l, --label LABEL                                     The label to be used for the version.
  -u, --api-base-url URL                                Base URL for Optimus API
      --save-version                                    Save version on successful load.
      --publish-version                                 Publish version on successful load. Returns validation error if
    --save-version is set to false
  -n, --num-partitions NUM-PARTITIONS                   Number of Spark RDD partitions.
  -t, --table TABLE                                     Table name
  -f, --file  FILE-NAME                                 File name
  -c, --content-type  CONTENT-TYPE                      Content Type for the file specified.
  -b, --batch-size BATCH-SIZE          [Default: 1000]  Batch size for put-entries API call.
  -r, --max-retries MAX-RETRIES        [Default: 10]    Max num of times to retry the load entries API.
  -o, --out REPORT-FILE                                 Location of the report file.
      --local                                           Set spark master to local[*].
  -s, --separator SEPARATOR            [Default: \t]    Field separatorfor the input files.
      --config CONFIG-FILE                              Additional config file
  -h, --help                                            Print help and usage.
```

## License

Copyright © 2018 Trainline.com - All rights reserved.
