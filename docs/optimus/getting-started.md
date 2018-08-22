---
title: Getting Started
layout: overview
weight: 20
---

Getting started with Optimus is really easy.

### Prerequisites:

* Install Java SDK (1.8+).
* Install [Leiningen](https://leiningen.org)

### Quick start (using in-memory database)
You can quickly fire up a version of Optimus which uses an in-memory database. The obvious downside is that you will lose all data once Optimus is shut down. Optimus also has built-in support for DynamoDB. (See: "Getting started with DynamoDB section below".)

##### Start the API and create a dataset.
Navigate to the `service` directory and execute the following commands:

```
#build standalone executable
optimus/service$ lein uberjar

#start Optimus API
optimus/service$ java -jar target/optimus-0.1.8-standalone.jar
```

Fire up a browser and navigate to [http://localhost:8888/](http://localhost:8888) to see the Swagger documentation for the API.

Lets create a new dataset `recommendations` with one table `items`.

```
curl -H 'Content-Type: application/json' -XPOST http://localhost:8888/v1/datasets -d '{
"name": "recommendations",
"tables": ["items"],
"content-type": "application/json"
}'
```

##### Load initial data
Navigate to the `loader` directory and execute the following commands:

```
#build standalone executable
optimus/loader lein uberjar

optimus/loader$ java -jar target/loader-0.1.8-standalone.jar  \
    --dataset recommendations  \
    --api-base-url http://localhost:8888/v1 \
    --table items --file dev/examples/sample_v001.tsv --content-type json \
    --out report \
    --save-version \
    --publish-version \
    --local
```

The above command orchestrates a full load workflow which involves the following steps:
* Create a new version for the dataset.
* Wait until the status of the version is `awaiting-entries`.
* Load data from the tsv file.
* Save the version.
* Wait until the status of the version is `saved`.
* Publish the version.

Detailed API interaction is available [here.](api-interaction.md)

Let us inspect the data loaded. Pick the `version-id` from the output report generated in the `report/` folder.

Lets inspect the recommendation scores for key `item001`.

```
curl -v "http://localhost:8888/v1/datasets/recommendations/tables/items/entries/item001"

#response
...
< X-Active-Version-Id: 2e0xc3scfnevqys3yix0ssf8z
< X-Version-Id: 2e0xc3scfnevqys3yix0ssf8z
...
{"scores": [{"productId": "item000", "score": 100}, {"productId": "item003", "score":200}, {"productId": "item004", "score":500}]}
```

You can observe the following from the response:
* output contains 3 scores.
* The `X-Active-Version-Id` and `X-Version-Id` in the HTTP headers returned are the same as the version requested. This is because setting `--publish-version` option in the load command calls the `publish` API which sets this version as the `active-version` for the dataset. Once published, data in this version of the dataset will be used to serve all requests made to retrieve data.

##### Load a new version.

Lets load a new version of the data.

```
#build standalone executable
optimus/loader$ java -jar target/loader-0.1.8-standalone.jar  \
    --dataset recommendations  \
    --api-base-url http://localhost:8888/v1 \
    --table items --file dev/examples/sample_v002.tsv --content-type json \
    --out report_v2 \
    --save-version \
    --local
```

Note that the above command drops the `--publish-version` option. The Loader will not auto publish the version. Pick the `version-id` from the output report generated in the `report/` folder.

Lets inspect the recommendation scores for key `item001`.

```
curl -v "http://localhost:8888/v1/datasets/recommendations/tables/items/entries/item000"

#response
...
< X-Active-Version-Id: 2e0xc3scfnevqys3yix0ssf8z
< X-Version-Id: 2e0xc3scfnevqys3yix0ssf8z
...
{"scores": [{"productId": "item000", "score": 100}, {"productId": "item003", "score":200}, {"productId": "item004", "score":500}]}
```

You can observe that the response still contains data from the previous version. Lets explicitly tell Optimus to return data for the new version.

```
# replace the value of version-id with the version-id from the previous step.
curl -v "http://localhost:8888/v1/datasets/recommendations/tables/items/entries/item001?version-id=39qcuhrid08anz6r9lejx4zh7

#response
...
< X-Active-Version-Id: 2e0xc3scfnevqys3yix0ssf8z
< X-Version-Id: 39qcuhrid08anz6r9lejx4zh7
...
{"scores": [{"productId": "item003", "score":200}, {"productId": "item004", "score":500}]}
```
You can make the following observations from the response:
* item000 has been removed from the recommendations. The response only has 2 scores.
* `X-Version-Id` matches the version-id supplied in the request, but `X-Active-Version-Id` is still set to the previous version.

##### Publish the new version
To publish the new version execute the following command
```
curl -XPOST "http://localhost:8888/v1/versions/39qcuhrid08anz6r9lejx4zh7/publish"

#response
{"status":"ok","message":"publish in progress","id":"39qcuhrid08anz6r9lejx4zh7"}

# wait until the status of the version is set to 'published'
curl http://localhost:8888/v1/versions/39qcuhrid08anz6r9lejx4zh7
```

Requesting the latest value for `item001` now returns data from the new version.
```
# replace the value of version-id with the version-id from the previous step.
curl -v "http://localhost:8888/v1/datasets/recommendations/tables/items/entries/item001

#response
...
< X-Active-Version-Id: 39qcuhrid08anz6r9lejx4zh7
< X-Version-Id: 39qcuhrid08anz6r9lejx4zh7
...
{"scores": [{"productId": "item003", "score":200}, {"productId": "item004", "score":500}]}
```

### Getting started with DynamoDB
Optimus requires 3 backend tables - a kv store, meta data store and a dynamic tasks store. To create all 3 tables with the default settings, execute the following:

```
optimus/service$ lein with-profile dev run -m optimus.dev-tools.create-dynamodb-tables [AWS-REGION]
```

Start optimus API with a path to config EDN file. (you can use the sample dev configuration in the repo). For detailed documentation around config, refer to the `src/optimus/service/main.clj`.

```
optimus/service$ java -jar target/optimus-0.1.8-standalone.jar config/dev.edn
```
