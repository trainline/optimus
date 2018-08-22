;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.backends.dynamodb-kv
  "This namespace contains an implementation of
  optimus.service.backend/KV protocol which uses Amazon DynamoDB
  as its data store"
  (:require [amazonica.aws.dynamodbv2 :as dyn]
            [amazonica.core :as aws]
            [clojure
             [set :refer [rename-keys]]
             [string :as str]]
            [clojure.tools.logging :as log]
            [optimus.service
             [backend :refer :all]
             [util :as u]]
            [optimus.service.backends.dynamo-util
             :refer
             [aws-config handle-exceptions]]
            [optimus.service.backends.middleware.binary-kv-store
             :refer
             [binary-kv-store]]
            [samsara.trackit :refer :all]
            [schema.core :as s]))

(def ^:const DEFAULT-CONFIG
  {;; :region - the aws region's endpoint to contact

   ;; the name of the table for the kv store
   :kv-store-table      "OptimusKV"

   ;; name of the version-index
   :version-index-name  "VersionIndex"

   ;; the initial read and write capacity
   ;; for the table and indices
   :read-capacity-units      10
   :write-capacity-units     10

   :max-batch-size           1000

   ;; aws client config
   ;; the aws client config to pass to calls to
   ;; dynamodb.
   :aws-client-config    nil
   })



(defn create-tables
  "Creates the necessary tables required for the dynamodb
  implementation of the kv store."
  [config]
  (let [{:keys [kv-store-table version-index-name
                read-capacity-units write-capacity-units] :as config}
        (merge DEFAULT-CONFIG config)]
    (dyn/create-table
     (aws-config config)
     ;;KV Store Table
     {:table-name kv-store-table

      :key-schema
      [{:attribute-name "id"          :key-type "HASH"}]

      :attribute-definitions
      [{:attribute-name "id"          :attribute-type "S"}
       ;; __version - attr used in secondary index to fetch all keys
       ;; that belongs to a version efficiently. This is useful
       ;; when verifying data by count and also during garbage
       ;; collection
       {:attribute-name "__version"   :attribute-type "S"}]

      :global-secondary-indexes
      [;; version-index - Index by version-id to retrieve all keys
       ;; by version efficiently. Useful when verifying data by count
       ;; and durfprojeing garbage collection.
       {:index-name version-index-name

        :key-schema
        [{:attribute-name "__version" :key-type "HASH"}]

        :projection {:projection-type "KEYS_ONLY"}

        :provisioned-throughput
        {:read-capacity-units 10 :write-capacity-units 10}}]

      :provisioned-throughput
      {:read-capacity-units read-capacity-units
       :write-capacity-units write-capacity-units}
      })))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;              ---==| D Y N A M O D B   K V   S T O R E |==----              ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn key->id
  "Converts the Key part of key-value pair to an identifier which can be used
  as the primary key in dynamodb. For eg.
  {:dataset \"ds1\" :table \"table1\" :key \"key1\" :version \"1.0\"}
  will be transformed to:
  \"ds1:1.0:table1:key1\""
  [{:keys [version dataset table key] :as akey}]
  (str/join ":" [dataset version table key]))



(defn version->__version
  "Takes a version and returns a high cardinality representation by
  appending a random number at the end. This is required to distribute
  writes efficiently to prevent unexpected
  throttling. https://amzn.to/2IiJcTR"
  [version]
  (let [suffix (format "%03d" (rand-int 100))]
    (some-> version (str "::" suffix))))



(defn add-id
  "Adds the :id attribute to the key specified. The value of the :id attr is
  computed using the key->id function"
  [key]
  (->> key key->id (assoc key :id)))


(defn- put-batch
  [config kv-store-table entries]
  (->> entries
       ;; transform entries in {Key s/Any} format
       ;; to a list of maps that conform to the structure
       ;; of "PutRequest" in the BatchWrite API.
       (map (fn [[k v]]
              (->> {:id (key->id k)
                    :value v
                    :__version (-> k :version version->__version)}
                   (assoc-in {} [:put-request :item]))))
       (assoc-in {} [:request-items kv-store-table])
       (dyn/batch-write-item config)))



(defn- get-batch
  [config kv-store-table keys]
  (track-time
   (u/metrics-name :kvstore :dynamodb :batch-get-item)
   (let [resp (dyn/batch-get-item config keys)]
     ;; if dynamo returns unprocessed items, throw too-many-requests exception
     (when-let [unprocessed (:unprocessed-keys resp)]
       (u/throw-too-many-requests
        "Unprocessed items returned by batch-get-item"
        {:unprocessed-count
         (-> kv-store-table keyword :keys count)
         ;;:unprocessed-items unprocessed
         }))
     resp)))


;; DyanamoDBKV
;;
;; Implementation of the KV protocol which uses DynamoDB as the backend
;; data store.
;;
;; Storage:
;; The KV Pairs are stored in one DynamoDB table which is specified by
;; the :kv-store-table attribute in the config. The primary key of the
;; DyanamoDB table (id) is derived from the attributes of the key and is of
;; the format: dataset:version:table:key. The value attribute is stored
;; as a 'map' type.
;;
;; Concurrency Control:
;; DynamoDBKV does not implement MVCC or any other form of
;; concurrency control and provides no guarantees in case of
;; concurrent updates to the same key. Any errors/corruption caused by
;; concurrent updates during load should be caught by the "data verfication"
;; step.
;;
(defrecord DynamoDBKV [config]
  KV

  (put-one [this {:keys [version dataset table key] :as akey} value]
    ;; Puts any single kv pair to dynamo db. The operation here is
    ;; effectively an upsert updating the value of the given key
    ;; if it already exists in the dynamodb table.
    (let [{:keys [kv-store-table]} config]
      (dyn/put-item (aws-config config)
                    {:table-name kv-store-table
                     :item {:id        (key->id akey)
                            :value     value
                            :__version (version->__version version)}}))
    this)



  (get-one [_ akey]
    (let [{:keys [kv-store-table]} config]
      (-> (dyn/get-item (aws-config config)
                        {:table-name kv-store-table
                         :key {:id (key->id akey)}})
          (get-in [:item :value]))))



  (get-many [_ rkeys]
    (let [{:keys [kv-store-table]} config
          ;;construct a map of id -> key like below:
          ;;{"ds1:v1:t1:k1" {:dataset "ds1" :version "v1" :table "t1" :key "k1"}
          id-keys (zipmap (map key->id rkeys) rkeys)
          ;; Build a default response map with key = rkey and val = nil
          default-response (zipmap rkeys (repeat (count rkeys) nil))]

      ;; TODO: refactor this to prepare-request and process-response.
      (as-> (keys id-keys) $
        (map #(assoc {} "id" %) $)
        ;; construct the request and call dyn/batch-get-item
        (assoc-in {} [:request-items kv-store-table :keys] $)

        (get-batch (aws-config config) kv-store-table $)

        (get-in $ [:responses (keyword kv-store-table)])
        ;; response is in the format: [{:id "id" :value "value"}]
        ;; convert to {"id" "value"}
        (mapcat (fn [v] [(:id v) (:value v)]) $)
        (apply hash-map $)
        ;; replace the keys (:id) with the corresponding keys in the
        ;; request
        (rename-keys $ id-keys)
        ;; add the rest of the keys with value set to nil
        (merge default-response $))))



  (put-many [this entries]
    (let [{:keys [kv-store-table max-batch-size]} config]

      (when (> (count entries) max-batch-size)
        (u/throw-validation-error
         (str  "Too many records. Max batch size allowed is"
               max-batch-size) {}))

      ;; DynamoDB batch-write-items has a limit of 25 entries per
      ;;call.
      (doseq [batch (partition-all 25 entries)]
        (let [resp (put-batch (aws-config config) kv-store-table batch)]
          (when-let [unprocessed (:unprocessed-items resp)]
            (u/throw-too-many-requests
             "Unprocessed items returned by batch-write-item"
             {:unprocessed-count
              (->> kv-store-table
                   keyword
                   (get unprocessed)
                   count)
              ;;:unprocessed-items unprocessed
              }))))

      this)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;              ---==| E X C E P T I O N   W R A P P E R |==----              ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Wraps DynamoDBKV and implements custom behaviour for certain exceptions.
;; See dynamodb-util.handle-exceptions for more info.
(defrecord DynamoDBKVExceptionHandler [kvstore]
  KV

  (put-one [_ key val]
    (DynamoDBKVExceptionHandler.
     (handle-exceptions put-one kvstore key val)))



  (get-one [_ key]
    (handle-exceptions get-one kvstore key))



  (get-many [_ keys]
    (handle-exceptions get-many kvstore keys))



  (put-many [_ entries]
    (DynamoDBKVExceptionHandler.
     (handle-exceptions put-many kvstore entries))))



(defn dynamodb-kv
  "Creates a new instance of DynamoDB KV store"
  [config]
  (->> (merge DEFAULT-CONFIG config)
       (DynamoDBKV.)
       (DynamoDBKVExceptionHandler.)))
