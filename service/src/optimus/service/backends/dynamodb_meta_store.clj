;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.backends.dynamodb-meta-store
  "This namespace contains an implementation of
  optimus.service.backend/MetadataStore protocol which uses
  Amazon DynamoDB as its data store"
  (:require [amazonica.aws.dynamodbv2 :as dyn]
            [amazonica.core :as aws]
            [clojure.string :as str]
            [optimus.service.util :refer [NonEmptyString]]
            [schema.core :as s]
            [optimus.service.util :as u]
            [clojure.tools.logging :as log]
            [optimus.service.backends.dynamo-util
             :refer [handle-exceptions aws-config]]
            [optimus.service.backend :refer :all
             :exclude [allowed-status-transitions all-statuses] :as b])
  (:import [com.amazonaws.services.dynamodbv2.model
            ConditionalCheckFailedException]))



(def ^:const DEFAULT-CONFIG
  {;; :region - the aws region's endpoint to contact

   ;; the name of the table for the kv store
   :meta-store-table      "OptimusMetadataStore"

   ;; name of the global secondary index that indexes the table by
   ;; entryType->key combination to allow faster access by types
   ;; eg. get all datasets.
   :type-index-name        "MetaTypeIndex"

   ;; the initial read and write capacity
   ;; for the table and indices
   :read-capacity-units      10
   :write-capacity-units     10})



(defn create-tables
  "Creates the necessary tables required for the dynamodb
  implementation of the meta data store."
  [config]
  (let [{:keys [region meta-store-table read-capacity-units write-capacity-units
                type-index-name]}
        (merge DEFAULT-CONFIG config)]
    (dyn/create-table
     (aws-config config)
     ;;Meta store table
     {:table-name  meta-store-table

      :key-schema
      [{:attribute-name "id"          :key-type "HASH"}]

      :attribute-definitions
      [;;id - Primary key for the item. The value for this attr
       ;;follows the pattern: dataset:<dataset-name> or
       ;;version:<version-id>
       {:attribute-name "id"            :attribute-type "S"}
       ;;__dataset - attr used in secondary index used to fetch all
       ;;versions or all datasets or versions by dataset faster. For a
       ;;dataset item the value of this attr will be the same as the
       ;;name of that dataset, for a version the value will be the
       ;;name of the dataset the version belongs to.
       {:attribute-name "__dataset" :attribute-type "S"}
       ;;__entryType - attr used in secondary index to fetch all the
       ;;versions or all datasets faster. The value of this attr will
       ;;be "dataset" for a dataset item and "version" for a version
       ;;item.
       {:attribute-name "__entryType"   :attribute-type "S"}]

      :provisioned-throughput
      {:read-capacity-units  10 :write-capacity-units 10}


      :global-secondary-indexes
      ;; type index - secondary index with the PK set to __entryType
      ;; and __dataset set to the dataset name. This will allow
      ;; retrieving all datasets (or) all versions (or) versions by
      ;; dataset name faster.
      [{:index-name type-index-name
        :key-schema
        [{:attribute-name "__entryType"  :key-type "HASH"}
         {:attribute-name "__dataset"    :key-type "RANGE"}]

        :projection {:projection-type "ALL"}

        :provisioned-throughput
        {:read-capacity-units 10 :write-capacity-units 10}}]})))



(defn- append-timestamp
  "Add timestamp to  audit-info"
  [audit-info]
  (assoc audit-info :timestamp (System/currentTimeMillis)))



;; InMemMetadatastore - In-Memory implementation of MetaDataStore.
(defrecord DynamoDBMetadataStore [config]
  MetadataStore

  ;; Creates a dataset if the arguments supplied are valid. Throws an exception
  ;; if a dataset already exists with the given dataset name. Takes an optional
  ;; map of audit information, adds a timestamp and persists it with the Dataset
  (create-dataset [this dataset]
    (let [{:keys [region meta-store-table]} config
          audit-info  (-> (get dataset :audit-info {})
                          (assoc :action :created)
                          (append-timestamp)
                          vector)
          enriched_ds (-> (merge DATASET-DEFAULTS dataset)
                          (dissoc :audit-info)
                          (assoc  :operation-log audit-info))
          dsname (:name dataset)
          id (str "dataset:" dsname)]

      (s/validate Dataset enriched_ds)

      (try
        (dyn/put-item (aws-config config)
                      {:table-name meta-store-table
                       :item {:id id
                              :value enriched_ds
                              :__entryType   "dataset"
                              :__dataset dsname
                              :__ver   0}
                       :condition-expression "attribute_not_exists(id)"})
        (catch ConditionalCheckFailedException ce
          (u/throw-validation-error "Dataset already exists" {:id id})))
      this))



  ;; Returns a single dataset with the supplied dataset name. Returns nil if no
  ;; dataset exists with the supplied dataset name. Value returned conforms to
  ;; service.kv/Dataset
  (get-dataset [mstore dsname]
    (let [{:keys [region meta-store-table type-index-name]} config]
      (->  (dyn/get-item (aws-config config)
                         {:table-name meta-store-table
                          :key {:id (str "dataset:" dsname)}})
           (get-in [:item :value]))))



  ;; Returns all Datasets in the metadata store. Returns a list of values that
  ;; conform to service.kv/Dataset
  (get-all-datasets [mstore]
    (let [{:keys [region meta-store-table type-index-name]} config]
      (->>  (dyn/query (aws-config config)
                       {:table-name meta-store-table
                        :index-name type-index-name
                        :expression-attribute-names
                        {"#et" "__entryType"}
                        :expression-attribute-values
                        {":et" "dataset"}
                        :key-condition-expression
                        "#et = :et"
                        })
            :items
            (map #(get % :value)))))



  ;; Creates a new version for the specified dataset with the specified lable
  ;; and id if the arguments supplied are valid. Takes an optional map of audit
  ;; information, adds a timestamp and action='created' and persists it with the
  ;; Dataset. Throws an excpetion if no dataset exists with the specified
  ;; dataset name. Defaults status of the Version to :preparing as per the
  ;; contract.
  (create-version [this {:keys [audit-info id dataset] :as version}]
    (let [audit-info  (->  (merge audit-info {})
                           (append-timestamp)
                           (assoc :action :created)
                           vector)
          enriched_ver (-> (merge VERSION-DEFAULTS version)
                           (dissoc :audit-info)
                           (assoc  :operation-log audit-info))
          {:keys [meta-store-table region]}   config]

      (s/validate Version enriched_ver)

      (if (nil? (get-dataset this dataset))
        (throw (ex-info "Dataset not found" {}))
        (try
          (dyn/put-item (aws-config config)
                        {:table-name meta-store-table
                         :item {:id (str "version:" id)
                                :value enriched_ver
                                :__entryType "version"
                                :__dataset   dataset
                                :__ver       0}
                         :condition-expression "attribute_not_exists(id)"})
          (catch ConditionalCheckFailedException ce
            (u/throw-validation-error "Version already exists" {}))))
      this))



  ;; Returns all versions for all datasets. Returns a list of values that
  ;; conform to service.kv/Version
  (get-all-versions [mstore]
    (let [{:keys [region meta-store-table type-index-name]} config]
      (->>  (dyn/query (aws-config config)
                       {:table-name meta-store-table
                        :index-name type-index-name
                        :expression-attribute-names
                        {"#entryType" "__entryType"}
                        :expression-attribute-values
                        {":et" "version"}
                        :key-condition-expression
                        "#entryType = :et"})
            :items
            (map #(get % :value))
            (map #(update % :status keyword))
            seq)))



  ;; Returns all versions for a given dataset name. Returns a list of values
  ;; that conform to service.kv/Version
  (get-versions [mstore dsname]
    (s/validate DatasetName dsname)

    (let [{:keys [region meta-store-table type-index-name]} config]
      (->> (dyn/query (aws-config config)
                      {:table-name meta-store-table
                       :index-name type-index-name
                       :expression-attribute-names
                       {"#ds" "__dataset"
                        "#et" "__entryType"}
                       :expression-attribute-values
                       {":et" "version"
                        ":ds" dsname}
                       :key-condition-expression
                       "#et = :et AND #ds = :ds"})
           :items
           (map #(get % :value))
           (map #(update % :status keyword))
           seq)))



  ;; Returns the version with the specified version-id. The value returned
  ;; conforms to service.kv/Version
  (get-version [mstore version-id]
    (s/validate NonEmptyString version-id)

    (let [{:keys [region meta-store-table type-index-name]} config
          version (-> (dyn/get-item (aws-config config)
                                    {:table-name meta-store-table
                                     :key {:id (str "version:" version-id)}})
                      (get-in [:item :value]))]
      (if version
        (update version :status keyword)
        nil)))



  ;; Updates status of the Version if the arguments supplied are valid. Throws
  ;; an exception if:
  ;; 1. Arguments supplied do not conform to their schemas.
  ;; 2. version-id specified doesnt exist.
  ;; 3. A transition requested is invalid.
  ;;    See: service.kv/allowed-status-transitions
  ;;         and service.kv/status-transition-allowed
  (update-status [mstore version-id target-status audit-info]
    (let [id (str "version:" version-id)
          {:keys [region meta-store-table type-index-name]} config]

      (s/validate NonEmptyString version-id)
      (s/validate Status target-status)
      (s/validate AuditInfo audit-info)

      (let [version-record (-> (dyn/get-item
                                (aws-config config)
                                {:table-name meta-store-table
                                 :key {:id id}})
                               :item)

            version (:value version-record)

            oplog {:action (str (:status version) " -> " target-status)
                   :timestamp (u/now)}]

        (when-not (b/status-transition-valid? (keyword (:status version))
                                              target-status)
          (u/throw-validation-error "Invalid state transition"
                                    {:current-state version}))

        (try
          (dyn/update-item
           (aws-config config)

           {:table-name meta-store-table
            :key {:id id}

            :expression-attribute-names
            {"#v"   "value"
             "#s"   "status"
             "#log" "operation-log"
             "#ver"  "__ver"}

            :expression-attribute-values {":targetstatus" (name target-status)
                                          ":newlog"       [(merge oplog audit-info)]
                                          ":currver"      (:__ver version-record)
                                          ":one"          1}

            :update-expression "SET #v.#s=:targetstatus,
                                    #v.#log=list_append(:newlog, #v.#log)
                                    ADD #ver :one"

            :condition-expression "attribute_exists(id) AND #ver=:currver"})

          (catch ConditionalCheckFailedException ce
            (log/error "invalid state error: conv" ce)
            (u/throw-conflict "Entity updated concurrently"
                              {:current-state version}))))

      mstore))


  ;; Activates the version by setting version.dataset.active-version to the
  ;; specified version-id. Throws an exception if the current status of the
  ;; version is not in the `published` state.
  (activate-version [mstore version-id]

    (s/validate NonEmptyString version-id)

    (let [version (get-version mstore version-id)
          {:keys [region meta-store-table type-index-name]} config]

      (cond
        (nil? version) (u/throw-missing-entity "Version does not exist")

        (not= (:status version) :published
              ) (u/throw-validation-error
                 "Cannot activate a version that is not published"
                 {:current-state version})

        :else nil)

      (let [dataset-id (str "dataset:" (:dataset version))

            dataset-record (-> (dyn/get-item (aws-config config)
                                             {:table-name meta-store-table
                                              :key {:id dataset-id}})
                               :item)

            dataset (:value dataset-record)

            oplog {:action (str "active-version: " (:active-version dataset)
                                " -> " version-id)
                   :timestamp (u/now)}]

        (when-not dataset-record
          (u/throw-validation-error "Dataset does not exist" {}))

        (try
          (dyn/update-item
           (aws-config config)

           {:table-name meta-store-table
            :key {:id dataset-id}

            :expression-attribute-names
            {"#v"   "value"
             "#av"   "active-version"
             "#log" "operation-log"
             "#ver" "__ver"}

            :expression-attribute-values {":targetver"    version-id
                                          ":newlog"       [oplog]
                                          ":currver"      (:__ver dataset-record)
                                          ":one"          1}

            :update-expression "SET #v.#av=:targetver,
                                    #v.#log=list_append(:newlog, #v.#log)
                                    ADD #ver :one"

            :condition-expression "attribute_exists(id) AND #ver=:currver"})

          (catch ConditionalCheckFailedException ce
            (log/error "invalid state error: conv" ce)
            (u/throw-conflict "Entity updated concurrently"
                              {:current-state dataset}))))

      mstore)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;              ---==| E X C E P T I O N   W R A P P E R |==----              ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Wraps DynamoDBMetadataStore and implements custom behaviour for certain exceptions.
;; See dynamodb-util.handle-exceptions for more info.
(defrecord DynamoDBMetadataStoreExceptionHandler [mstore]
  MetadataStore

  (create-dataset [_ dataset]
    (DynamoDBMetadataStoreExceptionHandler.
     (handle-exceptions create-dataset mstore dataset)))



  (get-dataset [_ dsname]
    (handle-exceptions get-dataset mstore dsname))



  (get-all-datasets [_]
    (handle-exceptions get-all-datasets mstore))


  (create-version [_ version]
    (DynamoDBMetadataStoreExceptionHandler.
     (handle-exceptions create-version mstore version)))



  (get-all-versions [_]
    (handle-exceptions get-all-versions mstore))


  (get-versions [_ dsname]
    (handle-exceptions get-versions mstore dsname))



  (get-version [_ version-id]
    (handle-exceptions get-version mstore version-id))



  (update-status [_ version-id target-state audit-info]
    (DynamoDBMetadataStoreExceptionHandler.
     (handle-exceptions update-status mstore version-id target-state audit-info)))



  (activate-version [_ version-id]
    (DynamoDBMetadataStoreExceptionHandler.
     (handle-exceptions activate-version mstore version-id))))



(defn dynamodb-meta-data-store
  "Creates a new DynamoDB meta data store"
  [config]
  (->> (DynamoDBMetadataStore. config)
       (DynamoDBMetadataStoreExceptionHandler.)))
