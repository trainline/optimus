;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.core
  "This namespace is the business logic layer for the Optimus service.
   Functions in this namespace perform the necessary orchestrations across the
   meta data store, kv store and queue that feeds the async task processor."
  (:require [optimus.service
             [backend :as b]
             [util :refer [rand-id throw-core-exception
                           throw-missing-entity throw-validation-error
                           metrics-name]]]
            [clojure.core.memoize :refer [ttl]]
            [samsara.trackit :refer :all]
            [where.core :refer [where]]))

;; BackendStore - record to encapsulate the backend components of the Model Data
;; Store
(defrecord BackendStore
    [queue meta-store kv-store pid config])



(defn ->BackendStore
  ([config]
   (BackendStore. nil nil nil nil config))
  ([queue meta-store kv-store pid config]
   (BackendStore. queue meta-store kv-store pid config)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                       ---==| D A T A S E T S |==----                       ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn create-dataset
  "Creates a new dataset for the given dataset name.
   Returns the dataset created."
  [{:keys [meta-store]} {:keys [name] :as dataset}]
  (track-time
   (metrics-name :core :create-dataset)
   (-> (b/create-dataset meta-store dataset)
       (b/get-dataset name))))



(defn get-dataset
  "Returns dataset for the given dataset name"
  [{:keys [meta-store]} dsname]
  (track-time
   (metrics-name :core :get-dataset)
   (b/get-dataset meta-store dsname)))



(defn get-datasets
  "Returns all datasets"
  [{:keys [meta-store]}]
  (track-time
   (metrics-name :core :get-dataset)
   (b/get-all-datasets meta-store)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                       ---==| V E R S I O N S |==----                       ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn create-version
  "Creates a new version and sends a :prepare event to the queue.
   Returns the version created."
  [{:keys [queue meta-store config] :as backend} version]
  (track-time
   (metrics-name :core :create-version)
   (let [version-id (rand-id)]
     (b/create-version meta-store (assoc version :id version-id))
     (b/send-message! queue
                      (get-in config [:async-task :operations-topic])
                      {:action :prepare :version-id version-id})
     (b/get-version meta-store version-id))))



(defn get-version
  "Returns a version for the given version-id"
  [{:keys [meta-store]} version-id]
  (track-time
   (metrics-name :core :get-version)
   (b/get-version meta-store version-id)))



(defn get-versions
  "Returns all versions for a given dataset name"
  [{:keys [meta-store]} dsname]
  (track-time
   (metrics-name :core :get-versions)
   (b/get-versions meta-store dsname)))



(defn save-version
  "Changes the status of the specified version to :saving and publishes a `save`
   event to the async task queue."
  [{:keys [meta-store queue config]} version-id]
  (track-time
   (metrics-name :core :save-version)
   (let [{:keys [status] :as version} (b/get-version meta-store version-id)]
     (cond
       ;; Version not found
       (nil? version) (throw-missing-entity "version not found"
                                            {:error :version-not-found})

       ;; invalid status - version must be :awaiting-entries to load data
       (not
        (b/status-transition-valid?
         status :saving)) (throw-validation-error "invalid version state"
                                                  {:error :invalid-version-state
                                                   :version version})

       :else (do
               (b/update-status meta-store version-id :saving {})
               (b/send-message! queue
                                (get-in config [:async-task :operations-topic])
                                {:action :save :version-id version-id}))))))



(defn publish-version
  "Changes the status of the specified version to :publishing and publishes a
   `publish` event to the async task queue."
  [{:keys [meta-store queue config]} version-id]
  (track-time
   (metrics-name :core :publish-version)
   (let [{:keys [status] :as version} (b/get-version meta-store version-id)]
     (cond
       ;; Version not found
       (nil? version) (throw-missing-entity "version not found"
                                            {:error :version-not-found})

       ;; invalid status - version must be :awaiting-entries to load data
       (not
        (b/status-transition-valid?
         status :publishing)) (throw-validation-error "invalid version state"
                                                      {:error :invalid-version-state
                                                       :version version})

       :else (do
               (b/update-status meta-store version-id :publishing {})
               (b/send-message! queue
                                (get-in config [:async-task :operations-topic])
                                {:action :publish :version-id version-id}))))))



(defn discard-version
  "Discards the version corresponding to the specified version-id and
  appends the specified reason to the operation log."
  [{:keys [meta-store queue]} version-id reason]
  (track-time
   (metrics-name :core :discard-version)
   (let [{:keys [status] :as version} (b/get-version meta-store version-id)]
     (cond
       ;; Version not found
       (nil? version) (throw-missing-entity "version not found"
                                            {:error :version-not-found})

       ;; invalid status - version must be :awaiting-entries to load data
       (not
        (b/status-transition-valid?
         status :discarded)) (throw-validation-error "invalid version state"
                                                     {:error :invalid-version-state})

       :else (do
               (b/update-status meta-store version-id :discarded {:reason reason}))))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                      ---==| L O A D   D A T A |==----                      ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def memoized-get-dataset (memoize b/get-dataset))



(defn dataset-missing-in
  "a `where` comparator that returns true if the dataset specified does not
   exist in the meta data store."
  [dataset meta-store]
  (not (memoized-get-dataset meta-store dataset)))



(defn table-missing-in
  "a `where` comparator that returns true if the table specified does not
   exist for corresponding dataset in the meta data store."
  [{:keys [dataset table]} meta-store]
  (not (if-let [ds (memoized-get-dataset meta-store dataset)]
         (get-in ds [:tables table] nil)
         false)))



(defn missing-datasets
  "Returns a collection of dataset names in the entries specified,
   that do not exist in the meta data store."
  [meta-store entries]
  (->>  entries
        (filter (where :dataset dataset-missing-in meta-store))
        (map #(get % :dataset))))



(defn missing-tables
  "Returns the entries for which the specified table name does not exist
    for the corresponding dataset in the meta data store"
  [meta-store entries]
  (->>  entries
        (filter (where table-missing-in meta-store))))



(defn validate-entries
  "Validates the entries specified. This function does NOT validate the
   schema of the entries supplied. It only checks if the tables
   in the request exist in the meta data store.
   Returns a vector that contains the result of validation and a map that
   contains missing tables. eg.
   [false {:missing-tables nil}
   (or)
   [false {:missing-tables ({:dataset \"ds1\" :table \"t2\"})}]
   (or)
   [true nil]"
  [meta-store entries]
  (let [ds-tbls (->> entries (map #(select-keys % [:dataset :table])) distinct)
        missing-tb (seq (missing-tables   meta-store ds-tbls))]

    (if (nil? missing-tb)
      [true nil]
      [false {:missing-tables missing-tb}])))



(defn- entry->kv
  "Transforms a flat entry that looks like: {:dataset :table :key :value}
   to a kv pair [{:dataset :table :key} :value]"
  [entry]
  (-> entry
      (select-keys [:dataset :table :version :key])
      vector
      (conj (:value entry))))



(defn- load-data
  "Creates `entries` by adding dataset name and version to the specified
   table-key-value pairs and loads them into the kv-store. Validates the
   dataset and table names supplied and throws an exception when the validation
   fails. See `create-entries` for information reg ex-data passed in the
   exception. Returns :ok if the load was successful."
  [{:keys [kv-store meta-store]} version-id dsname tbl-key-vals]
  ;; Structure of tbl-key-vals => [{:table "table" :key "key" :value "value"}]
  ;; Must be transformed to: {{:dataset "ds" :table "table" :key "key"} value}}
  ;; below structure in-order to be passed to the backend
  ;; Transformation Steps:
  ;; - conjoin {:dataset dsname :version version-id} to al in tbl-key-vals
  ;; -
  (let [entries (->> tbl-key-vals
                     (map #(conj {:dataset dsname :version version-id} %))
                     (mapcat entry->kv)
                     (apply hash-map))
        [valid? errors] (validate-entries meta-store (keys entries))]

    (if-not valid?
      (throw-missing-entity "Table(s) not found"
                            (conj {:error :tables-not-found} errors)))

    ;; Return true if put was successful.
    (b/put-many kv-store entries)
    :ok))



(defn create-entries
  "Loads data for the specified version. Throws an exception in the following
   cases with the given message and ex-data with 2 keys :error and additional
   information containing context related to the error.

   | Condition                | :error                       | :addl-info         |
   |--------------------------+------------------------------+--------------------|
   | Version:                 |                              |                    |
   | - not found              | :version-not-found           | nil                |
   | - does not match dataset | :invalid-version-for-dataset | {:version}         |
   | - invalid state          | :invalid-version-state       | {:version}         |
   |                          |                              |                    |
   | Data                     | :invalid-request             | {:missing-tables}  |
   |                          |                              |                    |

   Returns :ok on success."
  ([{:keys [meta-store kv-store] :as bknd-store} version-id dsname tbl-key-vals]
   (track-time
    (metrics-name :core :create-entries)
    (let [{:keys [dataset status] :as version} (b/get-version meta-store version-id)]
      (cond
        ;; Version not found
        (nil? version) (throw-missing-entity "version not found"
                                             {:error :version-not-found})

        ;; version does not match dataset
        (not= dataset dsname) (throw-validation-error
                               "version does not match dataset"
                               {:error :invalid-version-for-dataset
                                :version version})

        ;; invalid status - version must be :awaiting-entries to load data
        (not= status :awaiting-entries) (throw-validation-error
                                         "invalid version state"
                                         {:error :invalid-version-state
                                          :version version})

        :else (load-data bknd-store version-id dsname tbl-key-vals)))))



  ([bknd-store version-id dsname table-name key-vals]
   (track-time
    (metrics-name :core :create-entries table-name)
    (->> key-vals
         (map #(conj {:table table-name :version version-id} %))
         (create-entries bknd-store version-id dsname))))



  ([{:keys [meta-store] :as bknd-store} version-id dsname table-name key value]
   ;;Validate if dataset is present.
   (track-time
    (metrics-name :core :create-entry)
    (create-entries bknd-store version-id dsname
                    [{:table table-name :key key :value value}]))))



(defn get-active-version*
  "Returns the active version of the dataset"
  [bknd dataset-name]
  (let [{:keys [active-version] :as dataset} (get-dataset bknd dataset-name)]
    (cond
      (nil? dataset) (throw-missing-entity "dataset does not exist" {})
      (nil? active-version) (throw-validation-error "no active version for dataset" {})
      :else active-version)))



(def get-active-version
  "Returns the active version of the dataset. This function calls the
  get-active-version* function and caches the output for 10
  seconds."
  (ttl
   (fn [bknd dataset-name] (get-active-version* bknd dataset-name))
   :ttl/threshold 10000))



(defn get-entry
  "Return a single entry from the keyvalue store for the specified
  version-id dataset table key combination. Returns a map that contains
  the following keys::
  :active-version-id - version currently active for the dataset requested.
  :version-id        - version of the data returned
  :data              - value for the requested key"
  [{:keys [kv-store] :as bknd} version-id dataset table key]
  (track-time
   (metrics-name :core :get-entry)
   (let [active-version (get-active-version bknd dataset)
         version-id     (if version-id version-id active-version)]
     (->> {:dataset dataset :version version-id :table table :key key}
          (b/get-one kv-store)
          (assoc {:active-version-id active-version
                  :version-id        version-id}
                 :data)))))



(defn get-entries
  "Returns entries from the key value store for the specified
  version-id dataset table and keys.Returns a map that contains
  the following keys::
  :active-version-id - version currently active for the dataset requested.
  :version-id        - version of the data returned
  :data              - values for the requested keys"
  ([{:keys [kv-store] :as bknd} version-id dataset table keys]
   ;; keys - [{:key String}]
   (track-time
    (metrics-name :core :get-entries)
    (let [active-version (get-active-version bknd dataset)
          version-id     (if version-id version-id active-version)]
      (->> keys
           (map #(conj {:dataset dataset :table table :version version-id} %))
           (b/get-many kv-store)
           (assoc {:active-version-id active-version
                   :version-id        version-id} :data))))))
