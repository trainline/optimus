;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.backend
  "This ns contains protocols for a KV store, implementations of which
  can then be used as the backend for Optimus."
  (:refer-clojure :exclude [get])
  (:require [optimus.service
             [util :refer [WebSafeNonEmptyString metrics-name]]]
            [samsara.trackit :refer :all]
            [schema.core :as s]
            [clojure.tools.logging :as log]
            [clojure.string :as str]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                   ---==| K V   P R O T O C O L S |==----                   ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def Key
  "The 'Key' in Optimus is composed of dataset name, version, table and
  a key. Values are always persisted and retrieved using this composite Key."
  {:version s/Str :dataset s/Str :table s/Str :key WebSafeNonEmptyString})



(def Entries
  "Map of 'Key' Value pairs. Used in Batch put and get operations."
  {Key s/Any})



(defprotocol KV
  "Protocol for a simple KV Store."

  (put-one [kvstore {:keys [version dataset table key] :as akey} val]
    "Associates a specified value to the specified key. The key supplied
     must conform to service.kvstore/Key. It returns an updated kvstore.

        example:

        (put-one kvstore
          {:dataset \"d1\" :table \"t1\" :version \"v1\" :key \"k1\"}
          {:foo 1 :bar \"value\"})
        ;;=> kvstore'

    ")


  (get-one [kvstore {:keys [version dataset table key] :as akey}]
    "Returns the value for a specific key. The key supplied must conform to
     service.kvstore/Key. Returns `nil` if the value isn't found.

        example:

        (get-one kvstore
          {:dataset \"d1\" :table \"t1\" :version \"v1\" :key \"k1\"})
        ;;=> {:foo 1 :bar \"value\"}

    ")


  (get-many [kvstore keys]
    "Returns values for the specified set of keys. The keys argument must be
     a 'vector' of values that conform to service.kvstore/Key.
     It returns a map where each requested key (entries in `keys` vector)
     is a key in the returned map and the value of each key is the value
     associated with that particular key. It a key is not found it returns
     `nil` for that particular key.

        example:

        (get-many kvstore
          [{:dataset \"d1\" :table \"t1\" :version \"v1\" :key \"k1\"}
           {:dataset \"d2\" :table \"t2\" :version \"v2\" :key \"k2\"}])
        ;;=> {;; first entry
              {:dataset \"d1\" :table \"t1\" :version \"v1\" :key \"k1\"}
              {:foo 1 :bar \"value\"}

              ;; second entry
              {:dataset \"d2\" :table \"t2\" :version \"v2\" :key \"k2\"}
              {:foo 2 :bar \"value2\"}}

    ")

  (put-many [kvstore entries]
    "Copies all Key->Value mappings supplied in the entries argument to the
     KV store. Value of entries argument should conform
     to service.kvstore/Entries. It returns an updated kvstore.

        example:

        (put-many kvstore
          {;; first entry
           {:dataset \"d1\" :table \"t1\" :version \"v1\" :key \"k1\"}
           {:foo 1 :bar \"value\"}

           ;; second entry
           {:dataset \"d2\" :table \"t2\" :version \"v2\" :key \"k2\"}
           {:foo 2 :bar \"value2\"}})
        ;;=> kvstore'

    "))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                  ---==| K V   V A L I D A T I O N |==----                  ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; KvValidation - Wraps a key value store (implementation of service.kvstore/KV)
;; and validates request arguments before delegating to the respective functions
;; for the wrapped record. Validations are done using 'Schema' - the functions
;; throw exceptions with request arguments dont meet the requirements.
(defrecord KvValidation [kvstore]
  KV

  (put-one [_ key val]
    (s/validate Key key)
    (s/validate s/Str val)
    (KvValidation. (put-one kvstore key val)))



  (get-one [_ key]
    (s/validate Key key)
    (get-one kvstore key))



  (get-many [_ keys]
    (s/validate [Key] keys)
    (get-many kvstore keys))



  (put-many [_ entries]
    (s/validate Entries entries)
    (KvValidation. (put-many kvstore entries))))



(defn kv-validation
  "Returns KvValidation"
  [kvstore]
  (KvValidation. kvstore))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                     ---==| K V   M E T R I C S |==----                     ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; KVMetrics - Wraps the KV protocol and tracks relevant metrics.
;; Arguments:
;; ----------
;; kvstore - the KV implementation to wrap.
;; metrics-prefix - prefix for the metrics. This can be a string, keyword or
;;                  a vector containing string/keywords.
(defrecord KvMetrics [kvstore metrics-prefix]
  KV

  (put-one [_ key val]
    (track-time
     (metrics-name :kvstore metrics-prefix :put-one)
     (KvMetrics. (put-one kvstore key val) metrics-prefix)))



  (get-one [_ key]
    (let [resp (track-time
                (metrics-name :kvstore metrics-prefix :get-one)
                (get-one kvstore key))
          hits (count resp)]

      ;; Track hits
      (track-count
       (metrics-name :kvstore metrics-prefix :get-one :batch :hit)
       hits)

      ;; Track misses
      (track-count
       (metrics-name :kvstore metrics-prefix :get-one :batch :miss)
       (- 1 hits))

      ;; Return the response
      resp))



  (get-many [_ keys]
    (let [bsize (count keys)
          resp  (track-time
                 (metrics-name :kvstore metrics-prefix :get-many)
                 (get-many kvstore keys))
          hits  (count resp)]


      ;; Track batch size
      (track-distribution
       (metrics-name :kvstore metrics-prefix :get-many :batch :size)
       bsize)

      ;; Track hits
      (track-count
       (metrics-name :kvstore metrics-prefix :get-many :batch :hit)
       hits)

      ;; Track misses
      (track-count
       (metrics-name :kvstore metrics-prefix :get-many :batch :miss)
       (- bsize hits))

      ;; Return the response
      resp))



  (put-many [_ entries]
    ;; Track batch size
    (track-distribution
     (metrics-name :kvstore metrics-prefix :put-many :batch :size)
     (count entries))

    (track-time
     (metrics-name :kvstore metrics-prefix :put-many)
     (KvMetrics. (put-many kvstore entries) metrics-prefix))))



(defn kv-metrics
  "Returns KvMetrics"
  [kvstore metrics-prefix]
  (KvMetrics. kvstore metrics-prefix))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;       ---==| M E T A D A T A   S T O R E   P R O T O C O L S |==----       ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def DatasetName
  "Schema for DatasetName"
  WebSafeNonEmptyString)



(def StringOrKeyword (s/conditional string? s/Str :else s/Keyword))



(def AuditInfo
  "Functions that operate on the meta data store take an optional map with audit
   information like the user who initiated the change etc., Audit information
   must be an optional map containing a string/keyword key and any value."
  (s/maybe {StringOrKeyword s/Any}))



(def Tables
  "Schema for a list of Optimus table names."
  (s/conditional not-empty #{WebSafeNonEmptyString}))



(def EvictionPolicy
  "Eviction policy for the dataset"
  {:type (s/enum "keep-last-x-versions")
   :versions s/Num})



(def Dataset
  "Schema that describes a Dataset. Values returned by get dataset functions
   must conform to this schema."
  {:name DatasetName
   :tables Tables
   :eviction-policy EvictionPolicy
   :content-type  (s/enum "application/json")
   :active-version (s/maybe s/Str)
   :operation-log [AuditInfo]})



(def all-statuses
  "List of all allowed statuses"
  [:preparing
   :awaiting-entries
   :saving
   :saved
   :publishing
   :published
   :discarded
   :failed])



(def Status
  "Schema that describes a list of allowed values for a Version's status."
  (apply s/enum all-statuses))



;; Map of allowed transitions for every possible status of  a 'Version'.
;;
;; Note: This has been intentionally constructed as a map so that it can
;; be extended to support conditional state transitions. For eg.,
;; :saved -> :published is only allowed for datasets which have been published
;; atleast once.
(def allowed-status-transitions
  {:preparing        {:awaiting-entries true :discarded true :failed true}
   :awaiting-entries {:saving true :discarded true :failed true}
   :saving           {:saved true :discarded true :failed true}
   :saved            {:publishing true :published true :discarded true :failed true}
   :publishing       {:published true :discarded true :failed true}
   :published        {:saved true}
   :discarded        {}
   :failed           {}})



(def VerificationPolicy
  "Top level schema to support verification policies. This schema only
  enforces presence of a :strategy key. Validation of the strategy
  name and attributes, for eg: {:strategy \"count\" :count 1000} are
  deferred to the code that handles verification."
  {:strategy s/Str
   s/Any s/Any})



(def Version
  "Schema that describes a 'Version in the optimus. Values returned by
   get version functions must conform to this schema"
  {:id s/Str
   (s/optional-key :label) WebSafeNonEmptyString
   :dataset DatasetName
   :status Status
   (s/optional-key :verification-policy) VerificationPolicy
   :operation-log [AuditInfo]})


(defn status-transition-valid?
  "Returns true if the transition from 'from' status to 'to' status is allowed"
  [from to]
  (get-in allowed-status-transitions [from to]))


(def DATASET-DEFAULTS
  {:eviction-policy {:type "keep-last-x-versions"
                     :versions 10}
   :content-type "application/json"
   :active-version nil})



(def VERSION-DEFAULTS
  {:status :preparing})



(defprotocol MetadataStore
  "Protocol for the metadata store"

  (create-dataset [mstore dataset]
    "Creates a new dataset in the metadata store. The structure
     of dataset should be as per optimus.service.backend/Dataset")

  (get-dataset [mstore dsname]
    "Returns dataset with the specified dataset name")

  (get-all-datasets [mstore]
    "Returns all datasets in the meta data store")

  (create-version [mstore version]
    "Creates a new version for a given dsame with the specified label.
     Structure of the version should comply with optimus.service.
     backend/Version")

  (get-all-versions [mstore]
    "Returns all versions for all datasets in the metadata store.")

  (get-versions [mstore dsname]
    "Returns all versions for a given dataset name.")

  (get-version [mstore version-id]
    "Returns a version with the specified version-id")

  (update-status [mstore version-id target-state audit-info]
    "Updates the status of the specified version to the specified
     'target-state'. The target-state must be a valid status as defined
     in service.kv/Status. This operation will fail (throws an exception)
     if the transition requested is invalid.
     See: service.kv/allowed-status-transitions and
          service.kv/status-transition-allowed")

  (activate-version [mstore version-id]
    "Activates the version by setting the `active-version` attribute of the
     `dataset` to the version-id supplied. Throws an exception if the status of
      the version is not `published`"))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;         ---==| M E T A D A T A   S T O R E   M E T R I C S |==----         ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; MetadataStoreMetrics - Wraps a given MetaDataStore implementaiton and tracks
;; relevant metrics.
;; Arguments:
;; ----------
;; mstore - The MetaDataStore implementation to wrap.
;; metrics-prefix - prefix for the metrics. This can be a string, keyword or
;;                  a vector containing string/keyworkds
(defrecord MetadataStoreMetrics [mstore metrics-prefix]
  MetadataStore

  (create-dataset [_ dataset]
    (track-time
     (metrics-name :meta_store metrics-prefix :create-dataset)
     (-> (create-dataset mstore dataset)
         (MetadataStoreMetrics. metrics-prefix))))



  (get-dataset [_ dsname]
    (track-time
     (metrics-name :meta_store metrics-prefix :get-dataset)
     (get-dataset mstore dsname)))



  (get-all-datasets [_]
    (track-time
     (metrics-name :meta_store metrics-prefix :get-all-datasets)
     (get-all-datasets mstore)))


  (create-version [_ version]
    (track-time
     (metrics-name :meta_store metrics-prefix :create-version)
     (-> (create-version mstore version)
         (MetadataStoreMetrics. metrics-prefix))))



  (get-all-versions [_]
    (track-time
     (metrics-name :meta_store metrics-prefix :get-all-versions)
     (get-all-versions mstore)))


  (get-versions [_ dsname]
    (track-time
     (metrics-name :meta_store metrics-prefix :get-versions)
     (get-versions mstore dsname)))



  (get-version [_ version-id]
    (track-time
     (metrics-name :meta_store metrics-prefix :get-version)
     (get-version mstore version-id)))



  (update-status [_ version-id target-state audit-info]
    (track-time
     (metrics-name :meta_store metrics-prefix :update-status)
     (-> (update-status mstore version-id target-state audit-info)
         (MetadataStoreMetrics. metrics-prefix))))



  (activate-version [_ version-id]
    (track-time
     (metrics-name :meta_store metrics-prefix :activate-version)
     (-> (activate-version mstore version-id)
         (MetadataStoreMetrics. metrics-prefix)))))



(defn meta-data-store-metrics
  "Returns MetadataStoreMetrics"
  [mstore metrics-prefix]
  (MetadataStoreMetrics. mstore metrics-prefix))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                  ---==| D U R A B L E   Q U E U E |==----                  ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:const DEAFULT_LEASE_TIME (* 5 60 1000))


(defprotocol DurableQueue
  "A durable queue protocol for remote systems (presumes side effect)"

  (send-message!
    [this topic message]
    [this topic id message]
    "Append the given message to the queue for the given topic.
     It returns :ok throws an exception when not successful.")

  (reserve-next-message!
    [this topic pid]
    "Atomically it puts a lease on the next message it returns it
     in the form `{:id \"...\" :message \"message\"}`")

  (acknowledge-message! [this id pid]
    "It acknowledge a message once the processing is completed.
     It returns :ok throws an exception when not successful.
     If the message doesn't exists, or the given
     pid doesn't own the lease an exception is thrown.")

  (extend-message-lease! [this id pid]
    "Every reservation has a lease for a limited time.
     Use this to extend your lease on a given item.
     It returns :ok throws an exception when not successful.
     If the message doesn't exists, or the given
     pid doesn't own the lease an exception is thrown.")

  (list-messages! [this filters]
    "Returns a list of messages which match the given filter."))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;          ---==| D U R A B L E   Q U E U E   M E T R I C S |==----          ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DurableQueueMetrics - Wraps an instance of the DurableQueue and tracks
;; relevant events
;; Arguments:
;; ---------
;; queue - The queue to wrap.
;; metrics-prefix - prefix for the metrics tracked.
(defrecord DurableQueueMetrics [queue metrics-prefix]
  DurableQueue



  (send-message!
    [_ topic message]
    (track-time
     (metrics-name :queue metrics-prefix :send-message)
     (send-message! queue topic message)))



  (send-message!
    [_ topic id message]
    (track-time
     (metrics-name :queue metrics-prefix :send-message)
     (send-message! queue topic id message)))



  (reserve-next-message!
    [_ topic pid]
    (track-time
     (metrics-name :queue metrics-prefix :reserve-next-message)
     (reserve-next-message! queue topic pid)))



  (acknowledge-message! [_ id pid]
    (track-time
     (metrics-name :queue metrics-prefix :acknowledge-message)
     (acknowledge-message! queue id pid)))



  (extend-message-lease! [_ id pid]
    (track-time
     (metrics-name :queue metrics-prefix :extend-message-lease)
     (extend-message-lease! queue id pid)))



  (list-messages! [_ filters]
    (track-time
     (metrics-name :queue metrics-prefix :list-messages)
     (list-messages! queue filters))))



(defn queue-metrics
  "Returns DurableQueueMetrics"
  [queue metrics-prefix]
  (DurableQueueMetrics. queue metrics-prefix))
