;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.backends.inmemory
  (:require [optimus.service
             [backend :refer :all]
             [util :refer [NonEmptyString now rand-id]]]
            [schema.core :as s]
            [where.core :refer [where]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;             ---==| I N   M E M O R Y   K V   S T O R E |==----             ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; In-Memory implementation of a Key Value Store (service.kvstore/KV).
(defrecord InMemKV [data]
  KV

  (put-one [_ {:keys [version dataset table key] :as akey} val]
    (InMemKV. (assoc-in data [version dataset table key] val)))



  (get-one [kvstore {:keys [version dataset table key] :as akey}]
    (get-in data [version dataset table key]))



  (get-many [kvstore keys]
    (->> keys
         (map (juxt identity (partial get-one kvstore)))
         (into {})))



  (put-many [kvstore entries]
    (InMemKV.
     (reduce (fn [data [{:keys [version dataset table key] :as akey} value]]
          (assoc-in data [version dataset table key] value))
        data
        entries))))



(defn in-mem-kv
  "Returns InMemKV"
  [data]
  (InMemKV. data))



;; Wraps an atom that contains an InMemMetadataStore.
;; This will then act as a mutable meta data store.
(defrecord MutableKVStore [data]
  KV

  (put-one [this key val]
    (swap! data put-one key val)
    this)



  (get-one [kvstore key]
    (get-one @data key))



  (get-many [kvstore keys]
    (get-many @data keys))



  (put-many [this entries]
    (swap! data put-many entries)
    this))



(defn mutable-kv-store
  "Returns a new MutableKVStore"
  [kv-store]
  (MutableKVStore. (atom kv-store)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;       ---==| I N   M E M O R Y   M E T A D A T A   S T O R E |==----       ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- append-timestamp
  "Add timestamp to  audit-info"
  [audit-info]
  (assoc audit-info :timestamp (System/currentTimeMillis)))



;; InMemMetadatastore - In-Memory implementation of MetaDataStore.
(defrecord InMemMetadataStore [data]
  MetadataStore

  ;; Creates a dataset if the arguments supplied are valid. Throws an exception
  ;; if a dataset already exists with the given dataset name. Takes an optional
  ;; map of audit information, adds a timestamp and persists it with the Dataset
  (create-dataset [mstore dataset]
    (let [audit-info  (-> (get dataset :audit-info {})
                          (assoc :action :created)
                          (append-timestamp)
                          vector)
          enriched_ds (-> (merge DATASET-DEFAULTS dataset)
                          (dissoc :audit-info)
                          (assoc  :operation-log audit-info))
          dsname (:name dataset)]

      (s/validate Dataset enriched_ds)

      (if (nil? (get-in data [:datasets dsname]))
        (-> data
            (assoc-in [:datasets dsname] enriched_ds)
            (InMemMetadataStore.))
        (throw (ex-info "Dataset already exists" {})))))



  ;; Returns a single dataset with the supplied dataset name. Returns nil if no
  ;; dataset exists with the supplied dataset name. Value returned conforms to
  ;; service.kv/Dataset
  (get-dataset [mstore dsname]
    (get-in mstore [:data :datasets dsname]))



  ;; Returns all Datasets in the metadata store. Returns a list of values that
  ;; conform to service.kv/Dataset
  (get-all-datasets [mstore]
    (-> (get-in mstore [:data :datasets]) vals))



  ;; Creates a new version for the specified dataset with the specified lable
  ;; and id if the arguments supplied are valid. Takes an optional map of audit
  ;; information, adds a timestamp and action='created' and persists it with the
  ;; Dataset. Throws an excpetion if no dataset exists with the specified
  ;; dataset name. Defaults status of the Version to :preparing as per the
  ;; contract.
  (create-version [mstore {:keys [audit-info id dataset] :as version}]
    (let [audit-info  (->  (merge audit-info {})
                           (append-timestamp)
                           (assoc :action :created)
                           vector)
          enriched_ver (-> (merge VERSION-DEFAULTS version)
                           (dissoc :audit-info)
                           (assoc  :operation-log audit-info))]

      (s/validate Version enriched_ver)

      (if (nil? (get-dataset mstore dataset))
        (throw (ex-info "Dataset not found" {}))
        (-> data
            (assoc-in [:versions id] enriched_ver)
            (InMemMetadataStore.)))))



  ;; Returns all versions for all datasets. Returns a list of values that
  ;; conform to service.kv/Version
  (get-all-versions [mstore]
    (-> (get-in mstore [:data :versions]) vals))



  ;; Returns all versions for a given dataset name. Returns a list of values
  ;; that conform to service.kv/Version
  (get-versions [mstore dsname]
    (s/validate DatasetName dsname)

    (->> (get-all-versions mstore)
         (filter #(= (:dataset %) dsname))
         seq))



  ;; Returns the version with the specified version-id. The value returned
  ;; conforms to service.kv/Version
  (get-version [mstore version-id]
    (s/validate NonEmptyString version-id)

    (get-in mstore [:data :versions version-id]))



  ;; Updates status of the Version if the arguments supplied are valid. Throws
  ;; an exception if:
  ;; 1. Arguments supplied do not conform to their schemas.
  ;; 2. version-id specified doesnt exist.
  ;; 3. A transition requested is invalid.
  ;;    See: service.kv/allowed-status-transitions
  ;;         and service.kv/status-transition-allowed
  (update-status [mstore version-id target-status audit-info]
    (s/validate NonEmptyString version-id)
    (s/validate Status target-status)
    (s/validate AuditInfo audit-info)

    (let [version (get-version mstore version-id)]
      (if (status-transition-valid? (:status version) target-status)
        (-> data
            (update-in [:versions version-id] assoc :status target-status)
            (update-in [:versions version-id :operation-log] conj
                       (-> (append-timestamp audit-info)
                           (assoc :action (str
                                           (:status version) " -> " target-status))))
            (InMemMetadataStore.))
        (throw (ex-info "Invalid state transition"
                        {:current-state version})))))


  ;; Activates the version by setting version.dataset.active-version to the
  ;; specified version-id. Throws an exception if the current status of the
  ;; version is not in the `published` state.
  (activate-version [mstore version-id]
    (s/validate NonEmptyString version-id)

    (let [{:keys [status dataset] :as version} (get-version mstore version-id)]
      (if (= :published status)
        (-> data
            (assoc-in [:datasets dataset :active-version] version-id)
            (InMemMetadataStore.))
        (throw (ex-info "Cannot activate a version that is not published"
                        {:current-state version}))))))



(defn in-mem-meta-data-store
  "Returns a new InMemMetadataStore"
  [data]
  (InMemMetadataStore. data))


;; Wraps an atom that contains an InMemMetadataStore.
;; This will then act as a mutable meta data store.
(defrecord MutableInMemMetadataStore [data]
  MetadataStore

  (create-dataset [this dataset]
    (swap! data create-dataset dataset)
    this)

  (get-dataset [this dsname]
    (get-dataset @data dsname))

  (get-all-datasets [this]
    (get-all-datasets @data))

  (create-version [this version]
    (swap! data create-version version)
    this)

  (get-all-versions [this]
    (get-all-versions @data))

  (get-versions [this dsname]
    (get-versions @data dsname))

  (get-version [this version-id]
    (get-version @data version-id))

  (update-status [this version-id target-state audit-info]
    (swap! data update-status version-id target-state audit-info)
    this)

  (activate-version [this version-id]
    (swap! data activate-version version-id)))



(defn mutable-meta-data-store
  "Returns a new InMemMetadataStore"
  [meta-store]
  (MutableInMemMetadataStore. (atom meta-store)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;; ---==| I N   M E M O R Y   Q U E U E   I M P L E M E N T A T I O N |==---- ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn- find-first-processable-item
  "return [position item] or nil."
  [items topic lease-time]
  (->> items
       (map-indexed vector)
       (filter (where (comp :topic second) = topic))
       (filter #(and (not (-> % second :ack?))
                   (or (-> % second :pid nil?)
                      (-> % second :lease (< lease-time)))))
       (first)))



(defn- find-message-by-id
  "return [position item] or nil."
  [items id]
  (->> items
       (map-indexed vector)
       (filter #(-> % second :id (= id)))
       (first)))



(defrecord InMemQueue
    [config data-atom]

  DurableQueue

  ;; Append the given message to the queue.
  ;; It returns :ok throws an exception when not successful.
  (send-message!
    [this topic message]
    (send-message! this topic (rand-id) message))


  (send-message!
    [_ topic id message]
    (swap! data-atom update :items (fnil conj [])
           {:id id :topic topic :message message})
    :ok)


  ;; Atomically it puts a lease on the next message it returns it
  ;; in the form `{:id "" :topic "" :message "message"}`
  (reserve-next-message!
    [this topic pid]
    (let [data @data-atom
          [pos item] (find-first-processable-item (:items data) topic (now))
          lease-time (or (:lease-time config) DEAFULT_LEASE_TIME)]
      (when-not pos
        (throw (ex-info "No messages found." {:error :no-message-found})))
      (let [data' (update-in data [:items pos] assoc :pid pid :lease (+ (now) lease-time))]
        ;; exception when no messages are available?
        (if (compare-and-set! data-atom data data')
          (get-in data' [:items pos])
          (reserve-next-message! this topic pid)))))


  ;; It acknowledge a message once the processing is completed.
  ;; It returns :ok throws an exception when not successful.
  ;; If the message doesn't exists, or the given
  ;; pid doesn't own the lease an exception is thrown.
  (acknowledge-message!
    [this id pid]
    (let [data @data-atom
          [pos msg] (find-message-by-id (:items data) id)]


      (when (nil? pos)
        (throw (ex-info "Message not found." {:error :no-message-found :id id})))

      (when (:ack? msg)
        :ok)  ;; message already acked

      (when (not= pid (:pid msg))
        (throw (ex-info "Cannot acknowledge a message for which you don't own the lease."
                        {:error :wrong-owner :msg-id id :your-pid pid :msg-pid (:pid msg) :msg-lease (:lease msg)})))

      (when (and (= pid (:pid msg)) (> (now) (:lease msg)))
        (throw (ex-info "Cannot acknowledge a message for which you don't own the lease. Lease expired."
                        {:error :lease-expired :msg-id id :your-pid pid :msg-pid (:pid msg) :msg-lease (:lease msg)})))

      (let [data' (assoc-in data [:items pos :ack?] true)]
        (if (compare-and-set! data-atom data data')
          :ok
          (acknowledge-message! this pid id)))))


  ;; Every reservation has a lease for a limited time.
  ;; Use this to extend your lease on a given item.
  ;; It returns :ok throws an exception when not successful.
  ;; If the message doesn't exists, or the given
  ;; pid doesn't own the lease an exception is thrown.
  (extend-message-lease!
    [this id pid]
    (let [data @data-atom
          [pos msg] (find-message-by-id (:items data) id)
          lease-time (or (:lease-time config) DEAFULT_LEASE_TIME)]

      (when (nil? pos)
        (throw (ex-info "Message not found." {:error :no-message-found :msg-id id })))

      (when (:ack? msg)
        (throw (ex-info "Message already acknowledged." {:error :already-acknowledged :message msg})))

      (when (not= pid (:pid msg))
        (throw (ex-info "Cannot extend the lease for a message for which you don't own."
                        {:error :wrong-owner :msg-id id :your-pid pid :msg-pid (:pid msg) :msg-lease (:lease msg)})))

      (when (and (= pid (:pid msg)) (> (now) (:lease msg)))
        (throw (ex-info "Cannot extend the lease for a message for which you don't own. Lease expired."
                        {:error :lease-expired :msg-id id :your-pid pid :msg-pid (:pid msg) :msg-lease (:lease msg)})))

      (let [data' (update-in data [:items pos :lease] (fn [ol] (max ol (+ (now) lease-time))))]
        (if (compare-and-set! data-atom data data')
          :ok
          (extend-message-lease! this pid id)))))


  ;; Returns a list of messages which match the given filter.
  (list-messages!
    [_ {:keys [status size page pid topic] :as filters
        :or {status :all size 100 :page 1}}]

    ;; the topic is required
    (when-not topic
      (throw (ex-info "Missing required filter `topic`." filters)))

    (let [topic-filter  (where :topic = topic)
          pid-filter    (if pid (where :pid = pid) identity)
          status-filter (cond
                          (= :all          status) identity
                          (= :new          status) (where [:ack? not= true])
                          (= :acknowledged status) (where :ack? = true)
                          (= :reserved     status) (where [:and [:ack? not= true] [:lease not= nil] [:lease >= (now)]])
                          (= :expired      status) (where [:and [:ack? not= true] [:lease not= nil] [:lease < (now)]]))]
      (->> (:items @data-atom)
           (filter topic-filter)
           (filter pid-filter)
           (filter status-filter)
           (map #(select-keys % [:id :timestamp :topic :message :lease :pid]))))))



(defn in-memory-queue
  ([]
   (in-memory-queue {:lease-time DEAFULT_LEASE_TIME} {}))
  ([config]
   (in-memory-queue config {}))
  ([config data]
   (InMemQueue. config (atom data))))
