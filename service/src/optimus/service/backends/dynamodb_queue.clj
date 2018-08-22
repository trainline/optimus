;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.backends.dynamodb-queue
  (:require [amazonica.aws.dynamodbv2 :as dyn]
            [optimus.service.backends.dynamo-util :refer [aws-config]]
            [optimus.service
             [backend :refer :all]
             [util :refer [now] :as u]]
            [taoensso.timbre :as log]))

;; TODO: main init tables
;; TODO: search pagination

;; required in order to configure the logger and avoid flood of logs from AWS
(log/set-level! :info)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;           ---==| D Y N A M O   D U R A B L E   Q U E U E |==----           ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; This is a implementation of a durable queueing system based on DynamoDB.
;; The `DurableQueue` protocol was designed to support asynchronous tasks.
;; The assumption is the queue is a place where to put tasks which will be
;; completed in the background while the API return control to the user.
;; For the nature of the target use case these are the basic assumptions
;; and characteritics which are required to be supported:
;;
;;   * FIFO (but not strictly)
;;   * Low volume - maybe 10-20 items per week
;;   * Concurrent - should support access by multiple nodes
;;   * Track completed tasks for operational reference.
;;
;; We want to use DynamoDB because it is the primary storage for the
;; rest of the system and we didn't want to introduce a additional
;; service like SQS or Kinesis for such low usage.  Should assumptions
;; change, it might be required to review this implementation or
;; provide alternative implementation.
;;

;;
;; # The Dynamo table
;;
;; The following list shows each protocol function along with the
;; access pattern:
;;
;;    - send-message!         -> n/a
;;    - reserve-next-message! -> topic, status, timestamp and lease
;;    - acknowledge-message!  -> id
;;    - extend-message-lease! -> id
;;    - list-messages!        -> topic, status, timestamp (opt: lease, pid)
;;
;; We have selected `id` to be our partition key, for optimal and
;; uniform spread, and easy access. While `reserve-next-message!`
;; requires the `topic`, a `timestamp` ordered sequence of
;; items/messages and the current `status` to determine whether an
;; item is new, already reserved or acknowledged.  In order to support
;; a efficient search/selection of the item to process we use a
;; global-secondary-index in dynamo with `topic` as partition key, and
;; a synthetic (compound) key as sort key. The synthetic key is called
;; `__reserv_key` and it is composed of:
;;
;;    - N flag: 0/1 whether the item is new.
;;    - R flag: 0/1 whether the item has been reserved.
;;    - A flag: 0/1 whether the item has been acknowledged.
;;    - timestamp: the zero-padded `timestamp` of the issue, for ordering
;;
;; The first three flags are mutually exclusive, so only one
;; can be set at any point in time.
;; Here a sample representation of the `reservation-index`.
;;
;;     vvv<     20 digits      >
;;     NRA      timestamp
;;     0010000000001484675535808       - acknowledged item
;;     0010000000001484675535809       - acknowledged item
;;     0010000000001484675535810       - acknowledged item
;;     0100000000001484675535811       - reserved item
;;     1000000000001484675535812       - new item
;;     1000000000001484675743590       - new item
;;     1000000000001484675743593       - new item
;;
;;
;; Now it is easy to see how we can efficiently slice the list of
;; items to process. If we want to fine the next item to reserve we
;; can look into the reserved items (for expired leases) and new items
;; skipping all the acknowledged items by filtering the prefix for
;; things which start with `010` or `100`.
;;
;; One thing to keep in mind is that reserved items could bear an
;; expired lease and therefore be open to reservation again.
;; So the query must check the individual item's `:lease`.
;;
;;
;; ## Concurrency
;;
;; Since DynamoDB doesn't support transactions, all updates are
;; performed using Multiversion Concurrency Control (MVCC) techniques
;; and Dynamo's conditional update features.
;;
;; Every item contains a synthetic attribute called `__ver` which is
;; monotonically incremented on every write. Every write, optimistically
;; reads a previous version from the storage, computes a modification
;; and attempts to write it back checking that the current value of
;; `__ver` in the datastore hasn't changed in the meanwhile.
;; If the value is the same the new write is performed (with a new `__ver`)
;; and the transaction is successful. If the value was changed by another
;; concurrent write, then the test fail and the write is aborted.
;; The function which handle all the writes and ensures this logic is preserved
;; is `safe-put-item`. It includes also a special case for the first write
;; which ensures that no other object with the same `id` is already existing.
;;
;; END



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                 ---==| D E F A U L T   C O N F I G |==----                 ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(def ^:const DEFAULT-CONFIG
  {;; :region - the aws region's endpoint to contact

   ;; the name of the table for the task-queue
   :task-queue-table         "OptimusTasksQ"

   ;; the initial read and write capacity
   ;; for the table and indices
   :read-capacity-units      10
   :write-capacity-units     10

   ;; The name of the index used for the reservation
   ;; queries.
   :reservation-index        "reservation-index"

   ;; The default reservation lease time in millis.
   ;; After this time the reserved item, if not acknowledged,
   ;; will be available to grant reservation to other processes.
   :lease-time               DEAFULT_LEASE_TIME})



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                  ---==| C R E A T E   T A B L E S |==----                  ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn create-tables
  "Creates the tables for the DurableQueue and all necessary indexes"
  [config]
  (let [{:keys [region task-queue-table
                reservation-index history-index
                read-capacity-units write-capacity-units]
         } (merge DEFAULT-CONFIG config)]
    (dyn/create-table
     (aws-config config)
     ;;
     ;; main task queue
     ;;
     {:table-name task-queue-table
      :key-schema
      [{:attribute-name "id"           :key-type "HASH"}]
      :attribute-definitions
      [{:attribute-name "id"           :attribute-type "S"}
       {:attribute-name "topic"        :attribute-type "S"}
       {:attribute-name "__reserv_key" :attribute-type "S"}]

      :provisioned-throughput
      {:read-capacity-units read-capacity-units
       :write-capacity-units write-capacity-units}
      ;; indexes
      :global-secondary-indexes
      ;; reservation-index
      [{:index-name reservation-index
        :key-schema
        [{:attribute-name "topic"  :key-type "HASH"}
         {:attribute-name "__reserv_key"  :key-type "RANGE"}]
        :projection {:projection-type "ALL"}
        :provisioned-throughput
        {:read-capacity-units read-capacity-units
         :write-capacity-units write-capacity-units}}]})))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;             ---==| I N T E R N A L   F U N C T I O N S |==----             ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;
;; `find-next-processable-items` queries the reservation-index for
;; items which are new or reserved but with a expired lease.  It uses
;; the `__reserv_key` to jump to the appropriate position in the
;; index.  From that point it starts scanning the index and filter the
;; items until it finds a item which is new or expired.  The scanning
;; has a cost and we do scan only for the first 10 elements.
;; The consequence is that if a new item is in position 11 it will be
;; ignored until one of the first 10 is acknowledged or the lease expires.
;; This limitation isn't so bad as we do expect to have very low volumes,
;; and because of the expiration the system will eventually move on.
;; Another positive effect of this behaviour is that in case of exceptions
;; during the processing only the first 10 elements would be tried.
;; if all of them ran into exceptions, they will appear reserved
;; and blocked for the duration of the lease-time and eventually expired
;; and retried. Without this behaviour if the processing raises
;; and exception and there are 100s of items waiting in the queue,
;; all of them would be reserved.
;;
(defn- find-next-processable-items
  "It queries the reservation-index to find the next available item to process.
   The reservation index only contains item which haven't"
  ([{:keys [region task-queue-table reservation-index] :as config} topic timestamp]
   (first (find-next-processable-items config topic timestamp 10)))
  ([{:keys [region task-queue-table reservation-index] :as config} topic timestamp limit]
   (:items
    (dyn/query (aws-config config)
               {:table-name task-queue-table
                :index-name reservation-index
                :select "ALL_ATTRIBUTES"
                :key-condition-expression
                "topic = :topic AND #reserv > :reserved"
                :expression-attribute-values
                {":topic" topic, ":reserved" "002", ":timestamp" timestamp}
                :expression-attribute-names {"#reserv" "__reserv_key"}
                :filter-expression "lease < :timestamp"
                :limit limit}))))



(defn- list-messages
  "It returns a list of messages or items which match the given filter"
  [{:keys [region task-queue-table
           reservation-index history-index] :as config}
   {:keys [status size page pid id topic] :as filters
    :or {status :all size 100 :page 1}}]

  ;; the topic is required
  (when-not topic
    (throw (ex-info "Missing required filter `topic`." filters)))

  (let [ ;; POST-filtering based on pid
        id-filter
        (when id
          {:id {:comparison-operator "IN"
                :attribute-value-list id}})

        pid-filter
        (when pid
          {:pid {:comparison-operator "EQ"
                 :attribute-value-list [pid]}})

        ;; POST-filtering of reservations which expired
        expired-filter
        (when (= :expired status)
          {:lease {:comparison-operator "LT"
                   :attribute-value-list [(now)]}
           :pid {:comparison-operator "NOT_NULL"}})

        ;; POST-filtering of currently reserved items
        reserved-filter
        (when (= :reserved status)
          {:lease {:comparison-operator "GT"
                   :attribute-value-list [(now)]}})

        ;; RANGE-KEY-filtering based on status
        status-filter
        (case status
          :acknowledged        {:__reserv_key
                                {:comparison-operator "BEGINS_WITH"
                                 :attribute-value-list ["001"]}}
          (:reserved :expired) {:__reserv_key
                                {:comparison-operator "BEGINS_WITH"
                                 :attribute-value-list ["010"]}}
          :new                 {:__reserv_key
                                {:comparison-operator "BEGINS_WITH"
                                 :attribute-value-list ["100"]}}
          nil)

        ;; merging POST filters
        query-filter
        (when (or reserved-filter expired-filter pid-filter)
          {:query-filter (merge reserved-filter
                                expired-filter
                                pid-filter
                                id-filter)})

        query (merge
               {:table-name task-queue-table
                :index-name reservation-index
                :select "ALL_ATTRIBUTES"
                :key-conditions
                (merge status-filter
                       {:topic {:attribute-value-list [topic]
                                :comparison-operator "EQ"}})}
               query-filter)]
    (:items
     (dyn/query (aws-config config) query))))



(defn- inject-derived-attributes
  "Ensure that synthetic attributes are always present with the correct value"
  [{:keys [timestamp ack? lease pid ver] :as msg}]
  (let [status-flags (fn [{:keys [ack? pid]}] (cond ack? 1, pid 10, :else 100))]
    (as-> msg $

      ;; ver is used for Multiversion Concurrency Control and optimistic lock
      (update $ :__ver (fnil inc 0))

      ;; ensure that lease is always populated
      (update $ :lease (fnil identity 0))

      ;; __reserv_key is used for adding item which can be reserved to
      ;; the appropriate index (reservation-index).
      (assoc $ :__reserv_key (format "%03d%020d" (status-flags $) timestamp)))))



(defn- safe-put-item
  "This function uses MVCC to ensure that updates are not conflicting
   with other concurrent updates, causing data to be overwritten."
  {:style/indent 1}
  [{:keys [region task-queue-table] :as config} {:keys [__ver] :as item}]
  (let [updated-item (inject-derived-attributes item)]
    ;; checking whether it is a first-time insert or update
    (if-not __ver
      ;; if it is a first time insert then no item with the same id should
      ;; already exists
      (dyn/put-item
       (aws-config config)
       :table-name task-queue-table
       :item updated-item
       :condition-expression "attribute_not_exists(id)")
      ;; if it is performing an update then the condition is that
      ;; __ver must be unchanged in the db (no concurrent updates)
      (dyn/put-item
       (aws-config config)
       :table-name task-queue-table
       :item updated-item
       :condition-expression "#curver = :oldver"
       :expression-attribute-names {"#curver" "__ver"}
       :expression-attribute-values {":oldver" __ver}))
    updated-item))



(defn- get-message-by-id
  "returns the message given the id"
  [{:keys [region task-queue-table] :as config} id]
  (:item
   (dyn/get-item (aws-config config)
                 {:table-name task-queue-table
                  :key {:id {:s id}}})))



(defrecord DynamoDurableQueue
    [config]

  DurableQueue


  ;; Append the given message to the queue.
  ;; It returns :ok throws an exception when not successful.
  (send-message!
    [_ topic id message]
    (safe-put-item config
      {:id id
       :timestamp (now)
       :topic topic
       :message (u/to-json message)})
    :ok)

  (send-message!
    [this topic message]
    (send-message! this topic (u/rand-id) message))

  ;; Atomically it puts a lease on the next message it returns it
  ;; in the form `{:id "" :topic "" :message "message"}`
  (reserve-next-message!
    [this topic pid]
    (let [msg (find-next-processable-items config topic (now))
          lease-time (or (:lease-time config) DEAFULT_LEASE_TIME)]

      (when-not msg
        (throw (ex-info "No messages found." {:error :no-message-found})))

      (-> (safe-put-item config
            (assoc msg :pid pid :lease (+ (now) lease-time)))
          (update :message u/from-json))))


  ;; It acknowledge a message once the processing is completed.
  ;; It returns :ok throws an exception when not successful.
  ;; If the message doesn't exists, or the given
  ;; pid doesn't own the lease an exception is thrown.
  (acknowledge-message!
    [this id pid]
    (let [msg (get-message-by-id config id)]

      (when-not msg
        (throw (ex-info "Message not found." {:error :no-message-found :id id})))

      (when (:ack? msg)
        :ok)  ;; message already acked

      (when (not= pid (:pid msg))
        (throw (ex-info "Cannot acknowledge a message for which you don't own the lease."
                        {:error :wrong-owner :msg-id id :your-pid pid
                         :msg-pid (:pid msg) :msg-lease (:lease msg)})))

      (when (and (= pid (:pid msg)) (> (now) (:lease msg)))
        (throw (ex-info "Cannot acknowledge a message for which you don't own the lease. Lease expired."
                        {:error :lease-expired :msg-id id :your-pid pid
                         :msg-pid (:pid msg) :msg-lease (:lease msg)})))

      ;; acknowledging the message
      (safe-put-item config (assoc msg :ack? true))
      :ok))


  ;; Every reservation has a lease for a limited time.
  ;; Use this to extend your lease on a given item.
  ;; It returns :ok throws an exception when not successful.
  ;; If the message doesn't exists, or the given
  ;; pid doesn't own the lease an exception is thrown.
  (extend-message-lease!
    [this id pid]
    (let [msg (get-message-by-id config id)
          lease-time (or (:lease-time config) DEAFULT_LEASE_TIME)]

      (when-not msg
        (throw (ex-info "Message not found."
                        {:error :no-message-found :msg-id id })))

      (when (:ack? msg)
        (throw (ex-info "Message already acknowledged."
                        {:error :already-acknowledged :message msg})))

      (when (not= pid (:pid msg))
        (throw (ex-info "Cannot extend the lease for a message for which you don't own."
                        {:error :wrong-owner :msg-id id :your-pid pid
                         :msg-pid (:pid msg) :msg-lease (:lease msg)})))

      (when (and (= pid (:pid msg)) (> (now) (:lease msg)))
        (throw (ex-info "Cannot extend the lease for a message for which you don't own. Lease expired."
                        {:error :lease-expired :msg-id id :your-pid pid
                         :msg-pid (:pid msg) :msg-lease (:lease msg)})))

      ;; extending the lease expiration time
      (safe-put-item config
        (update msg :lease (fn [ol] (max ol (+ (now) lease-time)))))
      :ok))


  ;; Returns a list of messages which match the given filter.
  (list-messages!
    [_ {:keys [status size page pid id topic] :as filters
        :or {status :all :size 100 :page 1}}]
    (->> (list-messages config filters)
         (map #(update % :message u/from-json))
         (map #(select-keys % [:id :timestamp :topic :message :lease :pid])))))



(defn dynamo-queue
  ([]
   (dynamo-queue DEFAULT-CONFIG))
  ([config]
   (DynamoDurableQueue. config)))
