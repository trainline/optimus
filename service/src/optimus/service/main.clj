;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.main
  "This namespace contains functions required to initialise and run
  Optimus"
  (:gen-class)
  (:require [clojure.tools.logging :as log]
            [optimus.service
             [api :as api]
             [async-task :as a]
             [backend :as b]
             [core :as ms]
             [util :refer [deep-merge read-config service-version]]]
            [optimus.service.backends
             [dynamodb-kv :as dkv]
             [dynamodb-meta-store :as dms]
             [dynamodb-queue :as dq]
             [inmemory :as mem]]
            [optimus.service.backends.middleware.binary-kv-store :as bkv]
            [samsara.trackit :refer :all]
            [taoensso.timbre :as t]
            [taoensso.timbre.tools.logging :as tl]))



(defn vanity-header
  "Returns vanity-header with version"
  []
  (format
   "\n
        ▒█▀▀▀█ ▒█▀▀█ ▀▀█▀▀ ▀█▀ ▒█▀▄▀█ ▒█░▒█ ▒█▀▀▀█
        ▒█░░▒█ ▒█▄▄█ ░▒█░░ ▒█░ ▒█▒█▒█ ▒█░▒█ ░▀▀▀▄▄
        ▒█▄▄▄█ ▒█░░░ ░▒█░░ ▄█▄ ▒█░░▒█ ░▀▄▄▀ ▒█▄▄▄█

  |---------------------|%15s|----------------------------|
  " (service-version)))



(def DEFAULT-CONFIG
  {
   ;; :server - HTTP Server configuration
   :server
   {;; :port - HTTP port to use
    :port 8888
    ;; :auto-reload - reloads the handler every time to make sure that any changes
    ;; done to the code is picked up immediately while developing in the REPL.
    ;; set to true only during development.
    :auto-reload false
    ;; :context-root - the base path at which the api must be
    ;; mounted. For eg.  if the context-root is set to "optimus", the get
    ;; datasets api will be at /optimus/v1/datasets. Context root must
    ;; start with a / and must not end with a /. eg. "/optimus".
    ;; To mount the API at "/" set context-root to empty string. ""
    :context-root ""}

   ;; :aws - AWS configuration
   :aws
   {;; :region - the AWS region to be used.
    :default-use-gzip true
    }

   ;; :async-task - configuration for the async tasks that handles
   ;; events from the 'operations topic' and kicks off the
   ;; corresponding operations.
   :async-task
   {;; :poll-interval The interval in ms in which the async task will
    ;; poll the operations queue for new events/messages.
    :poll-interval 5000
    ;; :handler-fn - the call back function that handles the messages
    ;; from the operations topic. Must be of arity 3 - the arguments
    ;; passed will be [backend, message, extend-lease-fn]
    :handler-fn "optimus.service.async-task/handle-message"
    ;; :operations-topic - name of the topic that handles operation events.
    :operations-topic "versions"}

   ;; configuration for data verification using the :count strategy
   :count-verification
   {;; refer to safely (https://github.com/BrunoBonacci/safely) for more info.
    :retry-config
    [:max-retry 3
     :retry-delay [:random-exp-backoff :base 300 :+/- 0.50 :max 30000]
     :log-errors true
     :message "Error while verifying data. Counting kv pairs failed."]}

   ;; :kv-store - configuration for the data store that stores KV pairs (entries)
   :kv-store
   {;; name of the kv store
    :name "kv-store"
    ;; :type - the KV implementation to use. Default is :in-memory.
    :type :in-memory}

   ;; :kv-store - configuration for the data store that stores KV pairs (entries)
   ;; :kv-store
   ;; {;; :type - the KV implementation to use. Default is :in-memory.
   ;;  :type :dynamodb
   ;;  ;; :region - the AWS region to use
   ;;  ;; :kv-store-table - Name of the dynamodb table to store the KV pairs
   ;;  :kv-store-table "OptimusKV"
   ;;  }

   ;; :meta-data-store - configuration for the metadata store that stores
   ;; information about datasets, versions etc.,
   :meta-data-store
   {;; :name - name of the metadata store
    :name "meta-data-store"
    ;; :type - The metadata store implementation to use. Default is :in-memory
    :type :in-memory}

   ;; :queue - configuration for the operations queue
   :queue
   {;; :name -name of the durable queue
    :name "async-tasks-queue"
    ;; :type - the queue implementation to use.
    :type :in-memory
    ;; :lease-time - the lease time to set for reservations.
    ;; After this time the reserved item, if not acknowledged,
    ;; will be available to grant reservation to other processes.
    :lease-time 60000}

   ;; :queue - configuration for the operations queue
   ;; :queue
   ;; {;; :type - the queue implementation to use.
   ;;  :type :in-memory
   ;;  ;; :region - the AWS region to use
   ;;  ;; :lease-time - the lease time to set for reservations.
   ;;  ;; After this time the reserved item, if not acknowledged,
   ;;  ;; will be available to grant reservation to other processes.
   ;;  :lease-time 60000
   ;;  ;; :task-queue-table the name of the table for the task-queue
   ;;  :task-queue-table         "OptimusTasksQ"
   ;;  ;; :reservation-index - The name of the index used for the reservation
   ;;  ;; queries.
   ;;  :reservation-index        "reservation-index"}

   ;; :metrics
   ;; refer https://github.com/samsara/trackit for more information on
   ;; the config below.
   :metrics
   {;; :type - type of metric reporter to use
    :type  :console
    ;; :reporting-frequency-seconds - how often should metrics be flushed
    :reporting-frequency-seconds 30
    ;; :reporter-name - name for the metric reporter.
    :reporter-name "optimus"
    ;; disable jvm metrics by default.
    :jvm-metrics :none}


   ;; :logging - Logging configuration
   :logging
   {:level :info}})



(defn configure-logger
  [config]
  (when config
    (t/merge-config! config)))



(defn start-api-server
  "Starts Optimus"
  [{:keys [server]} bknd]
  (api/start-server server
                    #(#'api/handler bknd
                                    (:context-root server))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                        ---==| B A C K E N D |==----                        ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti queue
  "Returns a new queue based on the :type specified in the conf.
   conf argument should be a map with 2 keys :type and :config.
   :type - implementation of the queue. eg. :in-memory, :dynamodb
   :config - map that contains necessary config for the implementation."
  (fn [backend] (get-in backend [:config :queue :type])))



;; Returns a new in-memory queue.
(defmethod queue :in-memory [{:keys [config] :as backend}]
  (let [queue (-> (mem/in-memory-queue config)
                  (b/queue-metrics (-> config :queue :name)))]
    (assoc backend :queue queue)))



;; Returns a new DynamoDB queue
(defmethod queue :dynamodb [{:keys [config] :as backend}]
  (let [qconf (->> config :queue (merge dq/DEFAULT-CONFIG))
        queue (->  (dq/dynamo-queue qconf)
                   (b/queue-metrics (:name qconf)))]
    (-> backend
        (assoc :queue queue)
        (assoc-in [:config :queue] qconf))))



(defmulti kv-store
  "Returns a new kv-store based on the :type specified in the conf. conf
   argument should be a map with 2 keys :type and :config.
   :type - implementation of the kv store. eg. :in-memory, :dynamodb
   :config - map that contains necessary config for the implementation."
  (fn [backend] (get-in backend [:config :kv-store :type])))



;; Returns a new mutable kv store
(defmethod kv-store :in-memory [{:keys [config] :as backend}]
  (let [kv-store (-> (mem/in-mem-kv {})
                     b/kv-validation
                     mem/mutable-kv-store
                     (b/kv-metrics (-> config :kv-store :name)))]
    (assoc backend :kv-store kv-store)))



;; Returns a new dynamodb kv store.
(defmethod kv-store :dynamodb [{:keys [config] :as backend}]
  (let [{metrics-name :name :as kvconf}
        (->> config :kv-store (merge dkv/DEFAULT-CONFIG))
        kv-store (-> (dkv/dynamodb-kv kvconf)
                     (bkv/binary-kv-store {:metrics-prefix metrics-name})
                     b/kv-validation
                     (b/kv-metrics metrics-name))]
    (-> backend
        (assoc :kv-store kv-store)
        (assoc-in [:config :kv-store] kvconf))))



(defmulti meta-data-store
  "Returns a new meta-data-store based on the :type specified in the conf.
   conf argument should be a map with 2 keys :type and :config.
   :type - implementation of the metadata store. eg. :in-memory, :dynamodb
   :config - map that contains necessary config for the implementation."
  (fn [backend] (get-in backend [:config :meta-data-store :type])))



;;returns a new in-memory meta data store
(defmethod meta-data-store :in-memory [{:keys [config] :as backend}]
  (as-> (mem/in-mem-meta-data-store {}) $
    (mem/mutable-meta-data-store $)
    (b/meta-data-store-metrics $ (:name (:meta-data-store config)))
    (assoc backend :meta-store $)))



;; Returns a new dynamodb meta store.
(defmethod meta-data-store :dynamodb [{:keys [config] :as backend}]
  (let [{metrics-name :name :as mconf}
        (->> config :meta-data-store (merge dms/DEFAULT-CONFIG))
        meta-store (-> (dms/dynamodb-meta-data-store mconf)
                       (b/meta-data-store-metrics metrics-name))]
    (-> backend
        (assoc :meta-store meta-store)
        (assoc-in [:config :meta-data-store] mconf))))



(defn make-bknd
  "Create optimus backend from the given config. Optimus backend
  contains kv-store, meta-store, a pid and durable queue."
  [config]
  (-> (ms/->BackendStore config)
      (assoc :pid (name (gensym "optimus")))
      kv-store
      meta-data-store
      queue))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                           ---==| M A I N |==----                           ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn start
  [config]
  (configure-logger (:logging config))

  (println (vanity-header))

  (let [backend  (make-bknd config)
        async-processor  (a/start-async-processor backend
                                                  (:async-task config))
        http-server      (start-api-server config backend)
        metrics-reporter (start-reporting! (:metrics config))]

    {:backend          backend
     :async-processor  async-processor
     ;; start metrics
     :metrics-reporter metrics-reporter
     :http-server      http-server}))



(defn stop
  [system]
  (a/stop-async-processor (:async-processor system))
  (api/stop-server (:http-server system))
  (when-let [reporter (:metrics-reporter system)]
    (reporter))
  nil)



(defn -main
  [& [config-file & args]]
  (let [config (deep-merge DEFAULT-CONFIG (read-config config-file))]
    (start config)
    @(promise)))
