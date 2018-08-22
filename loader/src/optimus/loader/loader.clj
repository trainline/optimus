;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.loader.loader
  "This namespace contains functions for the spark job that load datas
  into optimus."
  (:require [optimus.loader.api :as client]
            [safely.core :refer [safely]]
            [clojure.string :as str]
            [cheshire.core :refer [generate-string parse-string]]
            [cheshire.generate :refer [add-encoder]]
            [sparkling
             [conf :as conf]
             [core :as spark]
             [destructuring :as sde]]
            [clojure.tools.logging :as log]))

;;TODO: move to a better place
;; Add a cheshire encoder to encode regex pattern.
;; This is used in generating the report file.
(add-encoder java.util.regex.Pattern
             (fn [c jsonGenerator]
               (.writeString jsonGenerator (.toString c))))



(defn spark-config
  "Creates an instance of spark config. Sets master only if the value
  of :local is true."
  [{:keys [local app-name] :as args}]
  (let [conf (conf/spark-conf)]
    ;;When run within the cluster, the value if master MUST not be
    ;;set in the config. This is set by the spark-submit script.
    (-> (if local (conf/master conf "local[*]") conf)
        (conf/app-name app-name))))



(defn- parse
  "Parse the given value into the supplied content-type"
  [value content-type]
  (condp = content-type
    :int   (Integer/parseInt value)
    :float (Float/parseFloat value)
    :json  (parse-string value)
    value))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                     ---==| L O A D E R   J O B |==----                     ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn load-kv-file
  "Loads KV pairs from the file specified into the specified
  version/dataset/table using the Optimus API."
  [{:keys [num-partitions api-base-url batch-size separator
           max-retries dataset]}
   spark-context version table file content-type]

  (log/info "-> loading :" file
            " with input delimiter:" separator
            " into table:" table
            " with content-type: " content-type)

  (->> (spark/text-file spark-context file)

       (spark/repartition num-partitions)
       ;; Split each line into key and value and convert to a spark
       ;;tuple.
       (spark/map-to-pair #(apply spark/tuple (str/split % separator 2)))

       ;; Transform each kv pair {:table "" :key "" :value ""} format.
       (spark/map
        (sde/key-value-fn (fn [k v] (zipmap [:table :key :value]
                                           [table k (parse v content-type)]))))

       ;; Load data for each partition
       (spark/foreach-partition
        (fn [entries]
          ;; batch data by 'batch-size' and call the Optimus API.
          (doseq [batch (partition-all batch-size entries)]
            (client/load-entries
             api-base-url content-type dataset version batch max-retries))))))



(defn load-files
  "Loads each file in the input into its corresponding table. Returns
  {:status :ok} if successful. Spark would fail the loader when there
  are any errors."
  [spark-context version-id {:keys [dataset tables] :as args}]

  (doseq [{:keys [table file content-type]} tables]
    (load-kv-file args spark-context version-id table file content-type))

  (log/info "Load completed successfully.")

  {:status :ok})



(defn poll-version
  [version-id target-status {:keys [api-base-url max-retries]}]
  (log/info "Polling version " version-id " until its status is set to: "
            target-status)

  (safely
   (when-not (= target-status
                (:status (client/get-version api-base-url version-id)))
     (throw
      (ex-info (str "Target status not reached."))))

   :on-error
   :max-retry max-retries
   :retry-delay [:random-exp-backoff :base 300 :+/- 0.50 :max 30000]
   :log-errors false
   :message (str "poll-version: version status not " target-status)))



(defn save-version
  "Calls the save version API. Returns the response from the publish
  version API when successful and a :failed status with a reason
  otherwise."
  [version-id {:keys [api-base-url max-retries] :as args}]
  (try

    (log/info "Attempting to save version :" version-id)
    (client/save-version api-base-url version-id max-retries)

    (catch Throwable t
      (log/error t "Save failed, but load was successful.")
      {:status :failed :reason (.getMessage t)})))



(defn publish-version
  "Calls the publish version API. Returns the response from the
  publish version API when successful and a :failed status with a
  reason otherwise."
  [version-id {:keys [api-base-url max-retries] :as args}]
  (try
    (log/info "Attempting to publish version :" version-id)
    (poll-version version-id :saved args)
    (client/publish-version api-base-url version-id max-retries)

    (catch Throwable t
      (log/error t "Publish failed, but load and save were successful.")
      {:status :failed :reason (.getMessage t)})))



(defn save-output
  [content file spark-context]
  (->> content
       (spark/into-rdd spark-context 1)
       (spark/save-as-text-file file)))



(defn start-loader
  "Creates the spark context and starts the loader job."
  [{:keys [api-base-url version dataset label input out max-retries] :as args}]
  (log/info "starting loader...")
  (let [;; create spark context.
        spark-conf    (spark-config args)
        spark-context (spark/spark-context spark-conf)

        ;; create version-id if not supplied by the user.
        version-id    (or version
                         (client/create-version api-base-url
                                                dataset label))]

    (log/info "Starting load with version-id : " version-id)
    (log/info "created spark context with config: " (.toDebugString spark-conf))

    ;; Intentionally skipping validation of version status when
    ;; version-id is supplied through command line args. When the
    ;; spark job fails the user will learn to provide correct
    ;; input. ;-D If a new version was created by the loader, wait
    ;; until its status changes to awaiting entries.

    (when-not version
      (poll-version version-id :awaiting-entries args))

    ;; Load files, save version, write report ....
    (cond-> {:input-arguments args  :version-id version-id}
      ;; Load files
      :always
      (assoc :load-files
             (load-files spark-context version-id args))

      ;; save version
      (:save-version args)  (assoc :save-version
                                   (save-version version-id args))

      ;; publish version
      (:publish-version args)  (assoc :publish-version
                                      (publish-version version-id args))

      ;; transform report to json
      :always
      ((comp list generate-string))

      ;; write report
      out
      (save-output (str out "/" version-id ".json")
                   spark-context))))
