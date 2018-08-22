;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.loader.main
  (:require [clojure.tools.cli :as cli]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [taoensso.timbre :as timbre]
            [optimus.loader.loader :refer [start-loader]])
  (:gen-class))

;; Log level for startup.
(timbre/set-level! :info)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                         ---==| C O N F I G |==----                         ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def DEFAULT-CONFIG
  {
   :api-base-url     nil
   ;; parallelism factor for the spark job. Spark RDD for this job
   ;; will be partitioned to the value set for this key.
   :num-partitions   10
   ;; batch size for the put entries API call.
   :batch-size       100
   ;; maximum number of times the call to put-entries must be retried
   ;; before failing the job.
   :max-retries      10
   ;; Location of the output file.
   :out              nil
   ;; If true, spark master is set to "local[*], otherwise its assumed
   ;; that the loader is being run using spark-submit script and the
   ;; master will be set from the env var.
   :local            true
   ;; App name for the spark job.
   :app-name         "optimus-loader"
   ;; Default separator for the input files
   :separator  #"\t"
   ;; logging config
   :logging          {:level :info}
   })


(def ^:const CONTENT-TYPES #{:json :string :int :float})


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                          ---==| U T I L S |==----                          ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn read-config
  "Reads the config from a file in EDN format"
  [config-file]
  (when config-file
    (edn/read-string (slurp config-file))))



(defn service-version
  "Reads the version of the running service"
  []
  (try
    (->> "optimus.version" io/resource slurp str/trim)
    (catch Exception x "development")))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                  ---==| V A N I T Y   H E A D E R |==----                  ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def vanity-header-optimus "

          ▒█▀▀▀█ ▒█▀▀█ ▀▀█▀▀ ▀█▀ ▒█▀▄▀█ ▒█░▒█ ▒█▀▀▀█
          ▒█░░▒█ ▒█▄▄█ ░▒█░░ ▒█░ ▒█▒█▒█ ▒█░▒█ ░▀▀▀▄▄
          ▒█▄▄▄█ ▒█░░░ ░▒█░░ ▄█▄ ▒█░░▒█ ░▀▄▄▀ ▒█▄▄▄█

  \n")



(def vanity-header-loader "
      ___                         ___
     (   )                       (   )
      | |    .--.     .---.    .-.| |    .--.    ___ .-.
      | |   /    \\   / .-, \\  /   \\ |   /    \\  (   )   \\
      | |  |  .-. ; (__) ; | |  .-. |  |  .-. ;  | ' .-. ;
      | |  | |  | |   .'`  | | |  | |  |  | | |  |  / (___)
      | |  | |  | |  / .'| | | |  | |  |  |/  |  | |
      | |  | |  | | | /  | | | |  | |  |  ' _.'  | |
      | |  | '  | | ; |  ; | | '  | |  |  .'.-.  | |
      | |  '  `-' / ' `-'  | ' `-'  /  '  `-' /  | |
     (___)  `.__.'  `.__.'_.  `.__,'    `.__.'  (___)

|-----------------------|%15s|--------------------------------|")



(defn vanity-header
  "Returns vanity header for the loader, formatted with the service
  version."
  []
  (->> (format vanity-header-loader (service-version))
       (str vanity-header-optimus "\n")))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                            ---==| C L I |==----                            ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def cli-opts
  "Command line opts for the loader."
  [ ;; Dataset name
   ["-d" "--dataset DATASET-NAME" "Dataset Name"
    :missing "[-d] dataset is required"]


   ;; Version ID
   ["-v" "--version VERSION-ID" "Version ID. Creates a new version if not supplied."]


   ;; Version Label
   ["-l" "--label LABEL" "The label to be used for the version."]


   ;; API URL
   ["-u" "--api-base-url URL" "Base URL for Optimus API"
    :missing "[-u] URL is required"
    :validate [io/as-url "Must be a valid URL"]]


   ;; Save Version
   [nil "--save-version"
    "Save version on successful load."
    :default false
    :default-desc "Default: false"]


   ;; Publish Version
   [nil "--publish-version"
    "Publish version on successful load. Returns validation error if
    --save-version is set to false"
    :default false
    :default-desc "Default: false"]


   ;; Number of partitions
   ["-n" "--num-partitions NUM-PARTITIONS" "Number of Spark RDD partitions."
    :default-desc "Default : 10"
    :default-desc ""
    :parse-fn #(Integer/parseInt %)]


   ["-t" "--table TABLE" "Table name"
    :missing   "[-t] atleast one table must be specified"
    :id        :tables
    :assoc-fn  (fn [m k v]
                 ;;Validate the last table file pair.
                 (if-let [last-item (last (get m k))]
                   (when-not (:file last-item)
                     (throw (ex-info (str "[-t] no file specified for table:"
                                          (:table last-item))
                                     {}))))

                 (-> m
                     (#(if-not (get % k) (assoc m k []) m))
                     (update-in [k] conj {:table v
                                          :file nil
                                          :content-type :string})))]


   ["-f" "--file  FILE-NAME" "File name"
    :missing   "[-t] aleast one table and file must be specified"
    :id        :tables
    :assoc-fn (fn [m k v]
                (let [last-item (last (get m k))
                      cnt   (count (get m k))]
                  (cond
                    ;; check for table
                    (empty? last-item)
                    (throw (ex-info (str "[-f] no table supplied for file:"
                                         (:file last-item)) {}))

                    ;; check for file
                    (not (empty? (:file last-item)))
                    (throw (ex-info (str "[-f] two files specified for same table:"
                                         (:table last-item)) {}))

                    :else
                    (assoc-in m [k (dec cnt) :file] v))))]


   ["-c" "--content-type  CONTENT-TYPE" "Content Type for the file specified."
    :id        :tables
    :parse-fn  keyword
    :assoc-fn (fn [m k v]
                (let [last-item (last (get m k))
                      cnt   (count (get m k))]
                  (cond
                    ;; check for file and table
                    (empty? last-item)
                    (throw
                     (ex-info
                      (str "[-c] content type specified without table and file:"
                           v) {}))

                    ;; check if both table and file are specified
                    (empty? (:file last-item))
                    (throw
                     (ex-info
                      (str "[-c] no file specified for table"
                           (:table last-item)) {}))

                    :else
                    (assoc-in m [k (dec cnt) :content-type] v))))

    :validate [#(get CONTENT-TYPES % nil)
               (str "[-c] content-type must be one of: "
                    (str/join "," (map name CONTENT-TYPES)))]]


   ;; Batch size
   ["-b" "--batch-size BATCH-SIZE" "Batch size for put-entries API call."
    :default 1000
    :default-desc "[Default: 1000]"
    :parse-fn #(Integer/parseInt %)]

   ;; Max Retries
   ["-r" "--max-retries MAX-RETRIES" "Max num of times to retry the load entries API."
    :default 10
    :default-desc "[Default: 10]"
    :parse-fn #(Integer/parseInt %)]

   ;; Out file
   ["-o" "--out REPORT-FILE" "Location of the report file."]

   ;; Local
   [nil "--local" "Set spark master to local[*]."
    :default false]


   ;; Delimiter for input files
   ["-s" "--separator SEPARATOR" "Field separatorfor the input files."
    :default #"\t"
    :default-desc "[Default: \\t]"
    :parse-fn re-pattern]


   ;; config file
   [nil "--config CONFIG-FILE" "Additional config file"]


   ["-h" "--help" "Print help and usage."]])



(defn usage [options-summary]

  (->> [\newline
        "Optimus - Loader"
        "--------------------------"
        "The 'Optimus - Loader' is a spark job that loads kv"
        "pairs from delimited input files in the request to the"
        "corresponding dataset, version and table of the Model Data"
        "Store using the Optimus API."
        ""
        "Usage:"
        ""
        "Using spark-submit:"
        ""
        "spark-submit --deploy-mode cluster --master yarn loader.jar         \\"
        "             --dataset <DATASET-NAME> --api-base-url <API-URL>      \\"
        "             [--label <VERSION-LABEL> ] [--version <VERSION-ID>]    \\"
        "             --table <TABLE> --file <FILE> [--content-type <TYPE>]  \\"
        "             [--table <TABLE> --file <FILE> [--content-type <TYPE>]]\\"
        "             [OPTIONS]"
        ""
        "As a standalone client:"
        ""
        "./loader --local"
        "         --dataset <DATASET-NAME> --api-base-url <API-URL>      \\"
        "         [--label <VERSION-LABEL> ] [--version <VERSION-ID>]    \\"
        "         --table <TABLE> --file <FILE> [--content-type <TYPE>]  \\"
        "         [--table <TABLE> --file <FILE> [--content-type <TYPE>]]\\"
        "         [OPTIONS]"
        ""
        "Options:"
        ""
        options-summary
        ""]
       (str/join \newline)))



(defn error-msg [errors summary]
  (->> [summary
        "The following error occurred while parsing your command: "
        ""
        ""
        (str/join \newline errors)]
       (str/join \newline)))


(defn- parse-opts
  "Parse cli opts and perform any additional validaitons required."
  [args cli-opts]
  (try
    (let [{:keys [options arguments errors summary] :as r}
          (cli/parse-opts args cli-opts)]

      (cond-> r
        ;; Check for table file combination for one last time
        (and (:tables options) (-> options :tables last :file not))
        (update-in [:errors] conj
                   (str  "[-f] no file specified for table:"
                         (-> options :tables last :table)))

        (and (:publish-version options) (false? (:save-version options)))
        (update-in [:errors] conj
                   "[--publish-version] --save-version must be true.")

        ;; check if config supplied exists
        (when-let [c (:config options)] (not (.exists (io/as-file c))))
        (update-in [:errors] conj "[-c] config file not found")))

    (catch Exception e
      {:errors [(.getMessage e)]})))



(defn -main
  [& args]
  (log/info (vanity-header))

  (let [{:keys [options arguments errors summary] :as r}
        (parse-opts args cli-opts)
        summary            (usage summary)]

    ;; Handle help and error conditions
    (cond
      (:help options) (log/info summary)

      (empty? args)   (log/info summary)

      errors          (log/info (error-msg errors summary))

      :else
      ;; Merge the configs. The order of precedence in order of
      ;; increasing precedence: cli -> config file -> default
      (let [config (merge DEFAULT-CONFIG (read-config (:config options)) options)]

        ;; Configure logging
        (when-let [logging (:logging config)]
          (timbre/merge-config! logging))

        ;; Start Loader
        (start-loader config)

        (log/info "Loader finished successfully.")))))
