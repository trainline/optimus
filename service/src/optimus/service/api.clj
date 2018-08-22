;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.api
  (:require [aleph.http :as aleph]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [compojure.api.sweet :refer :all]
            [optimus.service
             [backend :as b]
             [core :as sut]
             [util :as u :refer [metrics-name]]]
            [ring.swagger.json-schema :as rjs :refer [field]]
            [ring.util.http-response :refer :all]
            [samsara.trackit :refer :all]
            [ring.middleware.gzip :refer [wrap-gzip]]
            [schema.core :as s]))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                     ---==| M I D D L E W A R E |==----                     ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn wrap-app
  [app-fn auto-reload]
  (if auto-reload
    (do
      (log/info "AUTO-RELOAD enabled!!! I hope you are in dev mode.")
      (fn [r] ((app-fn) r)))
    (app-fn)))



(defn uri->metrics-name
  "Converts an URI to a metric name by replacing slash with underscore
  and removing semi-colons. For eg: /versions/:id/publish will be
  transformed to versions_id_publish"
  [uri]
  (-> uri
      (str/replace  #"/" " ")
      (str/trim)
      (str/replace  #" " "_")
      (str/replace  #":" "")))



(defn route->metrics-name
  "Converts the specified compojure route to a metrics-name.
   Compojure Route returned by compojure-api is a vector containing
  the http method and the route. This function returns a metric-name
  of the format [route].[method]"
  [route]
  (->> [(second route) (first route)]
       (str/join ".")
       uri->metrics-name))



(defn status->metrics-name
  "Converts a http status code to a metrics-name. eg. 200 and 201 will
  return 2xx etc"
  [status]
  (-> (quot status 100) (str "xx")))



(defn wrap-metrics
  "This middleware tracks the following metrics.

   __prefix__.http.request.[method].[api-name].[count,1min,median]
   __prefix__.http.response.[method].[api-name].[2xx..5xx].[count,1min, median]"
  [handler]
  (fn [{:keys [context :compojure/route] :as request}]

    (let [context-lbl (uri->metrics-name context)
          route-lbl   (route->metrics-name route)

          {:keys [status] :as resp}
          (track-time
           (metrics-name :http :request context-lbl route-lbl)
           (handler request))]

      (track-rate
       (metrics-name :http :response context-lbl route-lbl
                     (status->metrics-name status)))

      resp)))



(defn wrap-ring-metrics
  "This middleware is aimed at tracking metrics at the ring level.

   __prefix__.http.request.[count,1min,median]
   __prefix__.http.response.[2xx..5xx].[count,1min, median]"
  [handler]
  (fn [request]
    (let [{:keys [status] :as resp}
          (track-time
           (metrics-name :http :request :ring)
           (handler request))]

      (track-rate
       (metrics-name :http :response :ring
                     (status->metrics-name status)))

      resp)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;               ---==| C O M M O N   R E S P O N S E S |==----               ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def _Message
  "Schema for a common message to be used in Acknowledgement or Error response
   Not to be used directly. See AckResponse or ErrorResponse."
  {:status (field (s/enum :ok :error)
                  {:description "Status of the operation."})
   :message (field s/Str {:description "Description of the
                    status. When status is error this field will
                    contain an error message."})
   s/Any s/Any})



(s/defschema AckResponse
  "Schema for responses that are acknowledgements. Generally used to
   acknowledge create or accepted requests"
  (field
   _Message
   {:description "Acknowledgement message. Generally
                  used to acknowledge created or accepted
                  requests."
    :example {:status "ok" :message "version created" :id "1asf542d"}}))



(s/defschema ErrorResponse
  "Schema describing error responses"
  (field
   _Message
   {:description
    "Error message. In some cases, this
   message may contain additional information or context corresponding
   to the error."
    :example {:status "error"
              :message "invalid version"
              :version {:id "132453sdf"
                        :label "version1_1_0"
                        :dataset "recommendations"
                        :status :saved}}}))



(defn error-msg
  "returns an error response"
  ([msg & more]
   (->> (apply hash-map more)
        (conj {:status :error :message msg}))))



(defn ack-msg
  "returns an acknowledgement response"
  [msg & more]
  (->> (apply hash-map more)
       (conj {:status :ok :message msg})))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;              ---==| E X C E P T I O N   H A N D L E R |==----              ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn exception-handler
  "Handles exceptions thrown by the core layer.Exceptions from core
   layer are the ones whose :ex-type attribute is set to :core-exception.

   Returns http responses based on :type attribute in ex-data as listed below:
   :validation-error => 400 (Bad request)
   :missing-entity   => 404 (Not Found)
   Returns 500 for everything else."
  [^Exception e data request]

  (log/error e (str "Error processing request: ")) ;;TODO: restore request.

  (let [msg (error-msg (.getMessage e))
        {:keys [ex-type type] :as edata} (ex-data e)
        error (conj msg (dissoc edata :type :ex-type))]
    (if (= ex-type :core-exception)
      (cond
        (= type :validation-error)  (bad-request error)
        (= type :missing-entity)    (not-found error)
        (= type :conflict)          (conflict error)
        (= type :too-many-requests) (too-many-requests error)
        :else (internal-server-error error))
      (internal-server-error error))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                    ---==| D A T A S E T   A P I |==----                    ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(s/defschema WebSafeNonEmptyString
  (field u/WebSafeNonEmptyString {:description "Web safe non-empty
         string.Allowed characters are: [A-Za-z0-9-_.]"}))



(s/defschema Table
  (field WebSafeNonEmptyString
         {:description "Name of the table"}))



(s/defschema Tables
  "Schema for a list of Optimus table names."
  (field (s/conditional not-empty #{Table})
         {:description "List of unique table names that belong to the dataset."
          :example #{"items" "users"}}))



(s/defschema EvictionPolicy
  "Schema for Eviction policy for the dataset"
  (field {:type (field (s/enum "keep-last-x-versions")
                       {:description "Eviction policy for the dataset"
                        :example "keep-last-x-versions"})
          :versions (field s/Num
                           {:description "Number of versions to retain. Required
                            type is set to keep-last-x-versions"
                            :example 10})}
         {:description "Eviction policy for the dataset"}))



(s/defschema CreateDatasetRequest
  "Schema for create dataset request"
  (field
   {:name (field WebSafeNonEmptyString {:description "Name of the dataset"
                                        :example "recommendations"})
    :tables Tables
    (s/optional-key :content-type) (field
                                    (s/enum "application/json")
                                    {:description
                                     "Content Type which will be used
                                     to return individual keys."
                                     :example "application/json"})
    (s/optional-key :eviction-policy) EvictionPolicy}
   {:description "Create Dataset Request"}))



(s/defschema DatasetName
  "Schema for dataset name"
  (field WebSafeNonEmptyString {:description "Name of the dataset"
                                :example "recommendations"}))



(s/defschema Dataset
  "Schema for Dataset response"
  (field
   {:name DatasetName
    :tables Tables
    (s/optional-key :content-type) (field
                                    (s/enum "application/json")
                                    {:description
                                     "Content Type which will be used
                                     to return individual keys."
                                     :example "application/json"})
    :active-version (field (s/maybe s/Str) {:description "The version that is
    currently active for this dataset"})
    (s/optional-key :eviction-policy) EvictionPolicy
    (s/optional-key :operation-log) (field [{s/Any s/Any}]
                                           {:description "History of
                                           changes to the dataset"}) }
   {:description "Create Dataset Request"}))




(s/defschema Datasets
  "Schema for responses that return a list of datasets"
  (field [Dataset] {:description "List of datasets"}))


(defn- handle-create-dataset
  "Handler for create dataset request"
  [backend {:keys [name] :as dataset}]
  (if (sut/get-dataset backend name)
    (bad-request (error-msg "dataset already exists"))
    (do  (sut/create-dataset backend dataset)
         (created (str "/datasets/" name)
                  (ack-msg "Dataset created successfully"
                           :dataset name)))))



(defn- handle-get-datasets
  "Handler to get all datasets"
  [backend]
  (ok (sut/get-datasets backend)))



(defn- handle-get-dataset
  "Handler to get a dataset by name"
  [backend dataset-name]
  (let [dataset (sut/get-dataset backend dataset-name)]
    (if dataset
      (ok dataset)
      (not-found (error-msg "dataset not found")))))



(defn dataset-handlers
  "Handlers for Dataset API"
  [backend]
  (routes
   ;; Datasets
   (POST "/datasets" []
         :return AckResponse
         :middleware [wrap-metrics]
         :responses {201 {:schema AckResponse
                          :description "Dataset created successfully"}
                     400 {:schema ErrorResponse
                          :description "Bad request"}
                     500 {:schema ErrorResponse
                          :description "Internal Server error"}}
         :body [dataset CreateDatasetRequest]
         :tags ["datasets"]
         :summary "Creates a dataset"
         (handle-create-dataset backend dataset))


   (GET "/datasets" []
        :return Datasets
        :middleware [wrap-metrics]
        :responses {200 {:schema Datasets
                         :description "List of all datasets
                                  in optimus"}
                    500 {:schema ErrorResponse
                         :description "Internal Server Error"}}
        :tags ["datasets"]
        :summary "Get all datasets"
        (handle-get-datasets backend))


   (GET "/datasets/:name" [name]
        :return Dataset
        :middleware [wrap-metrics]
        :path-params [name :- String]
        :responses {200 {:schema Dataset
                         :description "Dataset for the specified key"}
                    404 {:schema ErrorResponse
                         :description "Dataset not found"}
                    500 {:schema ErrorResponse
                         :description "Internal Server Error"}}
        :tags ["datasets"]
        :summary "Get datasets by dataset name"
        (handle-get-dataset backend name))))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                   ---==| V E R S I O N S   A P I |==----                   ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def VerificationPolicy (field b/VerificationPolicy
                               {:description "The verification policy
    to be applied when a version is saved. When a verification policy
    is supplied, the version is marked as SAVED only if the data
    passes the verification check. If the verification check fails,
    the version is marked as FAILED."
                                :example {:strategy "count" :count 156787}}))


(s/defschema Label
  "Schema for a version label"
  (field WebSafeNonEmptyString
         {:description "A label that describes the version"
          :example     "v1-2017-01-02"}))



(s/defschema CreateVersionRequest
  "Schema for create version request"
  (field
   {:dataset DatasetName
    (s/optional-key :label) Label
    (s/optional-key :verification-policy) VerificationPolicy}
   {:description "Create Version Request"}))



(s/defschema DiscardVersionRequest
  "Schema for discard version request"
  (field {(s/optional-key :reason)
          (field (s/maybe s/Str)
                 {:description "Optional reason explaining why
   the version is being discarded."
                  :example "Some valid reason here."})}
         {:description "Discard version request"}))



(s/defschema Version
  "Schema for Version"
  (field
   {:id (field WebSafeNonEmptyString
               {:description "Unique identifier for the version"})
    (s/optional-key :label) Label
    :dataset DatasetName
    :status (field b/Status
                   {:description "Current status of the version."})

    (s/optional-key :verification-policy)
    VerificationPolicy

    (s/optional-key :operation-log)
    (field [{s/Any s/Any}]
           {:description "History of changes to the
           version"})} {:description "Version"}))



(defn- handle-create-version
  "Handler to create a new version"
  [backend version]
  (if-not (sut/get-dataset backend (:dataset version))
    (not-found (error-msg "dataset does not exist"))
    (let [{:keys [id]} (sut/create-version backend version)]
      (created (str "/versions/" id)
               (ack-msg "Version created successfully" :id id)))))



(defn- handle-get-versions
  "Handler for get versions for dataset"
  [backend dataset]
  (if-not (sut/get-dataset backend dataset)
    (not-found (error-msg "dataset does not exist"))
    (ok (sut/get-versions backend dataset))))



(defn- handle-get-version
  "Handler for get versions for dataset"
  [backend version-id]
  (let [version (sut/get-version backend version-id)]
    (if-not version
      (not-found (error-msg "version does not exist"))
      (ok version))))



(defn- handle-save-version
  "Handler for save version API"
  [backend id]
  (let [result (sut/save-version backend id)]
    (if (= :ok result)
      (accepted (ack-msg "save in progress" :id id))
      (internal-server-error
       (error-msg "save version failed"
                  :reason (str "save version returned:" result))))))



(defn- handle-publish-version
  "Handler for publish version API"
  [backend id]
  (let [result (sut/publish-version backend id)]
    (if (= :ok result)
      (accepted (ack-msg "publish in progress" :id id))
      (internal-server-error
       (error-msg "publish version failed"
                  :reason (str "publish version returned:" result))))))



(defn- handle-discard-version
  "Handler for discard version API"
  [backend id reason]
  (sut/discard-version backend id reason)
  (ok (ack-msg "version discarded successfully")))



(defn version-handlers
  "Handlers for Version API"
  [backend]
  (routes
   ;; Versions
   (POST "/versions" []
         :return AckResponse
         :middleware [wrap-metrics]
         :responses {201 {:schema AckResponse
                          :description "Version created successfully"}
                     400 {:schema ErrorResponse
                          :description "Bad request"}
                     500 {:schema ErrorResponse
                          :description "Internal Server error"}}
         :body [version CreateVersionRequest]
         :tags ["versions"]
         :summary "Creates a new version for a specified dataset"
         (handle-create-version backend version))



   (GET "/versions" []
        :return [Version]
        :middleware [wrap-metrics]
        :query-params [dataset :- String]
        :responses {200 {:schema [Version]
                         :description "List of all versions
                                  in optimus"}
                    500 {:schema ErrorResponse
                         :description "Internal Server Error"}}
        :tags ["versions"]
        :summary "Get all versions for a dataset"
        (handle-get-versions backend dataset))



   (GET "/versions/:id" [id]
        :return Version
        :middleware [wrap-metrics]
        :path-params [id :- String]
        :responses {200 {:schema Version
                         :description "Version for the specified id"}
                    404 {:schema ErrorResponse
                         :description "Version not found"}
                    500 {:schema ErrorResponse
                         :description "Internal Server Error"}}
        :tags ["versions"]
        :summary "Get version by version id"
        (handle-get-version backend id))



   (POST "/versions/:id/save" [id]
         :return AckResponse
         :middleware [wrap-metrics]
         :path-params [id :- String]
         :responses {202 {:schema AckResponse
                          :description "Save request accepted."}
                     400 {:schema ErrorResponse
                          :description "Bad request"}
                     500 {:schema ErrorResponse
                          :description "Internal Server error"}}
         :tags ["versions"]
         :summary "Notify Optimus to 'SAVE' the
         version"
         :description "Optimus will update the status of
         version to 'saving' and kick off the operations required
         before setting the status to 'saved'. On successful execution
         of this API, Optimus will stop accepting new
         data uploads for this version."
         (handle-save-version backend id))



   (POST "/versions/:id/publish" [id]
         :return AckResponse
         :middleware [wrap-metrics]
         :path-params [id :- String]
         :responses {202 {:schema AckResponse
                          :description "Publish request accepted."}
                     400 {:schema ErrorResponse
                          :description "Bad request"}
                     500 {:schema ErrorResponse
                          :description "Internal Server error"}}
         :tags ["versions"]
         :summary "Notify Optimus to 'PUBLISH' the
         version"
         :description "Optimus will update the status of
         version to 'publishing' and kick off the operations required
         before setting the status to 'published'."
         (handle-publish-version backend id))



   (POST "/versions/:id/discard" [id]
         :return AckResponse
         :middleware [wrap-metrics]
         :path-params [id :- String]
         :responses {200 {:schema AckResponse
                          :description "Version discarded successfully."}
                     400 {:schema ErrorResponse
                          :description "Bad request"}
                     500 {:schema ErrorResponse
                          :description "Internal Server error"}}
         :tags ["versions"]
         :body [{reason :reason :or {reason nil}} (s/maybe DiscardVersionRequest)]
         :summary "Discard the version"
         :description "Sets the status of the verison specified to
         discarded. No futher actions can be performed on this
         version."
         (handle-discard-version backend id reason))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                    ---==| E N T R I E S   A P I |==----                    ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(s/defschema TableKeyValue
  "Schema for Table key value pair"
  (field
   {:table (field Table
                  {:description "Name of the table to
   which the entry belongs."
                   :example "items"})
    :key (field WebSafeNonEmptyString
                {:description "The key part of the key-value pair."
                 :example "item1"})
    :value (field s/Any
                  {:description "The value part of the key-value pair."
                   :example "value1"})}
   {:description "A key value pair entry that contains a table name,
   key and value."}))



(s/defschema KeyValue
  "Schema for a typical key-value pair in optimus"
  (field
   {:key (field WebSafeNonEmptyString
                {:description "The key part of the key-value pair."
                 :example "key1"})
    :value (field s/Any
                  {:description "The value part of the key-value pair."
                   :example "value1"})}

   {:description "A key value pair entry that contains a key and value."}))



(s/defschema CreateEntriesForDatasetRequest
  (field
   [TableKeyValue]
   {:description "Create entries for dataset request."}))



(s/defschema CreateEntriesForTableRequest
  "Schema for Create Entries Request"
  (field
   [KeyValue]
   {:description "Create entries for table request."}))



(s/defschema GetEntriesForTableRequest
  (field (s/constrained
          [{:key (field WebSafeNonEmptyString
                        {:description "Key of the entry to request"})}]
          not-empty)
         {:description "List of keys to request."}))


(def GetEntriesMetadata
  "Metadata for GetEntries requests"
  {:status (field (s/enum :ok :error)
                  {:description "Status of the request"
                   :example "ok"})
   :keys-found (field s/Num
                      {:description
                       "Number of keys from the request whose values were found."
                       :example 4})
   :keys-missing (field s/Num
                        {:description
                         "Number of keys from the request whose values were not found."
                         :example 0})})



(s/defschema GetEntriesForTableResponse
  (field (assoc GetEntriesMetadata :data
                {(field WebSafeNonEmptyString {:description "Key" :example "key"})
                 (field s/Any {:description "Value" :example "value"})})
         {:description "Get Entries response"}))



(defn- handle-create-entries
  "Handler for create entries API."
  ([backend version-id dataset entries]
   (sut/create-entries backend version-id dataset entries)
   (ok (ack-msg "Data loaded successfully")))

  ([backend version-id dataset table entries]
   (sut/create-entries backend version-id dataset table entries)
   (ok (ack-msg "Data loaded successfully")))

  ([backend version-id dataset table key value]
   (sut/create-entries backend version-id dataset table key value)
   (ok (ack-msg "Data loaded successfully"))))



(defn- handle-get-entry
  "Handler for get entry API. Returns a response that contains the
   value of the key and 2 headers:
   x-active-version-id - The active version for the requested dataset.
   x-version-id        - The version of the data in the response.
   Returns 404 when the key requested is not found."
  [backend version-id dataset table key]
  (let [{:keys [data active-version-id version-id]}
        (sut/get-entry backend version-id dataset table key)
        response (assoc {:key key} :value data)]

    (-> (if data (ok response) (not-found (error-msg "key does not exist")))
        (header :x-active-version-id active-version-id)
        (header :x-version-id version-id))))



(defn remove-nil-vals
  "Remove nil values from map"
  [m]
  (reduce-kv
   (fn [m key value]
     (if (nil? value)
       (dissoc m key)
       m)) m  m))



(defn- handle-get-entries
  "Handler for get entries API.Returns a response that contains the
   value of the key and 2 headers:
   x-active-version-id - The active version for the requested dataset.
   x-version-id        - The version of the data in the response.
   Returns 200 even if the some or all of the keys requested are missing,
   but includes metadata about the count of keys missing in the response body."
  [backend version-id dataset table keys]
  (let [{:keys [active-version-id version-id data]
         } (sut/get-entries backend version-id dataset table keys)

        entries    (remove-nil-vals data)
        diff-count (- (count keys) (count entries))]

    (-> (ok {:status :ok
             :keys-found   (count entries)
             :keys-missing (if (pos? diff-count) diff-count 0)
             :data (->> entries
                        ;;The structure of data in the kvstore is
                        ;; {{:dataset :table :version :key} value}.
                        ;; Transform this to {:key value}.
                        (mapcat #(reduce (fn [k v] [(:key k)  v]) %))
                        (apply hash-map))})
        (header :x-active-version-id active-version-id)
        (header :x-version-id version-id))))




(defn entry-handlers
  "Handlers for entries API."
  [backend]
  (routes
   ;;
   (POST "/datasets/:dataset" [dataset]
         :return AckResponse
         :middleware [wrap-metrics]
         :query-params [version-id :- String]
         :responses {201 {:schema AckResponse
                          :description "Data loaded successfully"}
                     400 {:schema ErrorResponse
                          :description "Bad request"}
                     500 {:schema ErrorResponse
                          :description "Internal Server error"}}
         :body [entries CreateEntriesForDatasetRequest]
         :tags ["entries"]
         :summary "Load data for a specified dataset"
         (handle-create-entries backend version-id dataset entries))



   (POST "/datasets/:dataset/tables/:table" [dataset table]
         :return AckResponse
         :middleware [wrap-metrics]
         :query-params [version-id :- String]
         :responses {201 {:schema AckResponse
                          :description "Data loaded successfully"}
                     400 {:schema ErrorResponse
                          :description "Bad request"}
                     500 {:schema ErrorResponse
                          :description "Internal Server error"}}
         :body [entries CreateEntriesForTableRequest]
         :tags ["entries"]
         :summary "Load data for a specified table within a dataset"
         (handle-create-entries backend version-id dataset table entries))



   (GET "/datasets/:dataset/tables/:table/entries/:key"
        [dataset table key]
        :return KeyValue
        :middleware [wrap-metrics]
        :query-params [{version-id :- String nil}]
        :responses {200 {:schema KeyValue
                         :description "Value for the specified
                    dataset/version/table/key combination."}
                    404 {:schema ErrorResponse
                         :description "Version not found"}
                    500 {:schema ErrorResponse
                         :description "Internal Server Error"}}
        :tags ["entries"]
        :summary "Get value for the given dataset version table key combination"
        (handle-get-entry backend version-id dataset table key))



   (GET "/datasets/:dataset/tables/:table/entries"
        [dataset table]
        :return GetEntriesForTableResponse
        :middleware [wrap-metrics]
        :query-params [{version-id :- String nil}]
        :body [key-set GetEntriesForTableRequest]
        :responses {200 {:schema GetEntriesForTableResponse
                         :description "Entries for the specified keys in the
                    dataset/version/table combination."}
                    404 {:schema ErrorResponse
                         :description "Version not found"}
                    500 {:schema ErrorResponse
                         :description "Internal Server Error"}}
        :tags ["entries"]
        :summary "Get entries for the specified keys in the dataset
        version table combination"
        (handle-get-entries backend version-id dataset table key-set))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                            ---==| A P I |==----                            ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn handler
  "HTTP Handler"
  ([backend]
   (handler backend ""))

  ([backend base-path]
   (api
    {:exceptions
     {:handlers {:validation-error exception-handler
                 :compojure.api.exception/default exception-handler}}

     :swagger
     {:ui   (str base-path "/api-docs")
      :spec (str base-path "/swagger.json")
      :data
      {:info
       {:title "Optimus API"
        :version (u/service-version)
        :description
        "Optimus API is key-value store which allows the user
        to save and retrieve reference data and model's coefficients
        which are calculated across millions of users or items. The
        API resembles a common key/value store, however the main
        difference with other REST based key/value store is that it
        provides transactional semantics, which means that while an
        update of a new set of keys is in progress no user will be
        able to observe the new value until all keys have been
        successfully stored and committed"}
       :tags
       [{:name "datasets", :description "APIs that operate on datasets"}
        {:name "versions", :description "APIs that operate on versions"}
        {:name "entries",  :description
         "APIs to load/retrieve data for a given dataset and version"}]}}}

    ;; Redirect context root to swagger documentation.
    (GET (if (empty? base-path) "/" base-path) []
         :no-doc true
         (found (str base-path "/api-docs")))

    ;;
    ;; Version 1
    ;;
    (context base-path []
             (context "/v1" []
                      :tags ["Version 1"]
                      ;; Datasets
                      (dataset-handlers backend)
                      ;; Versions
                      (version-handlers backend)
                      ;; Entries
                      (entry-handlers backend)))
    ;;
    ;; Health check
    ;;
    (GET "/healthcheck" []
         :summary "Returns 200 ok if the service is running"
         :return {:status String :message String}
         {:status 200 :body {:status "OK" :message "The service is running normally."}})
    ;;
    ;; everything else is NOT FOUND
    ;;
    (undocumented
     (fn [_]
       {:status 404 :body {:status "ERROR" :message "Resource Not found"}})))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                    ---==| H T T P   S E R V E R |==----                    ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn start-server
  ([config handler-fn]
   (-> handler-fn
       (wrap-app (:auto-reload config))
       (wrap-gzip) ;;Must always be here. TODO: Remove when aleph
       ;;0.4.4 is released.
       (wrap-ring-metrics)
       (aleph.http/start-server config))))



(defn stop-server
  [server]
  (when server (.close server)))
