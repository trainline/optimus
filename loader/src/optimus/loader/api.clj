;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.loader.api
  "This namespace contains functions that call the optimus
  api."
  (:require [clojure.tools.logging :as log]
            ;;[aleph.http :as http]
            [clj-http.client :as http]
            [taoensso.timbre :as timbre]
            [cheshire.core :refer [generate-string parse-string]]
            [safely.core :refer [safely]]
            [optimus.loader.api :as client]))



(def DEFAULT-POST-ARGS
  {:content-type :transit+json
   :accept       :json
   :as           :json})


(defn- retryable-error?
  "Returns whether the HTTP request is retryable based on the value of
  HTTP status in the HTTP response."
  [ex]
  ;; Choosing to retry on all errors except 400 for simplicity.
  (not= 400 (:status (ex-data ex))))



(defn post
  "Does a HTTP POST to the given URL. Catches any exception, logs
  the error message and throws the exception back."
  ([url] (post url {}))
  ([url args]
   (try
     (->> args
          (merge DEFAULT-POST-ARGS)
          (http/post url)
          :body)

     (catch Throwable t
       (->>  (ex-data t) :body
             (#(if % % (.getMessage t)))
             ;;TODO: must be warn sometimes, error sometimes, check this.
             (log/warn t "POST to " url " failed with error :"))
       (throw t)))))



(defn load-entries
  "Stores the KV pairs (entries) supplied in Optimus by calling the PUT
  /datasets/dataset/tables API."
  [base-url content-type dataset version entries max-retries]
  (let [url (str base-url "/datasets/" dataset)]
    (safely
     (post url
           {:query-params {:version-id version}
            :form-params  entries})

     :on-error
     :max-retry max-retries
     :retry-delay [:random-exp-backoff :base 300 :+/- 0.50 :max 30000]
     :retryable-error? retryable-error?
     :log-errors true
     :message "put-entries: retrying due to error")))



(defn create-version
  "Creates a new version for the dataset with the given label.
   Returns the version-id"
  [base-url dataset label]
  (let [url (str base-url "/versions")]
    (-> (post url
              {:form-params
               (cond-> {:dataset dataset}
                 label (assoc :label label))})
        :id)))



(defn get-version
  "Retrieves the version with the given vesion-id"
  [base-url version-id]
  (let [url (str base-url "/versions/" version-id)]
    (->  (http/get url {:as :json})
         :body
         (update :status keyword))))



(defn save-version
  "Calls the save version api. Returns the body of the response."
  [base-url version-id max-retries]
  (let [url (str base-url "/versions/" version-id "/save")]
    (safely
     (post url)

     :on-error
     :max-retry max-retries
     :retry-delay [:random-exp-backoff :base 300 :+/- 0.50 :max 30000]
     :retryable-error? retryable-error?
     :log-errors true
     :message "save-version: retrying due to error")))



(defn publish-version
  "Calls the publish version api. Returns the body of the response."
  [base-url version-id max-retries]
  (let [url (str base-url "/versions/" version-id "/publish")]
    (safely
     (post url)

     :on-error
     :max-retry max-retries
     :retry-delay [:random-exp-backoff :base 300 :+/- 0.50 :max 30000]
     :retryable-error? retryable-error?
     :log-errors true
     :message "publish-version: retrying due to error")))
