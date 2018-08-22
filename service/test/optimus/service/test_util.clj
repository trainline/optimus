;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.test-util
  (:require [clojure.string :as str]
            [midje.sweet :refer :all]
            [optimus.service
             [core :as sut]
             [util :refer [rand-id]]]
            [optimus.service.backends.inmemory :as mem]
            [taoensso.timbre :as timbre]))

;; required in order to configure the logger and avoid flood of logs from AWS
(timbre/set-level! :info)


(defn rand-str
  [label]
  (str/join "__" [label (rand-id)]))



(defn backend-fixture
  "Returns a backend fixture, with a dataset as specified and one version for
   the dataset."
  ([]
   (backend-fixture nil nil))
  ([dsname tables]
   (let [queue      (mem/in-memory-queue)
         meta-store (mem/mutable-meta-data-store (mem/in-mem-meta-data-store {}))
         kv-store   (mem/mutable-kv-store (mem/in-mem-kv {}))
         bknd-store (sut/->BackendStore queue
                                        meta-store
                                        kv-store
                                        (rand-str "user")
                                        {:kv-store {:type :in-memory}
                                         :async-task
                                         {:operations-topic
                                          (rand-str "versions_topic")}})]

     (when (and dsname tables)
       (sut/create-dataset bknd-store {:name dsname
                                       :tables tables
                                       :audit-info {}})

       (sut/create-version bknd-store {:dataset dsname :label (rand-str "label")
                                       :audit-info nil}))

     bknd-store)))



(defchecker throws-validation-ex
  "A midje checker that checks if the result of a fact is an exception and
   the ex-data in the exception thrown by the fact contains
   :type => :validation-error. Optionally tests each key for value specified
   in the arguments.
   Usage:
      (throws-validation-ex)
      (throws-validation-ex :error :invalid-version-id)
      (throws-validation-ex [:addl-info :version-id] \"123134325\")"
  ([& tests]
   (fn [data]
     (when (instance? midje.util.exceptions.CapturedThrowable data)
       (let [ex (-> data midje.util.exceptions/throwable ex-data)]
         (->> (partition 2 tests)
              (#(conj % '(:ex-type :core-exception)))
              (map #(= (->> % first (conj []) (get-in ex)) (second %)))
              (every? true?)))))))
