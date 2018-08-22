;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.backends.async-task-test
  (:require [midje.sweet :refer :all]
            [optimus.service
             [async-task :as a]
             [backend :as b]
             [core :as sut]
             [main :as m]
             [util :refer [deep-merge]]
             [test-util :refer [backend-fixture rand-str throws-validation-ex]]]
            [optimus.service.backends.inmemory :as mem]
            [optimus.service.backends.dynamodb-kv :as dkv]))


(fact "handle-message (:publish) sets the version requested to :published and
       the current published version to :saved"
      (let [dsname     (rand-str "ds")
            tables     #{(rand-str "tb") (rand-str "ta")}
            {:keys [pid queue meta-store] :as bknd-store}
            (backend-fixture dsname tables)
            version1 (first (sut/get-versions bknd-store dsname))
            version-id2 (rand-str "version")]

        ;;transition to :publishing status
        (doall
         (map #(b/update-status meta-store (:id version1) % {})
              [:awaiting-entries :saving :saved :publishing]))

        (a/handle-message bknd-store {:action :publish :version-id (:id version1)} (fn []))
        (b/get-version meta-store (:id version1)) => (contains {:status :published})
        (b/get-dataset meta-store dsname) => (contains {:active-version (:id version1)})


        ;;create a new version
        (b/create-version meta-store {:dataset dsname :id version-id2 :label (rand-str "label")
                                      :audit-info {}})
        ;;
        ;; handle :prepare action
        (a/handle-message bknd-store {:action :prepare :version-id version-id2} (fn []))
        (b/get-version meta-store version-id2) => (contains {:status :awaiting-entries})

        ;; update status to :saving
        (b/update-status meta-store version-id2 :saving {})

        ;; handle :save action
        (a/handle-message bknd-store {:action :save :version-id version-id2} (fn []))
        (b/get-version meta-store version-id2) => (contains {:status :saved})

        ;; update status to :publishing
        (b/update-status meta-store version-id2 :publishing {})

        ;;handle :publish action
        (a/handle-message bknd-store {:action :publish :version-id version-id2} (fn []))
        (b/get-version meta-store version-id2) => (contains {:status :published})
        (b/get-version meta-store (:id version1)) => (contains {:status :saved})
        (b/get-dataset meta-store dsname) => (contains {:active-version version-id2})))
