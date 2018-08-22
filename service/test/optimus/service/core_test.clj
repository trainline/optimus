;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.core-test
  (:require [midje.sweet :refer :all]
            [optimus.service
             [backend :as b]
             [core :as sut]
             [test-util :refer [backend-fixture rand-str throws-validation-ex]]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                      ---==| U T I L I T I E S |==----                      ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn val-or-exception
  "Evaluates the function f in a try catch block and returns the value if call
   to f succeeds or the exception"
  [f]
  (try
    (f)
    (catch Exception e e)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                          ---==| T E S T S |==----                          ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(fact "create-entries: fails when version does not exist"
      (let [dsname     (rand-str "ds")
            tables     #{(rand-str "tb") (rand-str "ta")}
            bknd-store (backend-fixture dsname tables)
            version    (first (sut/get-versions bknd-store dsname))]
        (sut/create-entries bknd-store (rand-str "version") dsname
                            [{:table "t1" :key "k" :val "val"}])
        => (throws-validation-ex :error :version-not-found)))



(fact "create-entries: fails when version does not match dataset"
      (let [dsname     (rand-str "ds")
            tables     #{(rand-str "tb") (rand-str "ta")}
            bknd-store (backend-fixture dsname tables)
            version    (first (sut/get-versions bknd-store dsname))]
        (sut/create-entries bknd-store (:id version) (rand-str "ds")
                            [{:table "t1" :key "k" :val "value"}])
        => (throws-validation-ex :error :invalid-version-for-dataset)))




(fact "create-entries: fails when version is in an invalid state"
      (let [dsname     (rand-str "ds")
            tables     #{(rand-str "tb") (rand-str "ta")}
            bknd-store (backend-fixture dsname tables)
            version    (first (sut/get-versions bknd-store dsname))]
        (sut/create-entries bknd-store (:id version) dsname
                            [{:table "t1" :key "k" :val "val"}])
        => (throws-validation-ex :error :invalid-version-state)))



(fact "create-entries: fails when version is in an invalid state"
      (let [dsname     (rand-str "ds")
            tables     #{(rand-str "tb") (rand-str "ta")}
            bknd-store (backend-fixture dsname tables)
            version    (first (sut/get-versions bknd-store dsname))
            invalid-table (rand-str "table")]
        (b/update-status (:meta-store bknd-store) (:id version)
                         :awaiting-entries nil)

        (sut/create-entries bknd-store (:id version) dsname
                            [{:table invalid-table :key "k" :val "val"}])
        => (throws-validation-ex :error :tables-not-found)

        (->> (val-or-exception
              #(sut/create-entries bknd-store (:id version) dsname
                                   [{:table invalid-table :key "k" :val "value"}]))
             ex-data
             :missing-tables
             (map #(select-keys % [:table])))
        => (contains {:table invalid-table})))



(fact "create-entries: succeeds when valid data is passed"
      (let [dsname     (rand-str "ds")
            table      (rand-str "table")
            bknd-store (backend-fixture dsname #{table})
            version    (first (sut/get-versions bknd-store dsname))
            key        (rand-str "key")]
        (b/update-status (:meta-store bknd-store) (:id version)
                         :awaiting-entries nil)

        (sut/create-entries bknd-store (:id version) dsname
                            [{:table table :key key :value "valueable"}])

        (b/get-one (:kv-store bknd-store) {:dataset dsname :version (:id version)
                                           :table table :key key}) => "valueable"

        (sut/create-entries bknd-store (:id version) dsname table
                            [{:key key :value "more_valueable"}])

        (b/get-one (:kv-store bknd-store) {:dataset dsname :version (:id version)
                                           :table table :key key}) => "more_valueable"

        (sut/create-entries bknd-store (:id version) dsname table key "forever_valueable")

        (b/get-one (:kv-store bknd-store) {:dataset dsname :version (:id version)
                                           :table table :key key}) => "forever_valueable"))



(fact "save-version: fails with :version-not-found when version is not found"
      (let [dsname     (rand-str "ds")
            tables     #{(rand-str "tb") (rand-str "ta")}
            bknd-store (backend-fixture dsname tables)
            version    (first (sut/get-versions bknd-store dsname))]
        (sut/save-version bknd-store (rand-str "version"))
        => (throws-validation-ex :error :version-not-found)))



(fact "save-version: fails with :invalid-version-state when version is invalid"
      (let [dsname     (rand-str "ds")
            tables     #{(rand-str "tb") (rand-str "ta")}
            bknd-store (backend-fixture dsname tables)
            version    (first (sut/get-versions bknd-store dsname))]
        (sut/save-version bknd-store (:id version))
        => (throws-validation-ex :error :invalid-version-state)))



(fact "save-version: changes state to :saving and publishes :save event when successful"
      (let [dsname     (rand-str "ds")
            tables     #{(rand-str "tb") (rand-str "ta")}
            {:keys [pid queue meta-store config] :as bknd-store} (backend-fixture
                                                                  dsname
                                                                  tables)
            {:keys [id]}    (first (sut/get-versions bknd-store dsname))]
        ;;acknowledge the :prepare message
        (-> (b/reserve-next-message! (:queue bknd-store) (get-in config [:async-task :operations-topic]) pid)
            :id
            (#(b/acknowledge-message! queue % pid)))

        ;; update to :awaiting-entries
        (b/update-status meta-store id :awaiting-entries {})
        (sut/save-version bknd-store id)
        (-> (b/list-messages! queue {:topic (get-in config [:async-task :operations-topic]) :status :new})
            first
            :message
            (assoc :status (:status (sut/get-version bknd-store id))))
        => (contains {:status :saving :action :save :version-id id})))



(fact "publish-version: fails with :version-not-found when version is not found"
      (let [dsname     (rand-str "ds")
            tables     #{(rand-str "tb") (rand-str "ta")}
            bknd-store (backend-fixture dsname tables)
            version    (first (sut/get-versions bknd-store dsname))]
        (sut/publish-version bknd-store (rand-str "version"))
        => (throws-validation-ex :error :version-not-found)))



(fact "publish-version: fails with :invalid-version-state when version is invalid"
      (let [dsname     (rand-str "ds")
            tables     #{(rand-str "tb") (rand-str "ta")}
            bknd-store (backend-fixture dsname tables)
            version    (first (sut/get-versions bknd-store dsname))]
        (sut/publish-version bknd-store (:id version))
        => (throws-validation-ex :error :invalid-version-state)))



(fact "publish-version: changes state to :publishing and publishes :publish event when successful"
      (let [dsname     (rand-str "ds")
            tables     #{(rand-str "tb") (rand-str "ta")}
            {:keys [pid queue meta-store config] :as bknd-store} (backend-fixture
                                                                  dsname
                                                                  tables)
            {:keys [id]}    (first (sut/get-versions bknd-store dsname))]
        ;;acknowledge the :prepare message
        (-> (b/reserve-next-message! (:queue  bknd-store) (get-in config [:async-task :operations-topic]) pid)
            :id
            (#(b/acknowledge-message! queue % pid)))

        ;; update status to :awaiting entries
        (b/update-status meta-store id :awaiting-entries {})

        ;; save the version
        (sut/save-version bknd-store id)

        ;;acknowledge the :save message
        (-> (b/reserve-next-message! (:queue  bknd-store) (get-in config [:async-task :operations-topic]) pid)
            :id
            (#(b/acknowledge-message! queue % pid)))

        ;; update status to :saved
        (b/update-status meta-store id :saved {})

        (sut/publish-version bknd-store id)

        (-> (b/list-messages! queue {:topic (get-in config [:async-task :operations-topic]) :status :new})
            first
            :message
            (assoc :status (:status (sut/get-version bknd-store id))))
        => (contains {:status :publishing :action :publish :version-id id})))



(fact "discard-version: fails with :version-not-found when version is not found"
      (let [dsname     (rand-str "ds")
            tables     #{(rand-str "tb") (rand-str "ta")}
            bknd-store (backend-fixture dsname tables)
            version    (first (sut/get-versions bknd-store dsname))]
        (sut/publish-version bknd-store (rand-str "version"))
        => (throws-validation-ex :error :version-not-found)))



(fact "discard-version: fails with :invalid-version-state when version is invalid"
      (let [dsname     (rand-str "ds")
            tables     #{(rand-str "tb") (rand-str "ta")}
            bknd-store (backend-fixture dsname tables)
            version    (first (sut/get-versions bknd-store dsname))]
        (sut/publish-version bknd-store (:id version))
        => (throws-validation-ex :error :invalid-version-state)))



(fact "discard-version: changes state to :discarded."
      (let [dsname     (rand-str "ds")
            tables     #{(rand-str "tb") (rand-str "ta")}
            {:keys [pid queue meta-store config] :as bknd-store} (backend-fixture dsname tables)
            {:keys [id]}    (first (sut/get-versions bknd-store dsname))]
        ;;acknowledge the :prepare message
        (-> (b/reserve-next-message! (:queue  bknd-store) (get-in config [:async-task :operations-topic]) pid)
            :id
            (#(b/acknowledge-message! queue % pid)))

        ;; update status to :awaiting entries
        (b/update-status meta-store id :awaiting-entries {})

        ;; save the version
        (sut/save-version bknd-store id)

        ;;acknowledge the :save message
        (-> (b/reserve-next-message! (:queue  bknd-store) (get-in config [:async-task :operations-topic]) pid)
            :id
            (#(b/acknowledge-message! queue % pid)))

        ;; update status to :saved
        (b/update-status meta-store id :saved {})

        (sut/discard-version bknd-store id nil)
        (:status (sut/get-version bknd-store id)) => :discarded))
