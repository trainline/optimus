;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.backends.metadatastore-test
  (:require [midje.sweet :refer :all]
            [optimus.service.test-util :refer [rand-str]]
            [optimus.service.backend :refer :all]
            [optimus.service.util :refer [uuid]]
            [optimus.service.backends.inmemory :refer [in-mem-meta-data-store]]
            [optimus.service.backends.dynamodb-meta-store
             :refer [DEFAULT-CONFIG dynamodb-meta-data-store]]
            [schema.core :as s]
            [optimus.service.backend :as b]))

(defn gen-version-id
  "Generates a new version id"
  []
  (uuid))



(def rand-dsname (partial rand-str "dataset"))



(def rand-version-label (partial rand-str "label"))



(def rand-version-id (partial rand-str "version"))



(def rand-table (partial rand-str "table"))



(defn rand-tables
  "Returns a set containing x random table names"
  [x]
  (into #{} (repeatedly x rand-table)))



(defn- version-fixture
  [meta-store dsname version-id label audit-info]
  (as-> {:id version-id :dataset dsname :label label
         :audit-info audit-info} $

    (b/create-version meta-store $)))



(defn dataset-tests
  "Tests for functions that operate on datasets"
  [meta-store]
  (facts "create-dataset"
         (tabular
          (fact "validates dataset name, tables and audit-info supplied"
                (create-dataset meta-store {:name ?dsname :tables ?tables :audit-info ?audit-info})
                => ?result)
          ?dsname         ?tables                ?audit-info      ?result
          (rand-dsname)   (rand-tables 2)        {}               truthy
          nil             #{"t1" "t2"}           {}               (throws Exception)
          ""              (rand-tables 2)        {}               (throws Exception)
          (rand-dsname)   #{}                    {}               (throws Exception)
          (rand-dsname)   nil                    {}               (throws Exception)
          (rand-dsname)   ["t1" "t1"]            {}               (throws Exception)
          (rand-dsname)   (rand-tables 2)        nil              truthy
          (rand-dsname)   (rand-tables 2)        "someaudit"      (throws Exception))



         (fact "enforces uniqueness of dataset name"
               (let [dsname (rand-dsname)]
                 (-> meta-store
                     (create-dataset {:name dsname :tables (rand-tables 2)})
                     (create-dataset {:name dsname :tables (rand-tables 1)})))
               => (throws Exception))



         (fact "creates dataset successfully if dataset name is valid"
               (let [dsname (rand-dsname)
                     tables (rand-tables 1)]
                 (-> meta-store
                     (create-dataset {:name dsname :tables tables :audit-info {:user "sathya"}})
                     (get-dataset dsname))
                 => (contains {:name dsname :tables tables})))



         (fact "sets audit data when supplied"
               (let [dsname (rand-dsname)
                     uname (rand-str "user")]
                 (-> meta-store
                     (create-dataset {:name dsname :tables (rand-tables 1) :audit-info {:user uname}})
                     (get-dataset dsname)
                     :operation-log first :user) => uname)))



  (facts "get-dataset"
         (fact "returns dataset when a valid dataset name is supplied"
               (let [dsname  (rand-dsname)
                     tables  (rand-tables 1)
                     dataset (-> meta-store
                                 (create-dataset {:name dsname :tables tables :audit-info {}})
                                 (get-dataset dsname))]
                 ;; check if the dataset contains th correct content
                 dataset => (contains {:name dsname :tables tables})
                 ;; check if the dataset returned conforms to the correct schema
                 (s/check Dataset dataset) => nil))



         (fact "returns nil if no dataset is found with the given name"
               (get-dataset meta-store (rand-dsname)) => nil)



         (fact "returns nil if the dataset name supplied is nil"
               (get-dataset meta-store nil) => nil))



  (facts "get-all-datasets"
         (fact "returns all the datasets"
               (let [dsname1 (rand-dsname)
                     dsname2 (rand-dsname)
                     data (-> meta-store
                              (create-dataset {:name  dsname1 :tables (rand-tables 1)})
                              (create-dataset {:name  dsname2 :tables (rand-tables 1)})
                              (get-all-datasets))]
                 (-> data count (>= 2))  => truthy
                 (s/check [Dataset] data) => nil
                 (map :name data) => (contains [dsname1 dsname2] :gaps-ok :in-any-order)))


         ;;TODO: how to check this for mutable stores ? remove if this test is FUD
         (comment fact "returns nil when there are no datasets"
                  (get-all-datasets meta-store) => nil)))



(defn version-tests
  "Tests for functions that operate on versions"
  [meta-store]
  (facts "create-version"
         (fact "validates if the dataset supplied exists"
               (version-fixture meta-store (rand-dsname) (rand-version-id)
                                (rand-version-label) nil)
               => (throws Exception))

         (fact "adds a version id when a new version is created"
               (let [dsname (rand-dsname)]
                 (-> meta-store
                     (create-dataset {:name  dsname :tables (rand-tables 1)})
                     (version-fixture dsname (rand-version-id) "1.0" nil)
                     (get-versions dsname)
                     first
                     :id)) =not=> nil)



         (fact "returns the correct type"
               (let [dsname (rand-dsname)]
                 (-> meta-store
                     (create-dataset {:name  dsname :tables (rand-tables 1)})
                     (version-fixture dsname (gen-version-id) "1.0" nil)
                     type)) => (type meta-store)))



  (facts "get-all-versions"
         (fact "returns all versions"
               (let [dsname (rand-dsname)]
                 (-> meta-store
                     (create-dataset {:name dsname :tables (rand-tables 1)})
                     (version-fixture dsname (gen-version-id) "1.0" nil)
                     (version-fixture dsname (gen-version-id) "2.0" nil)
                     (get-all-versions)
                     (count)
                     (>= 2))) => truthy)


         ;;TODO: doesnt work very well with mutable stores.
         (comment fact "returns nil if there are no versions"
                  (get-all-versions meta-store) => nil)



         (fact "returns all versions"
               (let [dsname (rand-dsname)]
                 (-> meta-store
                     (create-dataset {:name dsname :tables (rand-tables 1)})
                     (version-fixture dsname (gen-version-id) "1.0" nil)
                     (version-fixture dsname (gen-version-id) "2.0" nil)
                     (get-all-versions)
                     ((partial s/check [Version])))) => nil))



  (facts "get-versions"                 ;get-versions by dataset name
         (fact "returns all versions for the dataset supplied"
               (let [dsname1 (rand-dsname)
                     dsname2 (rand-dsname)]
                 (-> meta-store
                     (create-dataset {:name  dsname1 :tables (rand-tables 1)})
                     (version-fixture dsname1 (gen-version-id) "1.0" nil)
                     (version-fixture dsname1 (gen-version-id) "2.0" nil)
                     (create-dataset {:name dsname2 :tables (rand-tables 1)})
                     (version-fixture dsname2 (gen-version-id) "2.0" nil)
                     (get-versions dsname1) count)) => 2)



         (fact "returns nil if there are no versions forthe dataset supplied"
               (let [dsname (rand-dsname)]
                 (-> meta-store
                     (create-dataset {:name dsname :tables (rand-tables 1)})
                     (get-versions dsname))) => nil)



         (fact "returns nil if dataset requested does not exist"
               (get-versions meta-store (rand-dsname)) => nil)



         (fact "returns valid version records"
               (let [dsname (rand-dsname)]
                 (-> meta-store
                     (create-dataset {:name  dsname :tables (rand-tables 1)})
                     (version-fixture dsname (rand-version-id) "1.0" nil)
                     (version-fixture dsname (rand-version-id) "2.0" nil)
                     (get-versions dsname)
                     ((partial s/check [Version])))) => nil))



  (facts "get-version"                  ;get-version by version-id
         (tabular
          (fact "validates the version-id supplied in the request"
                (get-version meta-store ?ver-id) => ?result)
          ?ver-id             ?result
          ""                  (throws Exception)
          nil                 (throws Exception)
          123                 (throws Exception)
          "valid-version"     nil)



         (fact "returns a valid response"
               (let [dsname (rand-dsname)
                     store (-> meta-store
                               (create-dataset {:name  dsname :tables (rand-tables 1)})
                               (version-fixture dsname (gen-version-id) "1.0" nil))
                     id     (-> store (get-versions dsname) first :id)
                     version (get-version store id)]
                 (s/check Version version) => nil
                 (:id version) => id)))



  (facts "update-status"
         (fact "ensures version-id being updated exists"
               (update-status meta-store (rand-version-id) :saved {})
               => (throws Exception))



         (tabular
          (fact "validates input arguments"
                (let [version-id ?ver
                      dsname     (rand-dsname)
                      mstore     (-> meta-store
                                     (create-dataset {:name dsname :tables (rand-tables 1)})
                                     (version-fixture dsname version-id (rand-version-label) nil))]
                  (update-status mstore version-id ?target-state ?audit-info)) => ?result)
          ?ver                ?target-state         ?audit-info  ?result
          (rand-version-id)   :awaiting-entries     {}           truthy
          (rand-version-id)   ""                    nil          (throws Exception)
          nil                 :awaiting-entries     {}           (throws Exception)
          (rand-version-id)   nil                   nil          (throws Exception))



         (fact "records audit history"
               (let [version-id (gen-version-id)
                     dsname     (rand-dsname)
                     mstore     (-> meta-store
                                    (create-dataset {:name dsname :tables (rand-tables 1)})
                                    (version-fixture dsname version-id (rand-version-label) nil)
                                    (update-status version-id :awaiting-entries
                                                   {:user "user"}))]
                 (-> (get-version mstore version-id)
                     :operation-log count)) => 2)



         (let [mstore (atom (in-mem-meta-data-store {}))
               dsname (rand-dsname)
               version-id (rand-version-id)]
           ;; create dataset
           (swap! mstore create-dataset {:name  dsname :tables (rand-tables 1)})
           ;; create a version
           (swap! mstore version-fixture dsname version-id (rand-version-label) nil)
           ;; TODO: make this test dynamic to test all transitions.
           (tabular
            (fact "ensures status transitions are valid"
                  (swap! mstore update-status version-id ?state nil) => ?result)
            ?state                     ?result
            :saved                     (throws Exception)
            :awaiting-entries          truthy
            :saved                     (throws Exception)
            :saving                    truthy
            :saved                     truthy
            :publishing                truthy
            :published                 truthy)))



  (facts "activate-status"
         (fact "ensures current version is 'published'"
               (let [dsname (rand-dsname)
                     version-id (gen-version-id)]
                 (-> meta-store
                     (create-dataset {:name dsname :tables (rand-tables 1)})
                     (version-fixture dsname version-id "1.0" nil)
                     (activate-version version-id)) => (throws Exception)))



         (fact "sets version.dataset.active-version to version-id"
               (let [dsname (rand-dsname)
                     version-id (gen-version-id)]
                 (-> meta-store
                     (create-dataset {:name dsname :tables (rand-tables 1)})
                     (version-fixture dsname version-id "1.0" nil)
                     (update-status version-id :awaiting-entries nil)
                     (update-status version-id :saving nil)
                     (update-status version-id :saved nil)
                     (update-status version-id :publishing nil)
                     (update-status version-id :published nil)
                     (activate-version version-id)
                     (get-dataset dsname)) => (contains {:active-version
                                                         version-id})))))



(defn run-all-tests
  "Run all tests"
  [meta-store description]
  (dataset-tests meta-store)
  (version-tests meta-store))



(facts "(*) compatibility tests for: in-memory-meta-data-store"
       (run-all-tests (in-mem-meta-data-store {}) "In Memory Meta Data Store Implementation"))


(facts "(*) compatibility tests for: dynamo-meta-data-store" :integration
       (run-all-tests (dynamodb-meta-data-store
                       (assoc DEFAULT-CONFIG
                              :meta-store-table "OptimusMetadataStore-Test"))
                      "DynamoDB Meta data store Implementation"))
