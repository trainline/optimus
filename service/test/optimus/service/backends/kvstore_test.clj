;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.backends.kvstore-test
  (:require [midje.sweet :refer :all]
            [optimus.service.test-util :refer [rand-str]]
            [optimus.service.util :refer [deep-merge]]
            [optimus.service.backend :refer :all]
            [optimus.service.backends.inmemory :refer [in-mem-kv]]
            [optimus.service.backends.dynamodb-kv :refer [dynamodb-kv]]
            [optimus.service.backends.middleware.binary-kv-store
             :refer [binary-kv-store]]
            [schema.core :as s]))



(def rand-ver (partial rand-str "version"))

(def rand-ds (partial rand-str "dataset"))

(def rand-table (partial rand-str "table"))

(def rand-key (partial rand-str "key"))

(def rand-value (partial rand-str "value"))



(defn key-fixture
  "Returns a new `Key` with random values"
  []
  {:version (rand-ver)
   :dataset (rand-ds)
   :table   (rand-table)
   :key     (rand-key)})



(defn put-one-tests
  "Tests for put-one"
  [kvstore]
  (facts "put-one"
         (tabular
          (fact "validates presence of version,dataset,table and key"
                (put-one kvstore ?key "value")) => ?result
          ?result               ?key
          (throws Exception)    {:version (rand-ver)}
          (throws Exception)    {:dataset (rand-ds)}
          (throws Exception)    {:version (rand-ver) :table (rand-table)}
          (throws Exception)    {:version (rand-ver) :dataset (rand-ds)}
          true                  (key-fixture))



         (fact "returns the correct type"
               (-> kvstore
                   (put-one (key-fixture) "value")) => (partial satisfies? KV))



         (fact "adds new key if key is absent"
               (let [value "value1"
                     key (key-fixture)]
                 (-> kvstore
                     (put-one key value)
                     (get-one key)) => value))



         (fact "updates an existing key with new value"
               (let [key (key-fixture)]
                 (-> kvstore
                     (put-one key "value1")
                     (put-one key "value2")
                     (get-one key))) => "value2")))



(defn put-many-tests
  "Tests for put-many"
  [kvstore]
  (facts "put-many"
         (tabular
          (fact "validates entries as per optimus.service.backend/Entry"
                (put-many kvstore ?entries) => ?result)
          ?result               ?entries
          not-empty             {(key-fixture) "value1"}
          (throws Exception)    {{:version (rand-ver)} "somevalue"}
          (throws Exception)    nil
          (throws Exception)    ["somekey" "somevalue"]
          (throws Exception)    {:key :value}
          (throws Exception)    {(key-fixture) "value1"
                                 {:version "v1"} "value2"})



         (fact "returns the correct type"
               (let [key1 (key-fixture)
                     key2 (key-fixture)]
                 (-> kvstore
                     (put-many {key1 "v1"
                                key2 "v2"})) => (partial satisfies? KV)))



         (fact "updates all entries supplied"
               (let [entries (hash-map  (key-fixture) "value1"
                                        (key-fixture) "value2"
                                        (key-fixture) "value3")]
                 (-> kvstore
                     (put-many entries)
                     (get-many (keys entries))
                     count)) => 3)



         (fact "upserts keys"
               (let [key1 (key-fixture)
                     key2 (key-fixture)]
                 (-> kvstore
                     (put-one key1 "value1")
                     (put-many {key1 "value2"
                                key2 "value3"})
                     (get-many [key1
                                key2])
                     ((partial map second))))
               => (just '("value2" "value3") :in-any-order))))



(defn get-one-tests
  "Tests for get-one"
  [kvstore]
  (facts "get-one"
         (tabular
          (fact "validates key as per optimus.service.backend/Key"
                (get-one kvstore ?key) => ?result)
          ?result                ?key
          nil                    (key-fixture)
          (throws Exception)     "key"
          (throws Exception)     {:version "v1"})


         (fact "returns the correct type"
               (let [key1 (key-fixture)
                     key2 (key-fixture)]
                 (-> kvstore
                     (put-many {key1 "value1"
                                key2 "value2"})
                     (get-one key1)
                     ((partial s/check s/Str))))  => nil)



         (fact "returns the correct item"
               (let [key1 (key-fixture)
                     key2 (key-fixture)]
                 (-> kvstore
                     (put-many {key1 "value1"
                                key2 "value2"})
                     (get-one key1)))  => "value1")



         (fact "returns nil when the key is not found"
               (let [key1 (key-fixture)
                     key2 (key-fixture)]
                 (-> kvstore
                     (put-many {key1 "value1"
                                key2 "value2"})
                     (get-one (key-fixture))))  => nil)))



(defn get-many-tests
  "Tests for get-many"
  [kvstore]
  (facts "get-many"
         (tabular
          (fact "validates key as per optimus_data_store.service.backend/Key"
                (get-many kvstore ?key) => ?result)
          ?result                ?key
          not-empty              [(key-fixture)]
          (throws Exception)     (key-fixture)
          (throws Exception)     "key"
          (throws Exception)     {:version (rand-ver)})


         (fact "returns the correct type"
               (let [key1 {:version "v1" :dataset "ds1" :table "t1"
                           :key "k1"}
                     key2 {:version "v1" :dataset "ds1" :table "t1"
                           :key "k2"}]
                 (-> kvstore
                     (put-many {key1 "value1"
                                key2 "value2"})
                     (get-many [key1])
                     ((partial s/check Entries))))  => nil)



         (fact "returns all keys present in the data store"
               (let [key1 {:version "v1" :dataset "ds1" :table "t1"
                           :key "k1"}
                     key2 {:version "v1" :dataset "ds1" :table "t1"
                           :key "k2"}]
                 (-> kvstore
                     (put-many {key1 "value1"
                                key2 "value2"})
                     (get-many [key1
                                (key-fixture)
                                (key-fixture)])
                     vals))  => (just ["value1" nil nil] :in-any-order))))



(defn kvstore-tests
  "KV Store Tests"
  [kvstore description]

  (facts (str  "(*) " description)
         (put-one-tests kvstore)
         (put-many-tests kvstore)
         (get-one-tests kvstore)
         (get-many-tests kvstore)))



(facts "(*) compatibility tests for: in-memory-kv"
       ;; Run the tests for InMemKV store
       (kvstore-tests (kv-validation (in-mem-kv {})) "InMemoryKVStore"))


(facts "(*) compatibility tests for: dynamo-kv" :integration
       (kvstore-tests (kv-validation (dynamodb-kv {:kv-store-table "OptimusKV-Test"}))
                      "DynamoDBKVStore"))

(facts "(*) compatibility tests for: binary-dynamo-kv" :integration
       (-> (dynamodb-kv {:type :dynamodb :kv-store-table "OptimusKV-Test"})
           binary-kv-store
           kv-validation
           (kvstore-tests "BinaryDynamoDBKVStore")))
