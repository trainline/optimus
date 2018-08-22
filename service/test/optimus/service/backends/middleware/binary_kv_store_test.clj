;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.backends.middleware.binary-kv-store-test
  (:require [optimus.service.backend :refer :all]
            [optimus.service.backends.middleware.binary-kv-store :refer :all]
            [optimus.service.backends.inmemory :as mem]
            [midje.sweet :refer :all]
            [taoensso.nippy :as nippy]))


(defn test-data []
  {:string "string"
   :integer 1234
   :bigint  (bigint 1234)
   :double  4.3
   :ratio   10/3
   :date    (java.util.Date.)
   :uuid    (java.util.UUID/randomUUID)
   :nil     nil
   :keyword :foo
   :boolean true
   :vector  [1 2 3 []]
   :map     {:foo 1}
   :set     #{:a :b :c}})



(defn test-store
  ([]
   (test-store (mem/in-mem-kv {})))
  ([backend]
   (binary-kv-store backend {:metrics-prefix "foo"})))


(facts "the binary middleware offers a safe roundtrip (what you store is what you get)"


       (fact "roundtrip on common data with put-one/get-one"
             (let [data (test-data)]
               (-> (test-store)
                   (put-one {:version "v1" :dataset "d1" :table "t1" :key "k1"} data)
                   (get-one {:version "v1" :dataset "d1" :table "t1" :key "k1"}))
               => data))


       (fact "roundtrip on common data with put-many/get-many"
             (let [data1 (test-data)
                   data2 (test-data)]
               (-> (test-store)
                   (put-many {{:version "v1" :dataset "d1" :table "t1" :key "k1"} data1
                              {:version "v2" :dataset "d2" :table "t2" :key "k2"} data2})
                   (get-many [{:version "v1" :dataset "d1" :table "t1" :key "k1"}
                              {:version "v2" :dataset "d2" :table "t2" :key "k2"}] ))

               => {{:version "v1" :dataset "d1" :table "t1" :key "k1"} data1
                   {:version "v2" :dataset "d2" :table "t2" :key "k2"} data2}))
       )



(facts "corner cases like keys not found"


       (fact "not found get/put-one"
             (let [data (test-data)]
               (-> (test-store)
                   (put-one {:version "v1" :dataset "d1" :table "t1" :key "k1"} data)
                   (get-one {:version "v2" :dataset "d2" :table "t2" :key "k2"}))
               => nil))


       (fact "not found get/put-many"
             (let [data1 (test-data)
                   data2 (test-data)]
               (-> (test-store)
                   (put-many {{:version "v1" :dataset "d1" :table "t1" :key "k1"} data1
                              {:version "v2" :dataset "d2" :table "t2" :key "k2"} data2})
                   (get-many [{:version "v1" :dataset "d1" :table "t1" :key "k1"}
                              {:version "v2" :dataset "d2" :table "t2" :key "k2"}
                              {:version "v3" :dataset "d3" :table "t3" :key "k3"}] ))

               => {{:version "v1" :dataset "d1" :table "t1" :key "k1"} data1
                   {:version "v2" :dataset "d2" :table "t2" :key "k2"} data2
                   {:version "v3" :dataset "d3" :table "t3" :key "k3"} nil}))


       )



(facts "if data is already present from a previous load it doesn't try to un-compress it."


       (fact "not found get/put-one"
             (let [data1  (test-data)
                   kv-mem (-> (mem/in-mem-kv {})
                              (put-one {:version "v1" :dataset "d1" :table "t1" :key "k1"} data1))
                   data2 (test-data)]
               (-> (test-store kv-mem)
                   (put-one {:version "v2" :dataset "d2" :table "t2" :key "k2"} data2)
                   (get-one {:version "v1" :dataset "d1" :table "t1" :key "k1"}))
               => data1))
       )
