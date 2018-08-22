(ns optimus.dev-tools.create-dynamodb-tables
  (:require [optimus.service.backends.dynamodb-kv :as kv]
            [optimus.service.backends.dynamodb-meta-store :as ms]
            [optimus.service.backends.dynamodb-queue :as q])
  (:gen-class))


(defn -main
  "Create all the necessary dynamodb tables"
  [& args]
  (let [aws-region (first args)
        aws-config (if aws-region {:region aws-region} {})]
    (kv/create-tables aws-config)
    (q/create-tables  aws-config)
    (ms/create-tables aws-config)))
