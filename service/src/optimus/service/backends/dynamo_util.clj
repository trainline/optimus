;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.backends.dynamo-util
  "Common utilities for DynamoDB implementations"
  (:require [optimus.service.util :refer [throw-too-many-requests]])
  (:import [com.amazonaws.services.dynamodbv2.model
            ProvisionedThroughputExceededException]))


(defn handle-exceptions
  "Catches specific exceptions like
  ProvisionedThroughputExceededexception and implementes behaviour
  desirable for optimus api."
  [f & args]
  (try
    (apply f args)
    (catch ProvisionedThroughputExceededException p
      (throw-too-many-requests (.getMessage p) {}))))



(defn aws-config
  "Returns a valid credentials map to be passed to functions exposed
  by amazonica library. Expects :region and :aws-client-config keys in
  the config map specified and returns a single credentials map."
  [{:keys [region aws-client-config]}]
  (cond-> {:client-config aws-client-config}
    region   (assoc :endpoint region)))
