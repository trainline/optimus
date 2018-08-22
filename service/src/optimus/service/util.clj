;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.util
  "This namespace contains common utility functions. Duh!"
  (:require [cheshire.core :as json]
            [clojure
             [edn :as edn]
             [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [safely.core :refer [safely]]
            [schema.core :as s])
  (:import java.nio.ByteBuffer
           java.util.UUID))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                 ---==| C O M M O N   S C H E M A S |==----                 ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn web-safe?
  "Predicate that checks if the string specified is web safe"
  [str]
  (re-matches #"^[A-Za-z0-9-_.]*$" str))



(def NonEmptyString
  "Schema for a String that is not empty"
  (s/conditional not-empty s/Str))



(def WebSafeNonEmptyString
  "Schema for a non-empty websafe string"
  (s/conditional #(not (s/check NonEmptyString %)) (s/pred web-safe?)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;               ---==| S T O P P A B L E   T H R E A D |==----               ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Copyright © 2015-2016 Samsara's authors.
;; Lifted from:
;; https://github.com/samsara/samsara/blob/master/moebius/src/moebius/core.clj
(defn stoppable-thread
  "Execute the function `f` in a separate thread called `name`,
   and it return a function without arguments which when called it
  stop the execution of the thread.  The function `f` can be a
  thunk (function with no arguments) or can optionally have a state
  which is passed on every execution and the result of a prior
  execution of `f` is passed to the next execution. The first time it
  is called with `nil`. When the function `f' expects a sate then
  the option `:with-state true' must be passed.
  Between two execution the thread will sleep of 3000 millis (configurable
  with :sleep-time 5000)
  Ex:
      (def t (stoppable-thread \"hello\" (fn [] (println \"hello world\"))))
      ;; in background you should see every 3s appear the following message
      ;; hello world
      ;; hello world
      ;; hello world
      ;; to stop the thread
      (t)
      (def t (stoppable-thread \"counter\"
               (fn [counter]
                  (let [c (or counter 0)]
                    (println \"counter:\" c)
                    ;; return the next value
                    (inc c)))
               :with-state true
               :sleep-time 1000))
      ;; in background you should see every 1s appear the following message
      ;; counter: 0
      ;; counter: 1
      ;; counter: 2
      ;; counter: 3
      ;; to stop the thread
      (t)
  "
  {:style/indent 1}
  [name f & {:keys [with-state sleep-time]
             :or {with-state false
                  sleep-time 3000}}]
  (let [stopped (atom false)
        thread
        (Thread.
         (fn []
           (log/debug "Starting thread:" name)
           (loop [state nil]

             (let [new-state
                   (safely
                    (if with-state (f state) (f))
                    :on-error
                    :message name
                    :log-level :trace
                    :default nil)]

               (safely
                (Thread/sleep sleep-time)
                :on-error
                :message (str name "/sleeping")
                :log-level :trace
                :default nil)

               ;; if the thread is interrupted then exit
               (when-not @stopped
                 (recur new-state)))))
         name)]
    (.start thread)
    ;; return a function without params which
    ;; when executed stop the thread
    (fn []
      (swap! stopped (constantly true))
      (.interrupt thread)
      (log/debug "Stopping thread:" name))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                     ---==| J S O N   U T I L S |==----                     ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn to-json
  "Convert a Clojure data structure into its json equivalent
   compacted version.
   usage:
   (to-json {:a \"value\" :b 123})
   ;=> {\"a\":\"value\",\"b\":123}
   "
  [data]
  (if-not data
    ""
    (json/generate-string data {:date-format "yyyy-MM-dd'T'HH:mm:ss.SSSX"})))



(defn from-json
  "Convert a json string into a Clojure data structure
   with keyword as keys"
  [data]
  (if-not data
    nil
    (json/parse-string data true)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                     ---==| M I S C   U T I L S |==----                     ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn uuid
  "Returns a new UUID."
  []
  (.toString (java.util.UUID/randomUUID)))



(defn rand-id
  "Returns a 128 bit random id (based on UUID) in a short format.
   It generates a random UUID and then encodes it into base36."
  []
  (let [^UUID       id (UUID/randomUUID)
        ^ByteBuffer buf (ByteBuffer/allocate (* 2 Long/BYTES))
        _ (.putLong buf (.getMostSignificantBits id))
        _ (.putLong buf (.getLeastSignificantBits id))]
    (-> (java.math.BigInteger. 1 (.array buf))
        (.toString Character/MAX_RADIX))))



(defn now
  "Returns the current time in milliseconds. Note that while the unit of
   time of the return value is a millisecond, the granularity of the
   value depends on the underlying operating system and may be
   larger. For example, many operating systems measure time in units of
   tens of milliseconds.

   See the description of the class Date for a discussion of slight
   discrepancies that may arise between computer time and coordinated
   universal time (UTC).
  "
  []
  (System/currentTimeMillis))



(defn deep-merge
  "Like merge, but merges maps recursively."
  [& maps]
  (let [maps (filter (comp not nil?) maps)]
    (if (every? map? maps)
      (apply merge-with deep-merge maps)
      (last maps))))



(defn read-config
  "Reads the config from a file in EDN format"
  [config-file]
  (when config-file
    (edn/read-string (slurp config-file))))



(defn service-version
  "Reads the version of the running service"
  []
  (try
    (->> "optimus.version" io/resource slurp str/trim)
    (catch Exception x "development")))



(defn load-function-from-name
  "Loads a function from a specified name. This function also requires
  the namespace in-order to make sure the resolution of the fqn is
  successful.

  NOTE - Copied from Samsara/trackit
  https://github.com/samsara/trackit/blob/master/trackit-core/src/samsara/trackit/reporter.clj
  Copyright © 2015-2016 Samsara's authors."
  ([fqn-fname]
   (if (string? fqn-fname)
     (let [[_ fns ff] (re-find #"([^/]+)/([^/]+)" fqn-fname)]
       (when (not (and fns ff))
         (throw
          (ex-info
           (str "function '" fqn-fname "' is invalid format. must be \"namespace/fun-name\".") {})))
       (load-function-from-name fns ff))
     fqn-fname))
  ([fn-ns fn-name]
   (when (not (and fn-ns fn-name))
     (throw (ex-info (str "function '" fn-ns "/" fn-name "' not found.") {})))
   ;; requiring the namespace
   (require (symbol fn-ns))
   (let [fn-symbol (resolve (symbol fn-ns fn-name))]
     (when-not fn-symbol
       (throw (ex-info (str "function '" fn-ns "/" fn-name "' not found.") {})))
     fn-symbol)))



(defn throw-core-exception
  "Throws an exception using ex-info with the supplied message and ex-data.
   Adds :ex-type :core-exception to ex-data."
  [ex-msg ex-data]
  (throw
   (ex-info ex-msg (assoc ex-data :ex-type :core-exception))))



(defn throw-validation-error
  "Throws an exception with :ex-type = :validation-exception and
   :type :validation-error. This error is thrown when a request is invalid"
  [ex-msg ex-data]
  (throw-core-exception ex-msg (assoc ex-data :type :validation-error)))



(defn throw-missing-entity
  "Throws an exception with :ex-type = :validation-exception and
   :type :validation-error. This error is thrown when an operation is requested
   on an entity which is not found."
  [ex-msg ex-data]
  (throw-core-exception ex-msg (assoc ex-data :type :missing-entity)))



(defn throw-conflict
  "Throws an exception with :ex-type = :validation-exception and
   :type :conflict. This error is thrown when an entity is
  concurrently updated during an update operation"
  [ex-msg ex-data]
  (throw-core-exception ex-msg (assoc ex-data :type :conflict)))



(defn throw-too-many-requests
  "Throws an exception with :ex-type = :validation-exception and
   :type :too-many-requests. This error is thrown when the requests to
  the backend are throttled"
  [ex-msg ex-data]
  (throw-core-exception ex-msg (assoc ex-data :type :too-many-requests)))




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                  ---==| M E T R I C S   U T I L S |==----                  ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Note the prefix must atleast contain 2 dots.
(def ^:const metrics-prefix "optimus.api")



(def dash->underscore #(str/replace % #"-" "_"))



(defn metrics-name
  "Creates a metric name by joining the args supplied with a dot"
  [& args]
  (->> args
       (conj [metrics-prefix])
       flatten
       (map name)
       (map dash->underscore)
       (str/join ".")))
