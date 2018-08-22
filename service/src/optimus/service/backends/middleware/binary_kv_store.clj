;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.backends.middleware.binary-kv-store
  "This is a implementation of the KV store protocol
   which encodes the value as a binary payload it optionally
   compress it. This can be used as a middleware between
   the client and the actual storage engine."
  (:require [optimus.service
             [backend :refer :all]
             [util :refer [metrics-name]]]
            [samsara.trackit :refer [track-distribution track-time]]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.compression :as compression])
  (:import java.nio.HeapByteBuffer))

;;
;; We want to be able to deserialize only when the value is serialized
;; for a particular version.  This means that on the same database we
;; could have a dataset/table which was loaded before this change,
;; therefore it will be a clojure map or string, and the same
;; dataset/table have a newer version which is serialized with nippy.
;; If the code detects that the value returned from the underlying
;; datastore is a byte[] then it will assume it has been serialized
;; with nippy and it will attempt to deserialize. If the returned
;; value is anything else then it will be just passed down.
;;
;; TODO: this in the future should come from a dataset/table/version
;; configuration.
;;


;;
;; Raw serialization functions
;;

(defn- raw-serialize [val config]
  (nippy/freeze val config))


(def ^:const byte-array-type (type (byte-array 1)))


(defn byte-array? [val]
  (= (type val) byte-array-type))


(defn byte-buffer? [val]
  (= (type val) java.nio.HeapByteBuffer))


(defn- raw-deserialize [val config]
  (cond
    (byte-buffer? val) (nippy/thaw (.array ^java.nio.HeapByteBuffer val) config)
    (byte-array? val)  (nippy/thaw val config)
    :else val))

;;
;; Instrumented version of the serialize and deserialize
;;
(defn iserialize [val {:keys [metrics-prefix] :as config}]
  (let [val' (track-time
              (metrics-name metrics-prefix :serialize :time)
              (raw-serialize val config))
        ;; track compressed size
        _ (track-distribution
           (metrics-name metrics-prefix :serialize :size)
           (count val'))]
    ;; return serialized value
    val'))


(defn ideserialize [val {:keys [metrics-prefix] :as config}]
  (track-time
   (metrics-name metrics-prefix :deserialize :time)
   (raw-deserialize val config)))

;;
;; General version
;;
(defn serialize [val {:keys [metrics-prefix] :as config}]
  (if metrics-prefix
    (iserialize val config)
    (raw-serialize val config)))


(defn deserialize [val {:keys [metrics-prefix] :as config}]
  (if metrics-prefix
    (ideserialize val config)
    (raw-deserialize val config)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                ---==| B I N A R Y   K V   S T O R E |==----                ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defrecord BinaryKVStore [config backend]
  KV

  (put-one [_ {:keys [version dataset table key] :as akey} val]
    (BinaryKVStore. config
                    (put-one backend akey (serialize val config))))



  (get-one [kvstore {:keys [version dataset table key] :as akey}]
    (some-> (get-one backend akey)
            (deserialize config)))



  (get-many [kvstore keys]
    (->> (get-many backend keys)
         (map (fn [[k v]] [k (deserialize v config)]))
         (into {})))



  (put-many [kvstore entries]
    (BinaryKVStore. config
                    (->> entries
                         (map (fn [[k v]] [k (serialize v config)]))
                         (into {})
                         (put-many backend)))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                         ---==| C O N F I G |==----                         ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(def ^:const DEFAULT_CONFIG
  {;; The compressor used to compress the binary data
   ;; possible options are: :none, :lz4, snappy, :lzma2,
   ;; :lz4hc. Default :lz4
   :compressor :lz4

   ;; The encryption algorithm used to encrypt the payload
   ;; :encryptor aes128-encryptor

   ;; If you wish to encrypt the data then add a password.
   ;; :password [:salted "your-pass"]

   ;; If you wish track how much time the serialization
   ;; takes and how big is the payload you need to
   ;; provide a `:metrics-prefix` value
   ;; :metrics-prefix ["kv" "binary"]

   })



(defn binary-kv-store
  ([backend]
   (binary-kv-store backend {}))
  ([backend config]
   (-> (merge DEFAULT_CONFIG config)
       (update :compressor #(case %
                              :lz4     compression/lz4-compressor
                              :snappy  compression/snappy-compressor
                              :lzma2   compression/lzma2-compressor
                              :lz4hc   compression/lz4hc-compressor
                              :none    nil
                              :default nil))
       (BinaryKVStore. backend))))
