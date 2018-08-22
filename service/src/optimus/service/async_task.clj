;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.async-task
  "This namespace contains functions required to setup and run the background
   task which handles events published by the core namespace."
  (:require [clojure.tools.logging :as log]
            [optimus.service
             [backend :as b]
             [core :as ms]
             [util :as u :refer
              [load-function-from-name metrics-name stoppable-thread]]]
            [optimus.service.backends.inmemory :as mem]
            [amazonica.aws.dynamodbv2 :as dyn]
            [safely.core :refer [safely safely-fn]]
            [schema.core :as s]
            [samsara.trackit :refer :all]
            [clojure.string :as str])
  (:import [com.amazonaws.services.dynamodbv2.model
            ProvisionedThroughputExceededException
            LimitExceededException
            InternalServerErrorException
            ItemCollectionSizeLimitExceededException
            ResourceInUseException]))


(defmulti handle-message
  "Dispatches to the handler corresponding to the :action parameter
   in the message"
  (fn [_ msg _] (keyword (:action msg))))



(defmulti verify-data
  "Dispatches to a multimethod based on the :strategy and :type of the
  kv store. Implementors must return a tuple with the folllowing values:
  [status {:reason \"actual count did not match expected\" :expected
  300 :actual 10000}]"
  (fn [{:keys [config] :as bknd-store} version _]
    [(keyword (get-in version [:verification-policy :strategy]))
     (get-in config  [:kv-store :type])]))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                       ---==| H A N D L E R S |==----                       ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                        ---==| P R E P A R E |==----                        ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Handler for :prepare action. Transitions the version from
;; :preparing -> :awaiting-entries. This function will be extended to perform
;; operations like adding more nodes, so that the API can handle the load.
(defmethod handle-message :prepare
  [{:keys [queue meta-store kv-store]} {:keys [version-id]} extend-lease-fn]
  ;;TODO: Implement later actual steps to prepare dataset
  (when extend-lease-fn (extend-lease-fn))
  (let [version (b/get-version meta-store version-id)]
    (b/update-status meta-store version-id :awaiting-entries {})))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                           ---==| S A V E |==----                           ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Handler for :save action. Transitions the version from :saving -> :saved.
(defmethod handle-message :save
  [{:keys [meta-store queue config]} {:keys [version-id]} extend-lease-fn]
  (let [{:keys [verification-policy] :as version}
        (b/get-version meta-store version-id)]
    ;;TODO Verification is disabled until a better solutions is implemented.
    (b/update-status meta-store version-id :saved {})))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;              ---==| D A T A   V E R I F I C A T I O N |==----              ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmethod handle-message :verify-data
  [{:keys [meta-store queue config] :as bknd}
   {:keys [version] :as message}
   extend-lease-fn]
  (let [{:keys [id verification-policy]} version
        [status reason] (verify-data bknd version extend-lease-fn)
        topic (get-in config [:async-task :operations-topic])]
    (extend-lease-fn)
    (b/send-message! queue
                     topic
                     {:action (if status :saved :fail)
                      :version-id id
                      :reason reason})))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                          ---==| S A V E D |==----                          ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmethod handle-message :saved
  [{:keys [meta-store]} {:keys [version-id reason]} extend-lease-fn]
  (b/update-status meta-store version-id :saved reason))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                           ---==| F A I L |==----                           ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmethod handle-message :fail
  [{:keys [meta-store]} {:keys [version-id reason]} extend-lease-fn]
  (b/update-status meta-store version-id :failed reason))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                        ---==| P U B L I S H |==----                        ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- revert-published-version
  "Changes the current published version(s) for the given dataset to :saved"
  [meta-store dsname]
  (->>  (b/get-versions meta-store dsname)
        (filter #(= (:status %) :published))
        (map #(b/update-status meta-store (:id %) :saved
                               {:initiated-by
                                "async_task/handle_message/:publish"}))
        doall))



;; Handler for :publish action. Transitions the version from :publishing ->
;; :published.
(defmethod handle-message :publish
  [{:keys [queue meta-store kv-store]} {:keys [version-id]} extend-lease-fn]
  ;;TODO: Implement later actual steps to save dataset
  (let [version (b/get-version meta-store version-id)
        all-published (b/get-versions meta-store (:dataset version))]
    ;;set current published version(s) to :saved
    (revert-published-version meta-store (:dataset version))
    ;; set supplied version to :published
    (b/update-status meta-store version-id :published {})
    ;; set dataset.active-version to specified version-id
    (b/activate-version meta-store version-id)))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                     ---==| A S Y N C   T A S K |==----                     ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn process-message
  "Performs the following steps:

   1. Reserve next message - 'Safely' retries with exponential backoff until
      a valid message is obtained from the queue.
   2. Dispatch to the correct 'handle-message' function - also passes a fn
      that the handler can use to extend the lease for the message.
   3. Acknowledge message.

  Any exception that is thrown by steps 2 and 3 are logged (& ignored) and
  returns :error. Returns :ok on successful execution."
  [{:keys [queue meta-store kv-store pid] :as bknd-store} topic handler-fn]
  (when-let [{:keys [id message] :as msg}
             (b/reserve-next-message! queue topic pid)]
    (log/info "processor: got message: " (pr-str msg) " from topic: " topic)
    (let []
      (try
        (track-time
         (metrics-name :async-task (:action message))
         (handler-fn bknd-store message
                     (fn [] (b/extend-message-lease! queue id pid))))

        (catch Throwable t
          ;; I know what you are thinking..
          ;; I found the below log very useful during debugging.
          (log/error "Error handing message: " t)

          ;;track rate of errors
          (track-rate
           (metrics-name :async-task (:action message) :errors))

          (throw t))))
    (log/info "processor: handle-message complete. acknowledging message:"
              (pr-str msg))
    (b/acknowledge-message! queue id pid)))



(defn start-async-processor
  "Starts a thread that calls the handler-fn supplied
  periodically. This function returns a function which can be used to
  stop the thread."
  [bknd-store
   {:keys [poll-interval operations-topic handler-fn]}]
  (let [hf (load-function-from-name handler-fn)]
    (stoppable-thread "async processor"
                      #(process-message bknd-store operations-topic hf)
                      :sleep-time poll-interval)))



(defn stop-async-processor
  "Stops the async processor. The input should be the future returned by
   start-async-processor function."
  [stop-fn]
  (when stop-fn (stop-fn)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                   ---==| V E R I F I C A T I O N |==----                   ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmethod verify-data :default
  [_ _ _]
  [false {:reason :invalid-verification-policy}])
