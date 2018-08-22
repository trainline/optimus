;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(ns optimus.service.backends.queue-test
  (:require [midje.sweet :refer :all]
            [optimus.service.backend :refer :all]
            [optimus.service.backends.inmemory :refer [in-memory-queue]]
            [optimus.service.backends.dynamodb-queue :refer
             [dynamo-queue DEFAULT-CONFIG]]
            [safely.core :refer [safely]]))


(defn- uuid []
  (.toString (java.util.UUID/randomUUID)))

(def ^:const TEST_LEASE_TIME 1000) ;; for testing purposes

(defn- wait-lease-expiration []
  (safely
   (Thread/sleep (* 2 TEST_LEASE_TIME))
   :on-error
   :default nil))

(defn compatibility-tests [queue]


  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;;                                                                            ;;
  ;;           ---==| R E S E V E - N E X T - M E S S A G E ! |==----           ;;
  ;;                                                                            ;;
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  (fact "queue: send-message! should be able to add a message to the queue"

         (let [topic (uuid)]
           (send-message! queue topic {:value 1})
           (->> (list-messages! queue {:topic topic}) (map :message)) => (contains {:value 1})))


  (fact "queue: reseve-next-message! should lock the next available message for the given process"

        (let [topic (uuid)]
          (send-message! queue topic {:value 1})
          (reserve-next-message! queue topic "pid1")
           (->> (list-messages! queue {:status :reserved :topic topic :pid "pid1"})
                (map :message)) => [{:value 1}]))


  (fact "queue: reseve-next-message! other process should NOT obtain the same message"

        (let [topic (uuid)]
          (send-message! queue topic {:value 1})
          (send-message! queue topic {:value 2})
          (reserve-next-message! queue topic "pid1")
          (reserve-next-message! queue topic "pid2")

          (->> (list-messages! queue {:status :reserved :topic topic :pid "pid1"})
               (map :message)) => [{:value 1}]

          (->> (list-messages! queue {:status :reserved :topic topic :pid "pid2"})
               (map :message)) => [{:value 2}]))


  (fact "queue: reseve-next-message! other process should NOT obtain the same message not even when the same pid is requesting"

        (let [topic (uuid)]
          (send-message! queue topic {:value 1})
          (send-message! queue topic {:value 2})
          (reserve-next-message! queue topic "pid1")
          (reserve-next-message! queue topic "pid1")

          (->> (list-messages! queue {:status :reserved :topic topic :pid "pid1"})
               (map :message)) => [{:value 1} {:value 2}]
          ))


  (fact "queue: reseve-next-message! should fail when no messages are available"

        (let [topic (uuid)]
          ;; empty queue
          (reserve-next-message! queue topic "pid1") => (throws Exception "No messages found.")

          ;; already reserved
          (send-message! queue topic {:value 1})
          (reserve-next-message! queue topic "pid1") ;; ok

          ;; fail - no more messages
          (reserve-next-message! queue topic "pid1") => (throws Exception "No messages found.")

          ))


  (fact "queue: reseve-next-message! should re-assign a message with a expired lease." :slow

        ;; same process "pid"
        (let [topic (uuid)]
          (send-message! queue topic {:value 1})
          ;; ok
          (-> (reserve-next-message! queue topic "pid1") :message) => {:value 1}

          ;; fail - no more messages
          (reserve-next-message! queue topic "pid1") => (throws Exception "No messages found.")

          (wait-lease-expiration)

          (-> (reserve-next-message! queue topic "pid1") :message) => {:value 1}

          )

        ;; different process "pid"
        (let [topic (uuid)]
          (send-message! queue topic {:value 1})
          ;; ok
          (-> (reserve-next-message! queue topic "pid1") :message) => {:value 1}

          ;; fail - no more messages
          (reserve-next-message! queue topic "pid2") => (throws Exception "No messages found.")

          (wait-lease-expiration)

          (-> (reserve-next-message! queue topic "pid2") :message) => {:value 1}

          )

        )


  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;;                                                                            ;;
  ;;           ---==| A C K N O W L E D G E - M E S S A G E ! |==----           ;;
  ;;                                                                            ;;
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  (fact "queue: acknowledge-message! should mark a message as completed and not longer processable"

        (let [topic (uuid)]
          (send-message! queue topic {:value 1})

          ;; ok
          (let [rmsg (reserve-next-message! queue topic "pid1")]
            (:message rmsg) => {:value 1}
            (acknowledge-message! queue (:id rmsg) "pid1") => :ok)

          ;; fail - no more messages
          (reserve-next-message! queue topic "pid2") => (throws Exception "No messages found.")
          ))



  (fact "queue: acknowledge-message! should fail when trying to ack a message which is not owned by the given process"

        (let [topic (uuid)]
          (send-message! queue topic {:value 1})

          (let [rmsg (reserve-next-message! queue topic "pid1")]
            (:message rmsg) => {:value 1}
            ;; different pid
            (acknowledge-message! queue (:id rmsg) "pid2") => (throws Exception #"you don't own the lease")
            )
          ))


  (fact "queue: acknowledge-message! should fail when trying to ack a message on which the lease is expired" :slow

        (let [topic (uuid)]
          (send-message! queue topic {:value 1})

          (let [rmsg (reserve-next-message! queue topic "pid1")]
            (:message rmsg) => {:value 1}

            (wait-lease-expiration)

            (acknowledge-message! queue (:id rmsg) "pid1") => (throws Exception #"Lease expired")
            )
          ))


  (fact "queue: acknowledge-message! should fail when trying to ack a message which doesn't exists"

        (let [topic (uuid)]
          (send-message! queue topic {:value 1})

          (let [rmsg (reserve-next-message! queue topic "pid1")]
            (:message rmsg) => {:value 1}

            ;; unknown message
            (acknowledge-message! queue (uuid) "pid1") => (throws Exception #"Message not found")
            )))



  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;;                                                                            ;;
  ;;          ---==| E X T E N D - M E S S A G E - L E A S E ! |==----          ;;
  ;;                                                                            ;;
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  (fact "queue: extend-message-lease! should extend the lease time on a previously reserved message"

        (let [topic (uuid)]
          (send-message! queue topic {:value 1})

          ;; ok
          (let [rmsg (reserve-next-message! queue topic "pid1")]
            (:message rmsg) => {:value 1}
            (extend-message-lease! queue (:id rmsg) "pid1") => :ok

            (let [xmsg (first (list-messages! queue {:topic topic :status :reserved}))]
              (> (:lease xmsg) (:lease rmsg)))
            )))


  (fact "queue: extend-message-lease! should fail when the previous lease is exipred" :slow

        (let [topic (uuid)]
          (send-message! queue topic {:value 1})

          ;; ok
          (let [rmsg (reserve-next-message! queue topic "pid1")]
            (:message rmsg) => {:value 1}

            (wait-lease-expiration)

            (extend-message-lease! queue (:id rmsg) "pid1") => (throws Exception #"Lease expired")
            )))


  (fact "queue: extend-message-lease! should fail when the lease is owned by another process"

        (let [topic (uuid)]
          (send-message! queue topic {:value 1})

          ;; ok
          (let [rmsg (reserve-next-message! queue topic "pid1")]
            (:message rmsg) => {:value 1}

            ;; different pid
            (extend-message-lease! queue (:id rmsg) "pid2") => (throws Exception "Cannot extend the lease for a message for which you don't own.")
            )))


  (fact "queue: extend-message-lease! should fail when the lease is already acknowledged."

        (let [topic (uuid)]
          (send-message! queue topic {:value 1})

          ;; ok
          (let [rmsg (reserve-next-message! queue topic "pid1")]
            (:message rmsg) => {:value 1}
            (acknowledge-message! queue (:id rmsg) "pid1")

            (extend-message-lease! queue (:id rmsg) "pid1") => (throws Exception #"Message already acknowledged")
            )))



  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;;                                                                            ;;
  ;;                 ---==| L I S T - M E S S A G E S ! |==----                 ;;
  ;;                                                                            ;;
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

  (fact "queue: list-messages! should be able to display new messages in the given topic"

        (let [topic (uuid)]
          (send-message! queue topic {:value 1})
          (send-message! queue topic {:value 2})

          (->> (list-messages! queue {:topic topic}) (map :message)) => [{:value 1} {:value 2}]
          (->> (list-messages! queue {:topic topic :status :new}) (map :message)) => [{:value 1} {:value 2}]
          (->> (list-messages! queue {:topic topic :status :all}) (map :message)) => [{:value 1} {:value 2}]

          ;; avoid topic mixup
          (list-messages! queue {:topic "another-topic"}) => []

          )

        (let [topic1 (uuid)
              topic2 (uuid)]
          (send-message! queue topic1 {:value 1})
          (send-message! queue topic2 {:value 2})

          ;; avoid topic mixup
          (->> (list-messages! queue {:topic topic1}) (map :message)) => [{:value 1}]
          (->> (list-messages! queue {:topic topic2}) (map :message)) => [{:value 2}]

          ))


  (fact "queue: list-messages! should be able to display reserved messages in a given topic"

        (let [topic (uuid)]
          (send-message! queue topic {:value 1})
          (send-message! queue topic {:value 2})
          (reserve-next-message! queue topic "pid1")

          (->> (list-messages! queue {:topic topic :status :reserved}) (map :message)) => [{:value 1}]
          (->> (list-messages! queue {:topic topic :status :all}) (map :message)) => [{:value 1} {:value 2}]

          ))



  (fact "queue: list-messages! should be able to display expired messages in a given topic" :slow

        (let [topic (uuid)]
          (send-message! queue topic {:value 1})
          (send-message! queue topic {:value 2})
          (reserve-next-message! queue topic "pid1")

          (wait-lease-expiration)

          (->> (list-messages! queue {:topic topic :status :reserved}) (map :message)) => []
          (->> (list-messages! queue {:topic topic :status :all}) (map :message)) => [{:value 1} {:value 2}]
          (->> (list-messages! queue {:topic topic :status :expired}) (map :message)) => [{:value 1}]

          ))



  (fact "queue: list-messages! should be able to display acknowledged messages in a given topic"

        (let [topic (uuid)]
          (send-message! queue topic {:value 1})
          (send-message! queue topic {:value 2})

          (as-> (reserve-next-message! queue topic "pid1") $
            (:id $)
            (acknowledge-message! queue $ "pid1"))


          (->> (list-messages! queue {:topic topic :status :reserved}) (map :message)) => []
          (->> (list-messages! queue {:topic topic :status :all}) (map :message)) => [{:value 1} {:value 2}]
          (->> (list-messages! queue {:topic topic :status :acknowledged}) (map :message)) => [{:value 1}]

          ))


  (fact "queue: list-messages! should be able to display messages reserved by pid"

        (let [topic (uuid)]
          (send-message! queue topic {:value 1})
          (send-message! queue topic {:value 2})

          (reserve-next-message! queue topic "pid1")
          (reserve-next-message! queue topic "pid2")

          (->> (list-messages! queue {:topic topic :status :reserved}) (map :message)) => [{:value 1} {:value 2}]
          (->> (list-messages! queue {:topic topic :status :reserved :pid "pid1"}) (map :message)) => [{:value 1}]
          (->> (list-messages! queue {:topic topic :status :reserved :pid "pid2"}) (map :message)) => [{:value 2}]
          (->> (list-messages! queue {:topic topic :status :all}) (map :message)) => [{:value 1} {:value 2}]
          ))

    )



(facts "(*) compatibility tests for: in-memory-queue"
       (compatibility-tests
        (in-memory-queue {:lease-time TEST_LEASE_TIME})))


(facts "(*) compatibility tests for: dynamo-queue" :integration
       (compatibility-tests
        (dynamo-queue
         (merge DEFAULT-CONFIG {:task-queue-table "OptimusTasksQ-Test"
                                :lease-time TEST_LEASE_TIME}))))
