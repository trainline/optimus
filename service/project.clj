;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(defproject com.trainline/optimus (-> "../ver/optimus.version" slurp .trim)
  :description "An API for storing and loading data with transactional semantics"
  :url "https://github.thetrainline.com/DataScience/optimus"
  :license {:name "All rights reserved"}

  :dependencies [[org.clojure/clojure "1.8.0"]
                 ;; TODO: Uncomment as required.
                 [metosin/compojure-api "1.1.10"]
                 [aleph "0.4.1"]

                 ;; aws sdk
                 [amazonica "0.3.81"
                  :exclusions [com.amazonaws/aws-java-sdk
                               com.amazonaws/amazon-kinesis-client]]
                 [com.amazonaws/aws-java-sdk-core "1.11.63"
                  :exclusions [commons-logging]]
                 [com.amazonaws/aws-java-sdk-dynamodb "1.11.63"
                  :exclusions [commons-logging]]

                 ;; logging
                 [com.taoensso/timbre "4.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [com.fzakaria/slf4j-timbre "0.3.2"]
                 [org.slf4j/log4j-over-slf4j "1.7.14"]
                 [org.slf4j/jul-to-slf4j "1.7.14"]
                 [org.slf4j/jcl-over-slf4j "1.7.14"]

                 ;; other libraries
                 [prismatic/schema "1.1.3"]
                 [com.brunobonacci/where "0.5.0"]
                 [com.brunobonacci/safely "0.3.0"
                  :exclusions [samsara/trackit-core]]
                 [cheshire "5.6.3"]
                 [samsara/trackit-core "0.6.0"]
                 [amalloy/ring-gzip-middleware "0.1.3"]
                 [com.taoensso/nippy "2.13.0"]]

  :profiles {:uberjar {:aot :all}
             :dev {:plugins [[lein-midje "3.2"]
                             [lein-binplus "0.6.2"]]
                   :source-paths ["dev" "src"]
                   :dependencies [[midje "1.9.0-alpha3"]]}}

  :bin {:name "optimus"
        :bootclasspath false
        :jvm-opts ["-server" "-Dfile.encoding=utf-8" "$JVM_OPTS" ]}


  :resource-paths ["resources" "../ver" ]

  :aliases {"itest" ["midje" ":config" ".midje_int.clj"]}

  :main optimus.service.main

  :repositories
  [["snapshots" {:url "https://artifactory.corp.local/artifactory/maven-datascience-snapshots"
                 :snapshots true
                 :sign-releases false}]
   ["releases" {:url "https://artifactory.corp.local/artifactory/maven-datascience-releases"
                :snapshots false
                :sign-releases false}]])
