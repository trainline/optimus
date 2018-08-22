;; Copyright (c) Trainline Limited, 2017. All rights reserved.
;; See LICENSE.txt in the project root for license information
(defproject com.trainline.optimus/loader (-> "../ver/optimus.version" slurp .trim)
  :description "Spark based component that parallely loads key value
  pairs to the Optimus API"
  :url "https://github.thetrainline.com/DataScience/optimus"
  :license {:name "All rights reserved"}

  :main optimus.loader.main

  :dependencies [[org.clojure/clojure "1.8.0"]

                 [clj-http "2.3.0"]
                 ;; spark
                 [gorillalabs/sparkling "1.2.5" ]

                 ;; logging
                 [com.taoensso/timbre "4.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.22"]

                 [com.fzakaria/slf4j-timbre "0.3.2"]
                 [org.slf4j/log4j-over-slf4j "1.7.14"]
                 [org.slf4j/jul-to-slf4j "1.7.14"]
                 [org.slf4j/jcl-over-slf4j "1.7.14"]

                 [com.brunobonacci/safely "0.3.0"]
                 [org.clojure/tools.cli "0.3.5"]

                 [org.apache.spark/spark-core_2.10 "1.6.2"
                  :exclusions [commons-net
                               org.slf4j/slf4j-log4j12]]

                 [com.cognitect/transit-clj "0.8.300"]]

  :profiles {:provided {:dependencies
                        [[org.apache.spark/spark-core_2.10 "1.6.2"
                          :exclusions [commons-net
                                       org.slf4j/slf4j-log4j12]]]}
             :dev {:plugins [[lein-midje "3.2"]]
                   :dependencies [[midje "1.9.0-alpha3"]]}}

  :resource-paths ["resources" "../ver" ]

  :uberjar-merge-with {#"reference.conf$" [slurp str spit]}

  :uberjar-exclusions [#"META-INF/LICENSE/*" #"META-INF/license/*"]

  ;;;:aliases {"itest" ["midje" ":config" ".midje_int.clj"]}

  :aot [#".*" sparkling.serialization sparkling.destructuring]

  :repositories
  [["snapshots" {:url "https://artifactory.corp.local/artifactory/maven-datascience-snapshots"
                 :snapshots true
                 :sign-releases false}]
   ["releases" {:url "https://artifactory.corp.local/artifactory/maven-datascience-releases"
                :snapshots false
                :sign-releases false}]])
