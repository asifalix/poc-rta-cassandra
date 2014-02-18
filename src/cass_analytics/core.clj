(ns cass-analytics.core
  (:use clj-time.core
        clj-time.format)
  (:require [clojure.math.combinatorics :as combo]
            [clojure.string :as str]
            )
  (:import [me.prettyprint.hector.api.factory HFactory]
           me.prettyprint.cassandra.serializers.StringSerializer
           me.prettyprint.cassandra.model.thrift.ThriftCounterColumnQuery))
; put this in cassandra-cli to get started
; CREATE KEYSPACE cademo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
; create column family users_usage with column_type='Super' and default_validation_class=CounterColumnType and replicate_on_write=true;

(set! *warn-on-reflection* true)

; begin cassandra helpers

(defn get-cluster []
  (HFactory/getOrCreateCluster "Test Cluster","127.0.0.1:9160"))

(defn increment-counter [counter-key counter-field]
  (let [cluster (get-cluster)
        keyspace (HFactory/createKeyspace "cademo" cluster)
        string-serializer (StringSerializer/get)
        mutator (HFactory/createMutator keyspace string-serializer)
        counter-column (HFactory/createCounterColumn counter-field 1)
        counter-super-column (HFactory/createCounterSuperColumn "users" [counter-column] string-serializer string-serializer)]
    (.insertCounter mutator counter-key "users_usage" counter-super-column)))

(defn query-counter [counter-key counter-field]
  (let [cluster (get-cluster)
        keyspace (HFactory/createKeyspace "cademo" cluster)
        string-serializer (StringSerializer/get)
        cq (HFactory/createSuperSliceCounterQuery keyspace string-serializer string-serializer string-serializer)
        resultsupercols (-> cq 
                            (.setColumnNames (into-array String [counter-field]))
                            (.setColumnFamily "users_usage")
                            (.setRange "users" "users" false 1000)
                            (.setKey counter-key)
                            (.execute)
                            (.get)
                            (.getSuperColumns))]
    (.getValue (first (.getColumns (first resultsupercols))))))

; end cassandra helpers

(defn get-random-timestamp []
  (let [millis-in-day (* 24 60 60 1000)
        random-days (Math/round (* (Math/random) 365))
        random-millis (Math/round (* (Math/random) millis-in-day))
        start-date (date-time 2013 01 01)]
    (-> start-date (.plusDays random-days) (.plusMillis random-millis))))

(defn get-random-device-type []
  (name (rand-nth [:android :ios :symbian :wp :blackberry])))

(defn get-random-appid []
  (name (rand-nth [:appid1 :appid2 :appid3])))

(defn get-random-device-version []
  (str (+ 1 (rand-nth (range 10))) "." (+ 1 (rand-nth (range 10)))))

(defn get-random-country []
  (name (rand-nth [:uk :us :in :jp :de])))

(defn random-event [] 
  {:timestamp (get-random-timestamp)
   :app-id (get-random-appid)
   :device-type (get-random-device-type)
   :device-version (get-random-device-version)
   :geo (get-random-country)})

(defn get-index-combinations []
  (let [interested-keys [:app-id :device-type :device-version :geo]]
    (combo/subsets interested-keys)))

(defn get-row-key [event index-keys]
  (let [datestr (unparse (formatter "yyyyMMdd") (:timestamp event))
        keyvals (map #(event %) index-keys)]
    (str/join "##" (into [datestr] keyvals))))

(defn get-column-name [event]
  (str "hour" (unparse (formatter "HH") (:timestamp event))))

(defn -main []
  (let [index-combinations (get-index-combinations)]
    (doseq [evt (repeatedly 10000000 random-event)]
      (let [row-keys (map #(get-row-key evt %) index-combinations)
            column-name (get-column-name evt)]
        (dorun
          (map #(increment-counter % column-name) row-keys))
        (dorun 
          (map #(increment-counter % "total") row-keys))))))

