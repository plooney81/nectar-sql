(ns plooney81.update-test
  (:require [clojure.test :refer :all]
            [plooney81.nectar.sql :as nsql]
            [plooney81.test-helpers :as th]))

(deftest simple-updates
  (th/test-nectar
    "basic update with where"
    (str "UPDATE users\n"
         "SET name = 'John'\n"
         "WHERE id = 1")
    {:update :users
     :set    {:name "John"}
     :where  [:= :id 1]})
  (th/test-nectar
    "update without where"
    (str "UPDATE users\n"
         "SET active = 0")
    {:update :users
     :set    {:active 0}})
  (th/test-nectar
    "update multi-column set"
    (str "UPDATE users\n"
         "SET name = 'John', age = 30")
    {:update :users
     :set    {:name "John" :age 30}})
  (th/test-nectar
    "update with aliased table and FROM"
    (str "UPDATE users u\n"
         "SET u.name = 'John'\n"
         "FROM orders AS o\n"
         "WHERE u.id = o.user_id")
    {:update [:users :u]
     :set    {:u.name "John"}
     :from   [[:orders :o]]
     :where  [:= :u.id :o.user_id]})
  (th/test-nectar
    "update with order by and limit"
    (str "UPDATE users\n"
         "SET name = 'John'\n"
         "ORDER BY id ASC\n"
         "LIMIT 5")
    {:update   :users
     :set      {:name "John"}
     :order-by [[:id :asc]]
     :limit    5}))
