(ns plooney81.delete-test
  (:require [clojure.test :refer :all]
            [plooney81.test-helpers :as th]))

(deftest simple-deletes
  (th/test-nectar
    "delete without where"
    "DELETE FROM users"
    {:delete-from [:users]})
  (th/test-nectar
    "delete with where"
    (str "DELETE FROM users\n"
         "WHERE id = 1")
    {:delete-from [:users]
     :where       [:= :id 1]})
  (th/test-nectar
    "delete with compound where"
    (str "DELETE FROM users\n"
         "WHERE (active = 0) AND (age < 18)")
    {:delete-from [:users]
     :where       [:and [:= :active 0] [:< :age 18]]})
  (th/test-nectar
    "delete with order by and limit"
    (str "DELETE FROM users\n"
         "WHERE active = 0\n"
         "ORDER BY id ASC\n"
         "LIMIT 10")
    {:delete-from [:users]
     :where       [:= :active 0]
     :order-by    [[:id :asc]]
     :limit       10}))
