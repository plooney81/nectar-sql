(ns plooney81.insert-test
  (:require [clojure.test :refer :all]
            [plooney81.test-helpers :as th]))

(deftest simple-inserts
  (th/test-nectar
    "simple-insert"
    (str "INSERT INTO users AS u (id, username, email)\n"
         "VALUES (1, 'john_doe', 'john@example.com')")
    {:insert-into [[:users :u] [:id :username :email]]
     :values      [[1 "john_doe" "john@example.com"]]})
  (th/test-nectar
    "multi row insert"
    (str "INSERT INTO users AS u (id, username, email)\n"
         "VALUES (1, 'john_doe', 'john@example.com'), (2, 'admin', 'admin@example.com')")
    {:insert-into [[:users :u] [:id :username :email]]
     :values      [[1 "john_doe" "john@example.com"] [2 "admin" "admin@example.com"]]})
  )
