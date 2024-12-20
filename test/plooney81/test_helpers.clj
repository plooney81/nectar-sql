(ns plooney81.test-helpers
  "Helper functions to be used in other testing namespaces"
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [honey.sql :as honey]
            [plooney81.nectar.sql :as nsql]))

(defn honey->text [honeysql]
  (-> (honey/format honeysql {:inline true :pretty true})
      first
      str/trim))

(defn test-nectar [description raw-sql expected-honey]
  (let [nectar (nsql/ripen raw-sql)]
    (testing description
      ;; tests that ripen outputs expected-honey
      (is (= nectar expected-honey))
      ;; tests that converting the nectar back to raw-sql gets us our original raw-sql
      (is (= (honey->text nectar) raw-sql)))))