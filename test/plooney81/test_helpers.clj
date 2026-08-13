(ns plooney81.test-helpers
  "Helper functions to be used in other testing namespaces"
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [honey.sql :as honey]
            [plooney81.nectar.sql :as nsql]))

(defn honey->text
  ([honeysql] (honey->text honeysql {}))
  ([honeysql format-opts]
   (-> (honey/format honeysql (merge {:inline true :pretty true} format-opts))
       first
       str/trim)))

(defn test-nectar
  "Checks that `raw-sql` ripens into `expected-honey`, and that formatting that
   honey gets us back to the original `raw-sql`.

   `format-opts` is merged into the honeysql format options, which queries
   carrying parameters need in order to format at all (`{:params {...}}`)."
  ([description raw-sql expected-honey]
   (test-nectar description raw-sql expected-honey {}))
  ([description raw-sql expected-honey format-opts]
   (let [nectar (nsql/ripen raw-sql)]
     (testing description
       ;; tests that ripen outputs expected-honey
       (is (= nectar expected-honey))
       ;; tests that converting the nectar back to raw-sql gets us our original raw-sql
       (is (= (honey->text nectar format-opts) raw-sql))))))
