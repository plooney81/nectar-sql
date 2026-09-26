(ns plooney81.test-helpers
  "Helper functions to be used in other testing namespaces"
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [honey.sql :as honey]
            [honey.sql.helpers :as h]
            [plooney81.nectar.sql :as nsql]))

(defn honey->text
  ([honeysql] (honey->text honeysql {}))
  ([honeysql format-opts]
   (-> (honey/format honeysql (merge {:inline true :pretty true} format-opts))
       first
       str/trim)))

(defn eval-helpers
  "Evaluates a generated helper form in this namespace, where `h` is aliased
   to `honey.sql.helpers`."
  [form]
  (binding [*ns* (the-ns 'plooney81.test-helpers)]
    (eval form)))

(defn test-nectar
  "Checks that `raw-sql` ripens into `expected-honey`, and that formatting that
   honey gets us back to the original `raw-sql`. Also checks that the helper
   output (both the form and the pretty-printed text) evaluates to that same honey.

   `format-opts` is merged into the honeysql format options, which queries
   carrying parameters need in order to format at all (`{:params {...}}`)."
  ([description raw-sql expected-honey]
   (test-nectar description raw-sql expected-honey {}))
  ([description raw-sql expected-honey format-opts]
   (let [nectar                 (nsql/ripen raw-sql)
         {:keys [form text]}    (nsql/ripen raw-sql {:output :helpers})]
     (testing description
       ;; tests that ripen outputs expected-honey
       (is (= nectar expected-honey))
       ;; tests that converting the nectar back to raw-sql gets us our original raw-sql
       (is (= (honey->text nectar format-opts) raw-sql))
       ;; tests that the helper form builds the same honey
       (is (= (eval-helpers form) expected-honey) (str "helper form: " (pr-str form)))
       ;; tests that the pretty-printed helper text round-trips too
       (is (= (eval-helpers (read-string text)) expected-honey) (str "helper text:\n" text))))))
