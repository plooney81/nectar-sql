(ns plooney81.helper-form-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [plooney81.nectar.sql :as nsql]
            [plooney81.test-helpers :as th]))

(deftest ripen-output-option
  (testing "ripen still returns the plain honey map by default"
    (is (= {:select [:a] :from [:t]} (nsql/ripen "SELECT a FROM t")))
    (is (= {:select [:a] :from [:t]} (nsql/ripen "SELECT a FROM t" {})))
    (is (= {:select [:a] :from [:t]} (nsql/ripen "SELECT a FROM t" {:output :map}))))
  (testing ":output :helpers returns the form and its text"
    (let [{:keys [form text]} (nsql/ripen "SELECT a FROM t" {:output :helpers})]
      (is (= '(-> (h/select :a) (h/from :t)) form))
      (is (= (str ";; (:require [honey.sql.helpers :as h])\n"
                  "(-> (h/select :a) (h/from :t))\n")
             text)))))

(deftest ripen-unknown-output
  (testing "an unrecognised :output fails rather than silently returning the map"
    (is (thrown? AssertionError (nsql/ripen "SELECT a FROM t" {:output :helper})))))

(deftest ripen-both-output
  (testing ":output :both returns the map alongside the helper output"
    (let [sql    "SELECT a FROM t WHERE b = 1"
          result (nsql/ripen sql {:output :both :alias 'sql})]
      (is (= #{:map :form :text} (set (keys result))))
      (is (= (nsql/ripen sql) (:map result)))
      (is (= (nsql/ripen sql {:output :helpers :alias 'sql})
             (dissoc result :map))))))

(deftest issue-example
  (let [{:keys [form text]} (nsql/ripen "SELECT a, b FROM t WHERE (x = ?) ORDER BY a DESC"
                                        {:output :helpers})]
    (is (= '(-> (h/select :a :b)
                (h/from :t)
                (h/where [:= :x :?p1])
                (h/order-by [:a :desc]))
           form))
    (is (= (str ";; (:require [honey.sql.helpers :as h])\n"
                "(-> (h/select :a :b)\n"
                "    (h/from :t)\n"
                "    (h/where [:= :x :?p1])\n"
                "    (h/order-by [:a :desc]))\n")
           text))))

(deftest nested-statements
  (testing "subqueries and CTEs become nested helper chains"
    (is (= '(-> (h/with [:stuff [:materialized (-> (h/select :*) (h/from :tbl))]])
                (h/select :s.a)
                (h/from [:stuff :s])
                (h/inner-join :c [:= :c.id :s.id])
                (h/where [:in :s.a (-> (h/select :a) (h/from :z))]))
           (:form (nsql/ripen (str "WITH stuff AS MATERIALIZED (SELECT * FROM tbl) "
                                   "SELECT s.a FROM stuff s INNER JOIN c ON c.id = s.id "
                                   "WHERE s.a IN (SELECT a FROM z)")
                              {:output :helpers})))))
  (testing "set operations take their member statements as arguments"
    (is (= '(h/union-all (-> (h/select :a) (h/from :t))
                         (-> (h/select :b) (h/from :u)))
           (:form (nsql/ripen "SELECT a FROM t UNION ALL SELECT b FROM u"
                              {:output :helpers})))))
  (testing "INSERT ... SELECT leads with insert-into"
    (is (= '(-> (h/insert-into :users [:id])
                (h/select :id)
                (h/from :old))
           (:form (nsql/ripen "INSERT INTO users (id) SELECT id FROM old"
                              {:output :helpers}))))))

(deftest honey->helpers
  (testing "accepts any honey map, not only ones from ripen"
    (is (= '(-> (h/select :a) (h/from :t) (h/limit 10))
           (:form (nsql/honey->helpers {:select [:a] :from [:t] :limit 10})))))
  (testing "custom alias"
    (let [{:keys [form text]} (nsql/honey->helpers {:select [:a] :from [:t]} {:alias 'sql})]
      (is (= '(-> (sql/select :a) (sql/from :t)) form))
      (is (str/starts-with? text ";; (:require [honey.sql.helpers :as sql])\n"))))
  (testing "a chain that would start with a map argument is seeded with {}"
    (let [honey {:set {:a 1}}
          form  (:form (nsql/honey->helpers honey))]
      (is (= '(-> {} (h/set {:a 1})) form))
      (is (= honey (th/eval-helpers form)))))
  (testing "clauses without a helper fall back to merge"
    (let [honey {:select [:a] :not-a-clause 1}
          form  (:form (nsql/honey->helpers honey))]
      (is (= '(-> (h/select :a) (merge {:not-a-clause 1})) form))
      (is (= honey (th/eval-helpers form)))))
  (testing "keywords that can't be read back are written as keyword calls"
    (let [honey {:select [[[(keyword "@>") :j "x"]]]}
          text  (:text (nsql/honey->helpers honey))]
      (is (str/includes? text "(keyword \"@>\")"))
      (is (= honey (th/eval-helpers (read-string text)))))))
