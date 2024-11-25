(ns plooney81.nectar.sql
  (:gen-class)
  (:require [plooney81.nectar.jsql :as jsql]
            [plooney81.nectar.sql.expression]
            [plooney81.nectar.sql.impl :as impl]
            [plooney81.nectar.sql.select]
            [plooney81.nectar.sql.select-item]
            [plooney81.nectar.sql.set-operation])
  (:import (net.sf.jsqlparser.statement.select Select)))

(defmethod impl/jsql->honey-adapter :default [_honey jsql]
  (throw (IllegalArgumentException.
           (str "Unsupported type: For jsql->honey-adapter " (.getClass jsql)))))

(defmethod impl/jsql->honey-adapter Select [honey jsql]
  (impl/select->honey honey jsql))

(defn ripen
  "Process of turning nectar into honey. Accepts a raw-sql string and returns a honeysql map."
  [raw-sql]
  (impl/jsql->honey-adapter {} (jsql/to-nectar raw-sql)))

(defn -main [& args]
  (let [query (first args)]
    (if query
      (println (ripen query))
      (println "Please provide a query string as an argument."))))



(comment

  (ripen
    "SELECT *
       FROM people
      WHERE age > 25
   ORDER BY age DESC")
  (defn around-the-horn [sql-string]
    (-> (ripen sql-string)
        (honey/format {:inline true :pretty true})))


  (-> {:select [:*], :from [:orders], :where [:not-between :amount 100 500]}
      (honey/format {:inline true}))
  (-> (str "SELECT *\nFROM orders\nWHERE status IN ('pending', 'shipped', 'delivered') AND status NOT IN ('bloop', 'skoop')")
      (ripen)
      #_(around-the-horn)
      )
  (-> (str "SELECT * FROM orders WHERE NOT something")
      (ripen)
      #_(around-the-horn))
  (-> (str "SELECT * FROM employees JOIN departments USING (department_id)")
      (ripen)
      #_(around-the-horn))

  )
