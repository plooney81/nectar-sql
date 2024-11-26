(ns plooney81.nectar.sql
  (:require [honey.sql :as honey]
            [honey.sql.pg-ops]
            [plooney81.nectar.jsql :as jsql]
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

(comment
  (do
    (require '[honey.sql :as honey])
    (defn around-the-horn [sql-string]
      (-> (ripen sql-string)
          (honey/format {:inline true :pretty true}))))

  (-> (str "SELECT json_column - 'age'")
      (ripen)
      #_(around-the-horn))

  (honey/format {:select [[[:? :json_column "name"]]]} {:inline true :pretty true})
  (honey/format {:select [[:- :json_column "age"]]} {:inline true :pretty true})

  )
