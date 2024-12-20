(ns plooney81.nectar.sql
  (:require [honey.sql :as honey]
            [honey.sql.pg-ops]
            [plooney81.nectar.jsql :as jsql]
            [plooney81.nectar.sql.expression]
            [plooney81.nectar.sql.impl :as impl]
            [plooney81.nectar.sql.insert]
            [plooney81.nectar.sql.select]
            [plooney81.nectar.sql.select-item]
            [plooney81.nectar.sql.set-operation])
  (:import (net.sf.jsqlparser.statement.insert Insert)
           (net.sf.jsqlparser.statement.select Select)))

(defmethod impl/jsql->honey-adapter :default [_honey jsql]
  (throw (IllegalArgumentException.
           (str "Unsupported type: For jsql->honey-adapter " (.getClass jsql)))))

(defmethod impl/jsql->honey-adapter Select [honey jsql]
  (impl/select->honey honey jsql))

(defmethod impl/jsql->honey-adapter Insert [honey jsql]
  (impl/insert->honey honey jsql))

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

  (around-the-horn "SELECT json_column -> 'name' -> 'another'")

  (around-the-horn "INSERT INTO users AS u (id, username, email) VALUES (1, 'john_doe', 'john@example.com')")

  (around-the-horn "INSERT INTO users AS u (id, username, email) VALUES (1, 'john_doe', 'john@example.com'), (2, 'admin', 'admin@example.com')")

  )
