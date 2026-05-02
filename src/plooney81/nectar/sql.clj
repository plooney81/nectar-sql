(ns plooney81.nectar.sql
  (:require [honey.sql :as honey]
            [honey.sql.pg-ops]
            [plooney81.nectar.jsql :as jsql]
            [plooney81.nectar.sql.expression]
            [plooney81.nectar.sql.impl :as impl]
            [plooney81.nectar.sql.insert]
            [plooney81.nectar.sql.select]
            [plooney81.nectar.sql.select-item]
            [plooney81.nectar.sql.set-operation]
            [plooney81.nectar.sql.update])
  (:import (net.sf.jsqlparser.statement.insert Insert)
           (net.sf.jsqlparser.statement.select Select)
           (net.sf.jsqlparser.statement.update Update)))

(defmethod impl/jsql->honey-adapter :default [_honey jsql]
  (throw (IllegalArgumentException.
           (str "Unsupported type: For jsql->honey-adapter " (.getClass jsql)))))

(defmethod impl/jsql->honey-adapter Select [honey jsql]
  (impl/select->honey honey jsql))

(defmethod impl/jsql->honey-adapter Insert [honey jsql]
  (impl/insert->honey honey jsql))

(defmethod impl/jsql->honey-adapter Update [honey jsql]
  (impl/update->honey honey jsql))

(defn ripen
  "Process of turning nectar into honey. Accepts a raw-sql string and returns a honeysql map."
  [raw-sql]
  (impl/jsql->honey-adapter {} (jsql/to-nectar raw-sql)))

(comment
  (do
    (require '[honey.sql :as honey])
    (defn around-the-horn [sql-string]
      (-> (plooney81.nectar.sql/ripen sql-string)
          (honey/format {:inline true :pretty true}))))

  (around-the-horn "SELECT json_column -> 'name' -> 'another'")

  (around-the-horn "INSERT INTO users AS u (id, username, email) VALUES (1, 'john_doe', 'john@example.com')")

  (around-the-horn "INSERT INTO users AS u (id, username, email) VALUES (1, 'john_doe', 'john@example.com'), (2, 'admin', 'admin@example.com')")

  (around-the-horn "SELECT CASE WHEN title_ref.title_id IS NOT NULL THEN title_ref.title_name ELSE person.manual_title END AS title FROM person LEFT JOIN title_reference title_ref ON person.title_id = title_ref.title_id;")

  (around-the-horn "SELECT DISTINCT col_name COLLATE latin1_bin FROM X")

  (around-the-horn "WITH stuff AS MATERIALIZED (SELECT * FROM table) SELECT * FROM stuff")

  (ripen "WITH stuff AS MATERIALIZED (SELECT * FROM table) SELECT * FROM stuff")

  )
