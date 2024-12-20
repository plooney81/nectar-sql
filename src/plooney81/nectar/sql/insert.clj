(ns plooney81.nectar.sql.insert
  (:require [honey.sql.helpers :as sql]
            [plooney81.nectar.jsql :as jsql]
            [plooney81.nectar.sql.helpers :as helpers]
            [plooney81.nectar.sql.impl :as impl]
            [plooney81.nectar.sql.select :as select])
  (:import (net.sf.jsqlparser.statement.insert Insert)
           (net.sf.jsqlparser.statement.select Values)))

(defn get-columns [jsql-insert]
  (when-let [columns (.getColumns jsql-insert)]
    (->> columns
         (mapv helpers/convert-column))))

(defn handle-into [honey jsql-insert]
  (if-let [table (.getTable jsql-insert)]
    (if-let [columns (get-columns jsql-insert)]
      (sql/insert-into honey (jsql/convert-table table) columns)
      (sql/insert-into honey (jsql/convert-table table)))
    honey))

(defn handle-columns [honey jsql-insert]
  (if-let [columns (get-columns jsql-insert)]
    (apply sql/columns honey columns)
    honey))

(defn convert-values [^Insert jsql-insert]
  (when-let [values (.getValues jsql-insert)]
    (let [converted-values   (->> values
                                  (.getExpressions)
                                  (mapv impl/expression->honey))
          columns-count      (count (get-columns jsql-insert))
          one-row-of-values? (<= (/ columns-count (count converted-values)) 1)]
      (if one-row-of-values?
        [converted-values]
        converted-values))))

(defn handle-values [honey jsql-insert]
  (if-let [values (convert-values jsql-insert)]
    (sql/values honey values)
    honey))

(defmethod impl/insert->honey Insert [honey ^Insert jsql-insert]
  (def my-jsql-insert jsql-insert)
  (-> honey
      (handle-into jsql-insert)
      #_(handle-columns jsql-insert)
      (handle-values jsql-insert))

  )

(comment

  (->> my-jsql-insert
       .getTable
       (jsql/convert-table))
   (->> my-jsql-insert
        .getColumns
        (map helpers/convert-column))
  (->> my-jsql-insert
       .getValues
       .getExpressions
       #_(mapv impl/expression->honey))

  (->> my-jsql-insert
       #_(map helpers/convert-column))

  )


(defmethod impl/insert->honey :default [_honey jsql-insert]
  (throw (IllegalArgumentException.
           (str "Unsupported Insert type: " (.getClass jsql-insert)))))
