(ns plooney81.nectar.sql.insert
  (:require [honey.sql.helpers :as sql]
            [plooney81.nectar.jsql :as jsql]
            [plooney81.nectar.sql.helpers :as helpers]
            [plooney81.nectar.sql.impl :as impl])
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

(defn convert-values [^Insert jsql-insert]
  (when-let [values (.getValues jsql-insert)]
    (let [converted-values (->> values
                                (.getExpressions)
                                (mapv impl/expression->honey))
          columns-count    (count (get-columns jsql-insert))]
      (if (<= (/ columns-count (count converted-values)) 1)
        [converted-values]
        converted-values))))

(defn handle-values [honey jsql-insert]
  (if-let [values (convert-values jsql-insert)]
    (sql/values honey values)
    honey))

(defn- handle-body [honey ^Insert jsql-insert]
  (let [select-body (.getSelect jsql-insert)]
    (if (instance? Values select-body)
      (handle-values honey jsql-insert)
      (merge honey (impl/select->honey {} select-body)))))

(defmethod impl/insert->honey Insert [honey ^Insert jsql-insert]
  (-> honey
      (handle-into jsql-insert)
      (handle-body jsql-insert)))


(defmethod impl/insert->honey :default [_honey jsql-insert]
  (throw (IllegalArgumentException.
           (str "Unsupported Insert type: " (.getClass jsql-insert)))))
