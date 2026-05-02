(ns plooney81.nectar.sql.delete
  (:require [honey.sql.helpers :as sql]
            [plooney81.nectar.jsql :as jsql]
            [plooney81.nectar.sql.helpers :as helpers]
            [plooney81.nectar.sql.impl :as impl])
  (:import (net.sf.jsqlparser.statement.delete Delete)))

(defn- handle-table [honey ^Delete jsql-delete]
  (sql/delete-from honey (jsql/convert-table (.getTable jsql-delete))))

(defn- handle-where [honey ^Delete jsql-delete]
  (if-let [where (.getWhere jsql-delete)]
    (sql/where honey (impl/expression->honey where))
    honey))

(defn- handle-order-by [honey ^Delete jsql-delete]
  (if-let [order-by-items (helpers/convert-order-by-items jsql-delete)]
    (apply sql/order-by honey order-by-items)
    honey))

(defn- handle-limit [honey ^Delete jsql-delete]
  (if-let [limit (.getLimit jsql-delete)]
    (sql/limit honey (jsql/get-limit-value limit))
    honey))

(defmethod impl/delete->honey Delete [honey ^Delete jsql-delete]
  (-> honey
      (handle-table jsql-delete)
      (handle-where jsql-delete)
      (handle-order-by jsql-delete)
      (handle-limit jsql-delete)))

(defmethod impl/delete->honey :default [_honey jsql-delete]
  (throw (IllegalArgumentException.
           (str "Unsupported Delete type: " (.getClass jsql-delete)))))
