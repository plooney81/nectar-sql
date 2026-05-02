(ns plooney81.nectar.sql.update
  (:require [honey.sql.helpers :as sql]
            [plooney81.nectar.jsql :as jsql]
            [plooney81.nectar.sql.helpers :as helpers]
            [plooney81.nectar.sql.impl :as impl])
  (:import (net.sf.jsqlparser.statement.update Update UpdateSet)))

(defn- convert-set-clause [^Update jsql-update]
  (->> (.getUpdateSets jsql-update)
       (reduce (fn [acc ^UpdateSet update-set]
                 (assoc acc
                        (helpers/convert-column (first (.getColumns update-set)))
                        (impl/expression->honey (first (.getValues update-set)))))
               {})))

(defn- handle-with-items [honey ^Update jsql-update]
  (if-let [with-items (.getWithItemsList jsql-update)]
    (helpers/convert-with-list honey with-items)
    honey))

(defn- handle-table [honey ^Update jsql-update]
  (sql/update honey (jsql/convert-table (.getTable jsql-update))))

(defn- handle-set [honey ^Update jsql-update]
  (sql/set honey (convert-set-clause jsql-update)))

(defn- handle-from [honey ^Update jsql-update]
  (if-let [from-item (.getFromItem jsql-update)]
    (sql/from honey (jsql/convert-table from-item))
    honey))

(defn- handle-where [honey ^Update jsql-update]
  (if-let [where (.getWhere jsql-update)]
    (sql/where honey (impl/expression->honey where))
    honey))

(defn- handle-order-by [honey ^Update jsql-update]
  (if-let [order-by-items (helpers/convert-order-by-items jsql-update)]
    (apply sql/order-by honey order-by-items)
    honey))

(defn- handle-joins [honey ^Update jsql-update]
  (if-let [joins (.getJoins jsql-update)]
    (helpers/convert-join-list honey joins)
    honey))

(defn- handle-limit [honey ^Update jsql-update]
  (if-let [limit (.getLimit jsql-update)]
    (sql/limit honey (jsql/get-limit-value limit))
    honey))

(defmethod impl/update->honey Update [honey ^Update jsql-update]
  (-> honey
      (handle-with-items jsql-update)
      (handle-table jsql-update)
      (handle-set jsql-update)
      (handle-from jsql-update)
      (handle-joins jsql-update)
      (handle-where jsql-update)
      (handle-order-by jsql-update)
      (handle-limit jsql-update)))

(defmethod impl/update->honey :default [_honey jsql-update]
  (throw (IllegalArgumentException.
           (str "Unsupported Update type: " (.getClass jsql-update)))))
