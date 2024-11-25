(ns plooney81.nectar.sql.set-operation
  (:require [honey.sql.helpers :as sql]
            [plooney81.nectar.sql.impl :as impl])
  (:import (net.sf.jsqlparser.statement.select ExceptOp IntersectOp UnionOp)))

(defmethod impl/set-operation UnionOp [^UnionOp jsql-union-op]
  (if (.isAll jsql-union-op)
    sql/union-all
    sql/union))

(defmethod impl/set-operation IntersectOp [^IntersectOp _jsql-intersect-op]
  sql/intersect)

(defmethod impl/set-operation ExceptOp [^ExceptOp _jsql-except-op]
  sql/except)

(defmethod impl/set-operation :default [jsql]
  (throw (IllegalArgumentException.
           (str "Unsupported Set Operation type: " (.getClass jsql)))))