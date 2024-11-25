(ns plooney81.nectar.sql.impl
  (:require [honey.sql :as honey]))

(defmulti
  jsql->honey-adapter
  "Converts jsql objects to honey-sql syntax based on the jsql class"
  (fn [_honey jsql]
    (.getClass jsql)))

(defmulti
  select->honey
  "Converts jsql-select objects to honey-sql based on the select class"
  (fn [_honey jsql-select]
    (.getClass jsql-select)))

(defmulti
  expression
  "Converts a jsql expression to an alternative syntax that will ultimately be used to convert to honeysql"
  (fn [jsql-expr]
    (.getClass jsql-expr)))

(defonce ^:private operator-list (atom #{:and :or :* :/ :+ :-}))

(defn register-operator! [operator]
  (assert (keyword? operator))
  (swap! operator-list conj operator)
  (when-not (honey/registered-op? operator)
    ;; Honeysql doesn't support all operators out of the box.
    ;; Here we tell honey to explicitly treat some keywords as operators.
    ;; https://github.com/seancorfield/honeysql/blob/develop/doc/extending-honeysql.md
    (honey/register-op! operator)))

(defn- expr->honey [expression]
  (if (:type expression)
    (let [convert-exprs-list @operator-list
          convert-exprs? (contains? convert-exprs-list (:type expression))
          convert-exprs (fn [exprs]
                          (map expr->honey exprs))
          exprs         (if convert-exprs?
                          (convert-exprs (:exprs expression))
                          (:exprs expression))]
      (into [(:type expression)] exprs))
    expression))

(defn expression->honey [jsql-expr]
  (expr->honey (expression jsql-expr)))

(defmulti
  function->honey
  "Takes a jsql function object and returns the appropriate honey-sql"
  (fn [jsql-function]
    (.getName jsql-function)))

(defmulti
  select-item
  "Takes a jsql select-item and returns the appropriate honey-sql syntax"
  (fn [jsql-expression _jsql-expression-alias]
    (.getClass jsql-expression)))

(defmulti
  set-operation
  "Takes a jsql set-operation and returns the appropriate honey-sql helper function"
  (fn [jsql-set-op]
    (.getClass jsql-set-op)))

(defn set-operation-zip [operation-fns select-statements]
  (let [[left right & left-over-selects] select-statements]
    (loop [left              left
           right             right
           op-fns            operation-fns
           left-over-selects left-over-selects]
      (if right
        (let [op-fn     (first op-fns)
              new-left  (apply op-fn [left right])
              new-right (first left-over-selects)]
          (recur new-left new-right (rest op-fns) (rest left-over-selects)))
        left))))