(ns plooney81.nectar.sql.select-item
  (:require [clojure.string :as str]
            [honey.sql.helpers :as sql]
            [plooney81.nectar.jsql :as jsql]
            [plooney81.nectar.sql.expression :as expr]
            [plooney81.nectar.sql.helpers :as helpers]
            [plooney81.nectar.sql.impl :as impl])
  (:import (net.sf.jsqlparser.expression Alias AnalyticExpression Function Parenthesis TimeKeyExpression)
           (net.sf.jsqlparser.statement.select SelectItem)))

(defn generic-select-item [jsql-expr alias]
  (let [expression (impl/expression->honey jsql-expr)]
    (if alias
      [expression alias]
      expression)))

(defmethod impl/select-item Parenthesis [^Parenthesis jsql-expression ^Alias alias]
  (let [inner (impl/select-item (jsql/get-expression jsql-expression) nil)]
    (if alias
      [inner alias]
      inner)))

(defmethod impl/select-item Function [^Function jsql-expr ^Alias alias]
  (let [expression (impl/expression->honey jsql-expr)]
    (if alias
      [expression alias]
      [expression])))

(defmethod impl/select-item TimeKeyExpression [^TimeKeyExpression jsql-expression ^Alias alias]
  (let [raw-value [:raw (.getStringValue jsql-expression)]]
    (if alias
      [raw-value alias]
      [raw-value])))

;; TODO - Move this to a utils namespace
(defn- get-analytical-expression-type [^AnalyticExpression jsql-expr]
  (let [kw (-> (.getType jsql-expr)
               (.toString)
               (str/lower-case)
               (keyword))]
    (cond
      (= kw :filter_only) :filter
      :else kw)))

(defmethod impl/select-item AnalyticExpression [^AnalyticExpression jsql-expr ^Alias alias]
  ;; Honeysql doesn't currently support different windowing frame definitions, so we have to get creative.
  ;; Window frame specifications define a subset of the current partition for computation.
  ;; Different window frame definitions:
  ;; - RANGE, ROWS
  (if-let [window-element (.getWindowElement jsql-expr)]
    (let [raw-value (.toString jsql-expr)]
      [[:raw raw-value] alias])
    (let [analytic-expression-type (get-analytical-expression-type jsql-expr)
          fn->keyword              (-> (jsql/get-name jsql-expr)
                                       (str/lower-case))
          parameter                (jsql/get-expression jsql-expr)
          params                   (when parameter
                                     (helpers/generate-fn-params [parameter]))
          partition-by             (->> (jsql/get-partition-by-expression-list jsql-expr)
                                        (map (fn [order-by-item]
                                               (impl/expression order-by-item))))
          order-by                 (helpers/convert-order-by-items jsql-expr)
          ;; TODO - Move to jsql
          filter-expression        (.getFilterExpression jsql-expr)
          filter-expression        (when filter-expression
                                     (impl/expression->honey filter-expression))
          inner-honey              (as-> {} honey
                                         (helpers/apply-if-not-empty sql/partition-by honey partition-by)
                                         (helpers/apply-if-not-empty sql/order-by honey order-by)
                                         (helpers/apply-if-not-empty sql/where honey filter-expression))
          inner-fn                 (expr/create-honey-fn fn->keyword params)
          raw-value                (.toString jsql-expr)
          window-name              (jsql/get-window-name jsql-expr)
          window-name              (when window-name
                                     (keyword window-name))
          exprs                    (cond
                                     (and window-name alias)
                                     [inner-fn window-name alias]

                                     window-name
                                     [inner-fn window-name]

                                     (or (= analytic-expression-type :filter)
                                         (= analytic-expression-type :within_group))
                                     [inner-fn inner-honey]

                                     alias
                                     [inner-fn inner-honey alias]

                                     :else
                                     [inner-fn inner-honey])]
      {:type  analytic-expression-type
       :exprs [exprs]
       :alias alias
       :raw   raw-value})))

(defmethod impl/select-item :default [jsql-expression jsql-expression-alias]
  (try (generic-select-item jsql-expression jsql-expression-alias)
       (catch Exception e
         (throw
           (IllegalArgumentException.
             (str "Unsupported Select Item type: " (.getClass jsql-expression)))))))

(defn select-item->honey [^SelectItem select-item]
  (let [alias (helpers/keywordize-alias select-item)]
    (impl/select-item (jsql/get-expression select-item) alias)))
