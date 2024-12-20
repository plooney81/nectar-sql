(ns plooney81.nectar.sql.select
  (:require [honey.sql.helpers :as sql]
            [plooney81.nectar.jsql :as jsql]
            [plooney81.nectar.sql.helpers :as helpers]
            [plooney81.nectar.sql.impl :as impl]
            [plooney81.nectar.sql.select-item :as select-item])
  (:import (net.sf.jsqlparser.statement.select PlainSelect SetOperation SetOperationList WithItem)))

;; TODO - Maybe some helper for all of the `convert` functions
;;        Copying the pattern of `if-let` reduce else: return honey
(defn convert-with-items [honey ^PlainSelect jsql-select]
  (if-let [with-items (jsql/get-with-items jsql-select)]
    (let [any-recursive? (some jsql/is-recursive with-items)
          with-fn        (if any-recursive?
                           sql/with-recursive
                           sql/with)]
      (->> with-items
           (reduce (fn [honey-sql ^WithItem with-item]
                     (let [alias  (helpers/keywordize-alias with-item)
                           select (jsql/get-select-in-paren-select with-item)]
                       (with-fn honey-sql [alias (impl/select->honey {} select)])))
                   honey)))
    honey))

(defn convert-select-items [honey jsql-select]
  (let [type->honey  {:filter       sql/filter
                      :within_group sql/within-group}
        distinct?    (jsql/is-distinct? jsql-select)
        select-fn    (if distinct?
                       sql/select-distinct
                       sql/select)
        select-items (->> (jsql/get-select-items jsql-select)
                          (map select-item/select-item->honey))]
    (->> select-items
         (group-by :type)
         (reduce (fn [honey [type select-items]]
                   (if (nil? type)
                     (apply select-fn honey select-items)
                     (let [combined-exprs         (mapcat :exprs select-items)
                           converted-select-items (into [type] combined-exprs)]
                       (if (= type :over)
                         (apply select-fn honey [[converted-select-items]])
                         (->> select-items
                              (reduce (fn [honey {:keys [exprs alias raw]}]
                                        (let [honey-fn  (get type->honey type)
                                              filter-ex (if honey-fn
                                                          (apply honey-fn (first exprs))
                                                          [:raw raw])]
                                          (select-fn honey [filter-ex alias])))
                                      honey))))))
                 honey))))

(defn convert-from-item [honey jsql-select]
  (if-let [from-item (jsql/get-from-item jsql-select)]
    (sql/from honey (jsql/convert-table from-item))
    honey))

(defn convert-join-items [honey jsql-select]
  (if-let [join-items (jsql/get-join-items jsql-select)]
    (->> join-items
         (map (fn [join-item]
                (let [type                     (jsql/determine-join-type join-item)
                      table                    (jsql/get-join-table join-item)
                      using                    (jsql/get-join-using-columns join-item)
                      on-expressions           (jsql/get-join-on-expressions join-item)
                      converted-table          (jsql/convert-table table)
                      converted-using          (->> using
                                                    (map helpers/convert-column))
                      converted-on-expressions (->> on-expressions
                                                    (map impl/expression->honey))]
                  (cond-> {:type  type
                           :table converted-table
                           :on    converted-on-expressions}
                    (not (empty? converted-using)) (assoc :using converted-using)))))
         (reduce (fn [honey join-item]
                   (let [{:keys [type table using on]} join-item
                         join-fn (case type
                                   :inner sql/inner-join
                                   :left sql/left-join
                                   :right sql/right-join
                                   :full sql/full-join
                                   :cross sql/cross-join
                                   :outer sql/outer-join
                                   sql/join)]
                     (if using
                       (join-fn honey table (into [:using] using))
                       (apply join-fn honey table on))))
                 honey))
    honey))

(defn convert-where-items [honey jsql-select]
  (if-let [where-item (jsql/get-where jsql-select)]
    (sql/where honey (impl/expression->honey where-item))
    honey))

(defn convert-window-item [jsql-expr]
  (let [window-element (jsql/get-window-element jsql-expr)
        partition-by   (->> (jsql/get-partition-by-expression-list jsql-expr)
                            (map impl/expression->honey))
        order-by       (helpers/convert-order-by-items jsql-expr)
        window-name    (jsql/get-window-name jsql-expr)
        window-name-kw (when window-name
                         (keyword window-name))
        inner-honey    (if window-element
                         [[:raw (helpers/window-definition-extraction jsql-expr window-name)]]
                         (as-> {} honey
                               (helpers/apply-if-not-empty sql/partition-by honey partition-by)
                               (helpers/apply-if-not-empty sql/order-by honey order-by)))]
    [window-name-kw inner-honey]))

(defn convert-window-items [honey jsql-select]
  (if-let [window-items (jsql/get-window-definitions jsql-select)]
    (->> window-items
         (reduce (fn [honey-sql jsql-expr]
                   (apply sql/window honey-sql (convert-window-item jsql-expr)))
                 honey))
    honey))

(defn convert-group-by-items [honey jsql-select]
  (if-let [group-by-items (jsql/get-group-by jsql-select)]
    (let [group-bys (->> (jsql/get-group-by-expression-list group-by-items)
                         (map (fn [group-by-expression]
                                (helpers/convert-column group-by-expression))))]
      (apply sql/group-by honey group-bys))
    honey))

(defn convert-order-by-items
  [honey jsql]
  (if-let [order-by-items (helpers/convert-order-by-items jsql)]
    (apply sql/order-by honey order-by-items)
    honey))

(defn convert-limit [honey jsql-select]
  (if-let [limit (jsql/get-limit jsql-select)]
    (sql/limit honey (jsql/get-limit-value limit))
    honey))

(defn convert-offset [honey jsql-select]
  (if-let [offset (jsql/get-offset jsql-select)]
    (sql/offset honey (jsql/get-offset-value offset))
    honey))

(defmethod impl/select->honey PlainSelect [honey ^PlainSelect jsql-plain-select]
  (-> honey
      (convert-with-items jsql-plain-select)
      (convert-select-items jsql-plain-select)
      (convert-from-item jsql-plain-select)
      (convert-join-items jsql-plain-select)
      (convert-where-items jsql-plain-select)
      (convert-window-items jsql-plain-select)
      (convert-group-by-items jsql-plain-select)
      (convert-order-by-items jsql-plain-select)
      (convert-limit jsql-plain-select)
      (convert-offset jsql-plain-select)
      ))

(defmethod impl/select->honey SetOperationList [honey ^SetOperationList jsql-set-operation-list]
  (let [selects           (->> (jsql/get-set-operation-list-selects jsql-set-operation-list)
                               (mapv (fn [^SetOperation jsql-set-operation]
                                       (impl/select->honey {} jsql-set-operation))))
        operation-fns     (->> (jsql/get-set-operation-list-operations jsql-set-operation-list)
                               (mapv impl/set-operation))]
    (-> (merge honey (impl/set-operation-zip operation-fns selects))
        (convert-order-by-items jsql-set-operation-list))))

(defmethod impl/select->honey :default [_honey jsql-select]
  (throw (IllegalArgumentException.
           (str "Unsupported SelectBody type: " (.getClass jsql-select)))))