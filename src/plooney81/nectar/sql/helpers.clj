(ns plooney81.nectar.sql.helpers
  (:require [clojure.string :as str]
            [honey.sql.helpers :as sql]
            [plooney81.nectar.jsql :as jsql]
            [plooney81.nectar.sql.impl :as impl])
  (:import (net.sf.jsqlparser.expression Function)))

(defn window-definition-extraction
  "Hacky way to remove the beginning portion of a window definition.
   Takes:   `rolling_window AS (PARTITION BY ...)`
   Returns: `(PARTITION BY ...)`

   Needed for window-item conversion when window-elements are used.
   Honey-sql doesn't currently support window-frame-definitions, so if we find any
   we just :raw everything outside of the window-name."
  [jsql-expr window-name]
  (-> jsql-expr
      (.toString)
      (str/replace (str window-name " AS ") "")
      (str/trim)))

(defn keywordize-alias
  "Grabs the alias from a jsql object and converts it to a keyword"
  [jsql]
  (keyword (jsql/get-alias jsql)))

(defn apply-if-not-empty [sql-fn honey items]
  (if-not (empty? items)
    (apply sql-fn honey items)
    honey))

(defn raw-honey [string]
  [[:raw string]])

(defn jsql->raw-honey [jsql-expr]
  (raw-honey (.toString jsql-expr)))

(defn convert-column [jsql-expr]
  (when jsql-expr
    (if-let [alias (jsql/get-alias jsql-expr)]
      (keyword (str alias "." (jsql/get-column-name jsql-expr)))
      (keyword (jsql/get-column-name jsql-expr)))))

(defn convert-order-by-items
  ([jsql-select]
   (convert-order-by-items jsql-select (jsql/get-order-by-elements jsql-select)))
  ([_jsql-select order-by-items]
   (when order-by-items
     (->> order-by-items
          (map (fn [order-by-item]
                 (when-let [column (-> (jsql/get-expression order-by-item)
                                       (convert-column))]
                   (let [asc?  (jsql/get-order-by-asc? order-by-item)
                         order (if asc? :asc :desc)]
                     [column order]))))))))

(defn make-inline-sql-keyword [string]
  (-> (str "!" string)
      keyword))

(defn convert-trim-spec [trim-spec]
  (when trim-spec
    (-> trim-spec
        (.toString)
        (str/lower-case)
        make-inline-sql-keyword)))

(defn convert-data-type [data-type]
  (-> data-type
      (str/lower-case)
      (str/replace #" " "-")
      (keyword)))

(defn generate-fn-params [parameters]
  (map impl/expression->honey parameters))

(defn handle-named-params [named-params]
  (let [params  (generate-fn-params named-params)
        keyword (case (count params)
                  1 [nil]
                  2 [:!from nil]
                  3 [:!from :!for nil])]
    (->> (interleave params keyword)
         (remove nil?)
         (into []))))

(defn handle-params [^Function jsql-function]
  (let [named-parameters (jsql/get-named-parameters jsql-function)
        parameters       (jsql/get-parameters jsql-function)]
    (if named-parameters
      (handle-named-params named-parameters)
      (generate-fn-params parameters))))

(defn convert-join-list [honey join-items]
  (->> join-items
       (map (fn [join-item]
              (let [type                     (jsql/determine-join-type join-item)
                    table                    (jsql/get-join-table join-item)
                    using                    (jsql/get-join-using-columns join-item)
                    on-expressions           (jsql/get-join-on-expressions join-item)
                    converted-table          (jsql/convert-table table)
                    converted-using          (->> using (map convert-column))
                    converted-on-expressions (->> on-expressions (map impl/expression->honey))]
                (cond-> {:type  type
                         :table converted-table
                         :on    converted-on-expressions}
                  (not (empty? converted-using)) (assoc :using converted-using)))))
       (reduce (fn [honey-acc join-item]
                 (let [{:keys [type table using on]} join-item
                       join-fn (case type
                                 :inner sql/inner-join
                                 :left  sql/left-join
                                 :right sql/right-join
                                 :full  sql/full-join
                                 :cross sql/cross-join
                                 :outer sql/outer-join
                                 sql/join)]
                   (if using
                     (join-fn honey-acc table (into [:using] using))
                     (apply join-fn honey-acc table on))))
               honey)))

(defn convert-with-list [honey with-items]
  (let [any-recursive? (some jsql/is-recursive with-items)
        with-fn        (if any-recursive? sql/with-recursive sql/with)]
    (->> with-items
         (reduce (fn [honey-acc with-item]
                   (let [alias    (keywordize-alias with-item)
                         select   (jsql/get-select-in-paren-select with-item)
                         subquery (impl/select->honey {} select)
                         wrapped  (if (jsql/is-materialized? with-item)
                                    [:materialized subquery]
                                    subquery)]
                     (with-fn honey-acc [alias wrapped])))
                 honey))))
