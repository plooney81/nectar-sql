(ns plooney81.nectar.sql.helper-form
  "Turns a HoneySQL data map into the equivalent `honey.sql.helpers` code:
   a quoted `->` chain of helper calls, plus a pretty-printed string of it."
  (:require [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [honey.sql :as honey]))

(def ^:private insert-clauses
  #{:insert-into :replace-into :patch-into :columns})

(def ^:private clause-rank
  "Order helper calls are emitted in: HoneySQL's clause order, except that the
   INSERT clauses lead, so `INSERT … SELECT` reads as insert-into then select.
   Clause order within a map doesn't change the map the chain builds."
  (let [order            (honey/clause-order)
        [before-select after] (split-with #(not= :select %) (remove insert-clauses order))
        emit-order       (concat before-select (filter insert-clauses order) after)]
    (into {} (map-indexed (fn [i k] [k i]) emit-order))))

(def ^:private single-arg-clauses
  "Clauses whose helper takes the map value as one argument, rather than
   spreading a vector value into multiple arguments."
  #{:where :having :update :set :values :limit :offset})

(def ^:private set-operations
  "Set operation helpers accept statement maps as their leading arguments;
   every other helper would treat a leading map as the statement to merge into."
  #{:union :union-all :intersect :except :except-all})

(defn- join-clause? [k]
  (and (not= k :cross-join)
       (or (= k :join) (str/ends-with? (name k) "-join"))))

(defn- statement?
  "A map is a (sub)statement when every key is a known HoneySQL clause, as
   opposed to e.g. the column->value map of a `:set` clause."
  [m]
  (and (map? m) (seq m) (every? clause-rank (keys m))))

(declare map->form)

(defn- walk-value
  "Converts nested statements into helper chains, and any seqs into vectors,
   since a seq inside a quoted form would be evaluated as a function call."
  [alias v]
  (cond
    (statement? v)  (map->form v alias)
    (map? v)        (into {} (map (fn [[k x]] [k (walk-value alias x)])) v)
    (sequential? v) (mapv #(walk-value alias %) v)
    :else           v))

(defn- helper [alias k]
  (symbol (str alias) (name k)))

(defn- clause->calls
  "Returns the helper calls that together produce `{k v}`."
  [alias k v]
  (let [f (helper alias k)
        v (walk-value alias v)]
    (cond
      (not (clause-rank k))
      [(list 'merge {k v})]

      (and (join-clause? k) (sequential? v) (even? (count v)))
      (map (fn [[table condition]] (list f table condition)) (partition 2 v))

      (or (single-arg-clauses k) (not (sequential? v)))
      [(list f v)]

      :else
      [(apply list f v)])))

(defn- leading-map-arg?
  "True when the call's first argument would evaluate to a map, which the
   helper would mistake for the statement to merge into."
  [[_f arg :as call]]
  (and (> (count call) 1)
       (or (map? arg) (seq? arg))))

(defn map->form
  "Returns a quoted helper form that evaluates to `honey-map`, with the
   helpers referenced through `alias` (e.g. `h/select`)."
  [honey-map alias]
  (let [clauses (sort-by (fn [[k _]] (clause-rank k Integer/MAX_VALUE)) honey-map)
        calls   (mapcat (fn [[k v]] (clause->calls alias k v)) clauses)
        [first-call & more] calls
        seed?   (and (leading-map-arg? first-call)
                     (not (set-operations (ffirst clauses))))]
    (cond
      (empty? calls)            {}
      seed?                     (apply list '-> {} calls)
      (empty? more)             first-call
      :else                     (apply list '-> calls))))

(defn- readable-keyword? [k]
  (try
    (= k (read-string (str k)))
    (catch Exception _ false)))

(defn- readable-form
  "Keywords such as `:@>` exist as data but cannot be read back from text, so
   they are written as `(keyword \"@>\")` calls instead."
  [form]
  (walk/postwalk
    (fn [x]
      (if (and (keyword? x) (not (readable-keyword? x)))
        (list 'keyword (subs (str x) 1))
        x))
    form))

(defn- thread-dispatch
  "`code-dispatch`, except `->` chains keep each step aligned under the first:

     (-> (h/select :a)
         (h/from :t))"
  [obj]
  (if (and (seq? obj) (= '-> (first obj)))
    (pp/pprint-logical-block :prefix "(" :suffix ")"
      (pp/write-out (first obj))
      (.write ^java.io.Writer *out* " ")
      (pp/pprint-indent :current 0)
      (pp/print-length-loop [steps (next obj)]
        (when steps
          (pp/write-out (first steps))
          (when (next steps)
            (.write ^java.io.Writer *out* " ")
            (pp/pprint-newline :linear)
            (recur (next steps))))))
    (pp/code-dispatch obj)))

(defn- pprint-code [form]
  (with-out-str
    (pp/with-pprint-dispatch thread-dispatch
      (pp/pprint (readable-form form)))))

(defn helpers-output
  "Returns `{:form <quoted helper form> :text <pretty-printed string>}` for
   `honey-map`. The text starts with the require line the form expects."
  [honey-map {:keys [alias] :or {alias 'h}}]
  (let [form (map->form honey-map alias)]
    {:form form
     :text (str ";; (:require [honey.sql.helpers :as " alias "])\n"
                (pprint-code form))}))
