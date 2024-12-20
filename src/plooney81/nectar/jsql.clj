(ns plooney81.nectar.jsql
  (:import (net.sf.jsqlparser.expression.operators.relational LikeExpression)
           (net.sf.jsqlparser.parser CCJSqlParserUtil)
           (net.sf.jsqlparser.statement.create.table ColDataType)
           (net.sf.jsqlparser.statement.insert Insert)
           (net.sf.jsqlparser.statement.select
             FromItem GroupByElement Join Limit Offset OrderByElement PlainSelect SelectItem SetOperationList WithItem)
           (net.sf.jsqlparser.expression
             AnalyticExpression CastExpression JsonExpression LongValue NotExpression TrimFunction Function Parenthesis SignedExpression
             WindowDefinition)
           (net.sf.jsqlparser.schema Column)
           ))

(defn get-jsql-methods
  "Helper function that returns a list of methods available for the provided jsql-object"
  [jsql]
  (->> (.getMethods (class jsql))
       (map #(.getName %))))

(defn get-select [jsql]
  (.getSelect jsql))

(defn get-plain-select [jsql]
  (.getPlainSelect jsql))

(defn get-select-in-paren-select [jsql]
  (-> jsql
      get-select
      get-select))

(defn get-offset [^PlainSelect jsql-select]
  (.getOffset jsql-select))

(defn get-limit [^PlainSelect jsql-select]
  (.getLimit jsql-select))

(defn get-with-items ^WithItem [^PlainSelect jsql-select]
  (.getWithItemsList jsql-select))

(defn is-recursive [^WithItem with-item]
  (.isRecursive with-item))

(defn get-parameters [^Function jsql]
  (.getParameters jsql))

(defn get-named-parameters [^Function jsql]
  (.getNamedParameters jsql))

(defmulti is-distinct? #(.getClass %))

(defmethod is-distinct? Function [jsql-function]
  (.isDistinct jsql-function))

(defmethod is-distinct? PlainSelect [jsql-plain-select]
  (some? (.getDistinct jsql-plain-select)))

(defmethod is-distinct? :default [jsql-expr]
  (throw (IllegalArgumentException.
           (str "Unsupported IsDistinct? for Type: " (.getClass jsql-expr)))))

(defn get-select-items [^PlainSelect jsql-select]
  (.getSelectItems jsql-select))

(defn get-from-item [^PlainSelect jsql-select]
  (.getFromItem jsql-select))

(defn get-name [general-jsql-select]
  (.getName general-jsql-select))

(defmulti get-alias #(.getClass %))

(defn generic-alias [jsql]
  (when-let [alias (.getAlias jsql)]
    (get-name alias)))

;; TODO: Is this really all that necessary?
;;       Couldn't we just use a `defn`
(defmethod get-alias :default [jsql]
  (try (generic-alias jsql)
       (catch Exception e
         (throw
           (IllegalArgumentException.
             (str "Unsupported get-alias for Type: " (.getClass jsql)))))))

(defmethod get-alias Column [^Column column]
  (when-let [table (.getTable column)]
    (get-name table)))

(defmethod get-alias Insert [^Insert jsql-insert]
  (when-let [table (.getTable jsql-insert)]
    (get-name table)))

(defmulti get-schema #(.getClass %))

(defn generic-get-schema [jsql-item]
  (.getSchemaName jsql-item))

(defmethod get-schema :default [jsql]
  (try (generic-get-schema jsql)
       (catch Exception _e
         (throw
           (IllegalArgumentException.
             (str "Unsupported get-schema for Type: " (.getClass jsql)))))))

(defmulti convert-table #(.getClass %))

(defn generic-table-conversion [jsql-item]
  (let [alias  (get-alias jsql-item)
        schema (get-schema jsql-item)]
    (if alias
      (let [name  (get-name jsql-item)
            table (if schema
                    (str schema "." name)
                    name)]
        [(keyword table) (keyword alias)])
      (keyword (.toString jsql-item)))))

(defmethod convert-table :default [jsql]
  (try (generic-table-conversion jsql)
       (catch Exception e
         (throw
           (IllegalArgumentException.
             (str "Unsupported convert-table for Type: " (.getClass jsql)))))))

(defn get-join-items [^PlainSelect jsql-select]
  (.getJoins jsql-select))

(defn get-join-table [^Join jsql-join-item]
  (.getRightItem jsql-join-item))

(defn get-join-using-columns [^Join jsql-join-item]
  (.getUsingColumns jsql-join-item))

(defn get-join-on-expressions [^Join jsql-join-item]
  (.getOnExpressions jsql-join-item))

(defn determine-join-type [^Join jsql-join-item]
  (let [outer?       (.isOuter jsql-join-item)
        right?       (.isRight jsql-join-item)
        left?        (.isLeft jsql-join-item)
        full?        (.isFull jsql-join-item)
        inner?       (.isInner jsql-join-item)
        simple?      (.isSimple jsql-join-item)
        cross?       (.isCross jsql-join-item)
        natural?     (.isNatural jsql-join-item)
        ;; Combos
        full-outer?  (and full? outer?)
        left-outer?  (and left? outer?)
        right-outer? (and right? outer?)]
    (cond
      inner? :inner
      (or left-outer? left?) :left
      (or right-outer? right?) :right
      (or full-outer? full?) :full
      cross? :cross
      outer? :outer
      natural? :natural
      simple? :simple
      :else :simple)))

(defn get-set-operation-list-selects [^SetOperationList jsql-set-operation-list]
  (.getSelects jsql-set-operation-list))

(defn get-set-operation-list-operations [^SetOperationList jsql-set-operation-list]
  (.getOperations jsql-set-operation-list))

(defn get-where [^PlainSelect jsql-select]
  (.getWhere jsql-select))

(defn get-window-definitions
  ^WindowDefinition [^PlainSelect jsql-select]
  (.getWindowDefinitions jsql-select))

(defn get-window-element [jsql-expr]
  (.getWindowElement jsql-expr))

(defn get-group-by [^PlainSelect jsql-select]
  (.getGroupBy jsql-select))

(defmulti get-order-by-elements #(.getClass %))

(defmethod get-order-by-elements SetOperationList [jsql-set-operation-list]
  (.getOrderByElements jsql-set-operation-list))

(defmethod get-order-by-elements PlainSelect [jsql-select]
  (.getOrderByElements jsql-select))

(defmethod get-order-by-elements WindowDefinition [jsql-expr]
  (.getOrderByElements jsql-expr))

(defmethod get-order-by-elements AnalyticExpression [jsql-expr]
  (.getOrderByElements jsql-expr))

(defmethod get-order-by-elements :default [jsql-expr]
  (throw (IllegalArgumentException.
           (str "Unsupported GetOrderByElements for Type: " (.getClass jsql-expr)))))

(defmulti get-expression #(.getClass %))

(defmethod get-expression SelectItem [jsql-expr]
  (.getExpression jsql-expr))

(defmethod get-expression OrderByElement [jsql-expr]
  (.getExpression jsql-expr))

(defmethod get-expression AnalyticExpression [jsql-expr]
  (.getExpression jsql-expr))

(defmethod get-expression SignedExpression [jsql-expr]
  (.getExpression jsql-expr))

(defmethod get-expression Parenthesis [jsql-expr]
  (.getExpression jsql-expr))

(defmethod get-expression TrimFunction [jsql-expr]
  (.getExpression jsql-expr))

(defmethod get-expression NotExpression [jsql-expr]
  (.getExpression jsql-expr))

(defmethod get-expression JsonExpression [jsql-expr]
  (.getExpression jsql-expr))

(defmethod get-expression :default [jsql-expr]
  (throw (IllegalArgumentException.
           (str "Unsupported GetExpression for Type: " (.getClass jsql-expr)))))

(defn get-sign [^SignedExpression jsql]
  (.getSign jsql))

(defn get-trim-spec [jsql]
  (.getTrimSpecification jsql))

(defn get-from-expression [jsql]
  (.getFromExpression jsql))

(defn is-case-insensitive? [^LikeExpression jsql]
  (.isCaseInsensitive jsql))

(defn get-data-type [^ColDataType jsql]
  (.getDataType jsql))

(defn get-col-data-type [^CastExpression jsql]
  (.getColDataType jsql))

(defn get-order-by-asc? [^OrderByElement jsql-order-by]
  (.isAsc jsql-order-by))

(defn get-partition-by-expression-list [jsql-expr]
  (.getPartitionExpressionList jsql-expr))

(defn get-group-by-expression-list [^GroupByElement jsql-group-by]
  (.getGroupByExpressionList jsql-group-by))

(defn get-column-name [jsql-expr]
  (.getColumnName jsql-expr))

(defn get-left-expression [jsql-expr]
  (.getLeftExpression jsql-expr))

(defn get-right-expression [jsql-expr]
  (.getRightExpression jsql-expr))

(defn is-not? [jsql-expr]
  (.isNot jsql-expr))

(defn get-value [jsql-expr]
  (.getValue jsql-expr))

(defn get-window-name [jsql-expr]
  (.getWindowName jsql-expr))

(defn get-limit-value [^Limit limit]
  (let [row-count-expr (.getRowCount limit)]
    (if (instance? LongValue row-count-expr)
      (.getValue ^LongValue row-count-expr)
      (throw (Exception. "Unsupported RowCountExpression type")))))

(defn get-offset-value [^Offset offset]
  (let [offset-expr (.getOffset offset)]
    (if (instance? LongValue offset-expr)
      (.getValue ^LongValue offset-expr)
      (throw (Exception. "Unsupported OffsetExpression type")))))

(defn to-nectar [^String text]
  (CCJSqlParserUtil/parse  text))