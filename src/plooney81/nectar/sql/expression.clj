(ns plooney81.nectar.sql.expression
  (:require [clojure.string :as str]
            [honey.sql.pg-ops :as sut]
            [plooney81.nectar.jsql :as jsql]
            [plooney81.nectar.sql.impl :as impl]
            [plooney81.nectar.sql.helpers :as helpers])
  (:import (net.sf.jsqlparser.expression.operators.arithmetic
             Addition BitwiseAnd BitwiseLeftShift BitwiseOr BitwiseRightShift Concat Division Modulo Multiplication Subtraction)
           (net.sf.jsqlparser.expression.operators.conditional AndExpression OrExpression)
           (net.sf.jsqlparser.expression.operators.relational
             Between EqualsTo GreaterThan GreaterThanEquals InExpression JsonOperator LikeExpression MinorThan MinorThanEquals
             NotEqualsTo IsNullExpression ParenthesedExpressionList RegExpMatchOperator)
           (net.sf.jsqlparser.expression
             CastExpression DoubleValue JsonExpression NotExpression SignedExpression TrimFunction Function LongValue Parenthesis StringValue)
           (net.sf.jsqlparser.schema Column)
           (net.sf.jsqlparser.statement.create.table ColDataType)
           (net.sf.jsqlparser.statement.select AllColumns ParenthesedSelect)))

(defmethod impl/expression Parenthesis [jsql-expr]
  (impl/expression (jsql/get-expression jsql-expr)))

(defmethod impl/expression ParenthesedSelect [jsql-expr]
  (impl/jsql->honey-adapter {} (jsql/get-plain-select jsql-expr)))

(defmethod impl/expression ParenthesedExpressionList [jsql-expr]
  (->> jsql-expr
       (mapv #(impl/expression %))))

(defn- get-left-and-right [jsql-expr]
  (let [left-expression  (impl/expression (jsql/get-left-expression jsql-expr))
        right-expression (impl/expression (jsql/get-right-expression jsql-expr))]
    [left-expression right-expression]))

(defn raw-honey [string]
  [[:raw string]])

(defn jsql-expr->raw-honey [jsql-expr]
  (raw-honey (.toString jsql-expr)))

(defn- handle-regular-operation
  [operator exprs]
  {:type  operator
   :exprs exprs})

(defn- handle-prefix-notation-operation
  "If the previous expression has the same type, combine the expr's.
   This mechanism allows us to express 1 + 2 + 3 as [:+ 1 2 3]."
  [operator exprs]
  (let [[left-expr right-expr] exprs]
    (if (= (:type left-expr) operator)
      (update left-expr :exprs conj right-expr)
      (handle-regular-operation operator exprs))))

(defmethod impl/expression NotExpression [jsql-expr]
  [:not (impl/expression (jsql/get-expression jsql-expr))])

(defmethod impl/expression AndExpression [jsql-expr]
  (handle-prefix-notation-operation :and (get-left-and-right jsql-expr)))

(defmethod impl/expression OrExpression [jsql-expr]
  (handle-prefix-notation-operation :or (get-left-and-right jsql-expr)))

(defmethod impl/expression EqualsTo [jsql-expr]
  (handle-regular-operation := (get-left-and-right jsql-expr)))

(defmethod impl/expression NotEqualsTo [jsql-expr]
  (handle-regular-operation :not= (get-left-and-right jsql-expr)))

(defmethod impl/expression GreaterThan [jsql-expr]
  (handle-regular-operation :> (get-left-and-right jsql-expr)))

(defmethod impl/expression GreaterThanEquals [jsql-expr]
  (handle-regular-operation :>= (get-left-and-right jsql-expr)))

(defmethod impl/expression MinorThan [jsql-expr]
  (handle-regular-operation :< (get-left-and-right jsql-expr)))

(defmethod impl/expression MinorThanEquals [jsql-expr]
  (handle-regular-operation :<= (get-left-and-right jsql-expr)))

(defmethod impl/expression Modulo [^Modulo jsql-expr]
  (handle-prefix-notation-operation :% (get-left-and-right jsql-expr)))

(defmethod impl/expression Multiplication [^Multiplication jsql-expr]
  (handle-prefix-notation-operation :* (get-left-and-right jsql-expr)))

(defmethod impl/expression Division [^Division jsql-expr]
  (handle-prefix-notation-operation :/ (get-left-and-right jsql-expr)))

(defmethod impl/expression Addition [^Addition jsql-expr]
  (handle-prefix-notation-operation :+ (get-left-and-right jsql-expr)))

(defmethod impl/expression Subtraction [^Subtraction jsql-expr]
  [(handle-prefix-notation-operation :- (get-left-and-right jsql-expr))])

(impl/register-operator! :<<)
(defmethod impl/expression BitwiseLeftShift [^BitwiseLeftShift jsql-expr]
  (handle-prefix-notation-operation :<< (get-left-and-right jsql-expr)))

(impl/register-operator! :>>)
(defmethod impl/expression BitwiseRightShift [^BitwiseRightShift jsql-expr]
  (handle-prefix-notation-operation :>> (get-left-and-right jsql-expr)))

(impl/register-operator! :&)
(defmethod impl/expression BitwiseAnd [^BitwiseAnd jsql-expr]
  (handle-prefix-notation-operation :& (get-left-and-right jsql-expr)))

(impl/register-operator! :|)
(defmethod impl/expression BitwiseOr [^BitwiseAnd jsql-expr]
  (handle-prefix-notation-operation :| (get-left-and-right jsql-expr)))

(defmethod impl/expression Concat [^Concat jsql-expr]
  (handle-prefix-notation-operation :|| (get-left-and-right jsql-expr)))

(defmethod impl/expression InExpression [^InExpression jsql-expr]
  (cond-> (handle-prefix-notation-operation :in (get-left-and-right jsql-expr))
    (jsql/is-not? jsql-expr) (assoc :type :not-in)))

(defmethod impl/expression Between [^Between jsql-expr]
  (let [left-expr    (impl/expression (jsql/get-left-expression jsql-expr))
        start        (impl/expression (.getBetweenExpressionStart jsql-expr))
        end          (impl/expression (.getBetweenExpressionEnd jsql-expr))
        between-type (if (jsql/is-not? jsql-expr) :not-between :between)]
    [between-type left-expr start end]))

(defmethod impl/expression LikeExpression [jsql-expr]
  (let [case-insensitive? (jsql/is-case-insensitive? jsql-expr)
        like-type          (if case-insensitive? "ilike" "like")
        actual-type       (if (jsql/is-not? jsql-expr)
                            (keyword (str "not-" like-type))
                            (keyword like-type))]
    (handle-regular-operation actual-type (get-left-and-right jsql-expr))))

(defmethod impl/expression IsNullExpression [jsql-expr]
  (let [is-not?   (jsql/is-not? jsql-expr)
        left-expr (impl/expression (jsql/get-left-expression jsql-expr))]
    (cond-> (handle-regular-operation := [left-expr nil])
      is-not? (assoc :type :not=))))

(defmethod impl/expression SignedExpression [^SignedExpression jsql-expr]
  (let [sign       (jsql/get-sign jsql-expr)
        expression (impl/expression (jsql/get-expression jsql-expr))]
    (case sign
      \+ expression
      \- (- expression)
      \~ (first (helpers/jsql->raw-honey jsql-expr)))))

(defmethod impl/expression ColDataType [^ColDataType jsql-expr]
  (helpers/convert-data-type (jsql/get-data-type jsql-expr)))

(defmethod impl/expression CastExpression [^CastExpression jsql-expr]
  (let [left-expression (impl/expression (jsql/get-left-expression jsql-expr))
        data-type       (impl/expression (jsql/get-col-data-type jsql-expr))]
    [[:cast left-expression data-type]]))

(defmethod impl/expression Function [^Function jsql-expr]
  (impl/function->honey jsql-expr))

(defn trim-inner-quotes
  "Removes single quotes around a string if they exist"
  [v]
  (if (and (string? v) (re-matches #"^'.*'$" v))
    (subs v 1 (dec (count v)))
    v)
  )

(defn cleanup-key
  "Tries to convert to an int if possible. If not trim-inner-quotes."
  [k]
  (try
    (Integer/parseInt k)
    (catch NumberFormatException e
      (trim-inner-quotes k))))

;; https://github.com/seancorfield/honeysql/blob/develop/test/honey/sql/pg_ops_test.cljc
(defmethod impl/expression JsonExpression [^JsonExpression jsql-expr]
  (let [expression (impl/expression (jsql/get-expression jsql-expr))
        expression (if (vector? expression) (first expression) expression)
        {:keys [operator exprs]} (->> jsql-expr
                                      (.getIdentList)
                                      (map (fn [ident]
                                             {:operator (keyword (.getValue ident))
                                              :exprs    (cleanup-key (.getKey ident))}))
                                      (apply merge))]
    [[operator expression exprs]]))

(impl/register-operator! :?)
(impl/register-operator! :?|)
(defmethod impl/expression JsonOperator [^JsonOperator jsql-expr]
  (let [operator      (.getStringExpression jsql-expr)
        json-operator (case operator
                        "@>" sut/at>
                        "<@" sut/<at
                        "@?" sut/at?
                        "@@" sut/atat
                        (keyword operator))]
    [(handle-regular-operation json-operator (get-left-and-right jsql-expr))]))

(defmethod impl/expression :default [jsql-expr]
  (throw
    (IllegalArgumentException.
      (str "Unsupported Expression type: " (.getClass jsql-expr)))))

(defn create-honey-fn
  ([fn-name parameters]
   (create-honey-fn fn-name parameters false))
  ([fn-name parameters distinct?]
   (if (empty? parameters)
     (keyword (str "%" fn-name))
     (cond->> parameters
              distinct? (into [:distinct])
              distinct? (conj [])
              :always (into [(keyword fn-name)])))))

(defmethod impl/expression TrimFunction [^TrimFunction jsql-expr]
  (let [trim-spec       (helpers/convert-trim-spec (jsql/get-trim-spec jsql-expr))
        from-expression (helpers/convert-column (jsql/get-from-expression jsql-expr))
        expression      (impl/expression->honey (jsql/get-expression jsql-expr))
        expressions     (cond-> []
                          trim-spec (conj trim-spec)
                          expression (conj expression)
                          from-expression (concat [(helpers/make-inline-sql-keyword "from") from-expression]))
        fn-keyword      "trim"]
    (create-honey-fn fn-keyword expressions)))

(defmethod impl/expression RegExpMatchOperator [^RegExpMatchOperator jsql-expr]
  (jsql-expr->raw-honey jsql-expr))

(defmethod impl/expression Column [jsql-expr]
  (helpers/convert-column jsql-expr))

(defmethod impl/expression AllColumns [^AllColumns _jsql-expr]
  :*)

(defmethod impl/expression StringValue [^StringValue jsql-expr]
  (jsql/get-value jsql-expr))

(defmethod impl/expression LongValue [^LongValue jsql-expr]
  (jsql/get-value jsql-expr))

(defmethod impl/expression DoubleValue [^DoubleValue jsql-expr]
  (jsql/get-value jsql-expr))

(defn generic-fn->honey [^Function jsql-function]
  (let [fn->keyword (-> (.getName jsql-function)
                        (str/lower-case))
        distinct?   (jsql/is-distinct? jsql-function)
        params      (helpers/handle-params jsql-function)]
    (create-honey-fn fn->keyword params distinct?)))

(defmethod impl/function->honey :default [jsql-function]
  (try (generic-fn->honey jsql-function)
       (catch Exception e
         (throw
           (IllegalArgumentException.
             (str "Unsupported JSQL Function type: " (.getName jsql-function)))))))