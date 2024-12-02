(ns plooney81.select-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [honey.sql :as honey]
            [honey.sql.helpers :as sql]
            [plooney81.nectar.sql :as nsql]))

(defn- honey->text [honeysql]
  (-> (honey/format honeysql {:inline true :pretty true})
      first
      str/trim))

(defn test-nectar [description raw-sql expected-honey]
  (let [nectar (nsql/ripen raw-sql)]
    (testing description
      ;; tests that ripen outputs expected-honey
      (is (= nectar expected-honey))
      ;; tests that converting the nectar back to raw-sql gets us our original raw-sql
      (is (= (honey->text nectar) raw-sql)))))

(deftest simple-selects
  (test-nectar
    "simple-select"
    (str "SELECT *, 1, 'some-string', some_column\n"
         "FROM first_table\n"
         "WHERE something = another_thing")
    {:select [:* 1 "some-string" :some_column]
     :from   [:first_table]
     :where  [:= :something :another_thing]})
  (test-nectar
    "distinct selects"
    (str "SELECT DISTINCT col_1, col_2\n"
         "FROM first_table\n"
         "WHERE something = another_thing")
    {:select-distinct [:col_1 :col_2]
     :from            [:first_table]
     :where           [:= :something :another_thing]})
  (test-nectar
    "multi-column select"
    (str "SELECT first_column, second_column\n"
         "FROM first_table\n"
         "WHERE something = another_thing")
    {:select [:first_column :second_column]
     :from   [:first_table]
     :where  [:= :something :another_thing]})
  (test-nectar
    "AND clause with NULL handling"
    (str "SELECT first_column, second_column\n"
         "FROM first_table\n"
         "WHERE (something = another_thing) AND (something_else IS NULL)")
    {:select [:first_column :second_column]
     :from   [:first_table]
     :where  [:and [:= :something :another_thing]
              [:= :something_else nil]]})
  (test-nectar
    "AND clause with NOT NULL handling"
    (str "SELECT first_column, second_column\n"
         "FROM first_table\n"
         "WHERE (something = another_thing) AND (something_else IS NOT NULL)")
    {:select [:first_column :second_column]
     :from   [:first_table]
     :where  [:and [:= :something :another_thing]
              [:not= :something_else nil]]}))

(deftest logical-operators
  (test-nectar
    "One logical operator for more than two clauses"
    (str "SELECT first_column, second_column\n"
         "FROM first_table\n"
         "WHERE (something = another_thing) "
         "AND (something_else IS NOT NULL) "
         "AND (one_last_thing IS NULL)")
    {:select [:first_column :second_column]
     :from   [:first_table]
     :where  [:and
              [:= :something :another_thing]
              [:not= :something_else nil]
              [:= :one_last_thing nil]]})
  (test-nectar
    "OR Clause"
    (str "SELECT first_column, second_column\n"
         "FROM first_table\n"
         "WHERE (something = another_thing) "
         "OR (something_else IS NOT NULL)")
    {:select [:first_column :second_column]
     :from   [:first_table]
     :where  [:or [:= :something :another_thing]
              [:not= :something_else nil]]})
  (test-nectar
    "Mixing AND and OR Clauses"
    (str "SELECT first_column, second_column\n"
         "FROM first_table\n"
         "WHERE ((something = another_thing)"
         " AND (something_else IS NOT NULL))"
         " OR (one_more_thing IS NOT NULL)")
    {:select [:first_column :second_column]
     :from   [:first_table]
     :where  [:or [:and [:= :something :another_thing]
                   [:not= :something_else nil]]
              [:not= :one_more_thing nil]]})
  (test-nectar
    "Greater than/equals and less than/equals support"
    (str "SELECT ft.first_column, ft.second_column\n"
         "FROM first_table AS ft\n"
         "WHERE (ft.something >= ft.another_thing) "
         "AND (ft.something > ft.blippity) "
         "AND (ft.something_else <= ft.skippity) "
         "AND (ft.something_once < ft.skoopity)")
    {:select [:ft.first_column :ft.second_column]
     :from   [[:first_table :ft]]
     :where  [:and
              [:>= :ft.something :ft.another_thing]
              [:> :ft.something :ft.blippity]
              [:<= :ft.something_else :ft.skippity]
              [:< :ft.something_once :ft.skoopity]]})
  (test-nectar
    "LIKE and ILIKE support"
    (str "SELECT ft.first_column, ft.second_column\n"
         "FROM first_table AS ft\n"
         "WHERE (ft.something LIKE 'skoopity') "
         "AND (ft.something_else ILIKE 'skippity') "
         "AND (ft.something_more NOT LIKE 'bloopity') "
         "AND (ft.something_final NOT ILIKE 'blippity')")
    {:select [:ft.first_column :ft.second_column]
     :from   [[:first_table :ft]]
     :where  [:and
              [:like :ft.something "skoopity"]
              [:ilike :ft.something_else "skippity"]
              [:not-like :ft.something_more "bloopity"]
              [:not-ilike :ft.something_final "blippity"]]}))

(deftest aliasing
  (test-nectar
    "Added an alias"
    (str "SELECT ft.first_column, ft.second_column\n"
         "FROM first_table AS ft\n"
         "WHERE (ft.something = ft.another_thing) "
         "AND (ft.something_else IS NOT NULL)")
    {:select [:ft.first_column :ft.second_column]
     :from   [[:first_table :ft]]
     :where  [:and [:= :ft.something :ft.another_thing]
              [:not= :ft.something_else nil]]})
  (test-nectar
    "Select statement aliases"
    (str "SELECT ft.first_column AS my_first\n"
         "FROM first_table AS ft")
    {:select [[:ft.first_column :my_first]]
     :from   [[:first_table :ft]]})
  (test-nectar
    "Schema with an alias"
    (str "SELECT ft.first_column AS my_first\n"
         "FROM some_schema.first_table AS ft")
    {:select [[:ft.first_column :my_first]]
     :from   [[:some_schema.first_table :ft]]}))

(deftest grouping-and-ordering
  (test-nectar
    "GROUP BY/ORDER BY support"
    (str "SELECT ft.first_column, ft.second_column\n"
         "FROM first_table AS ft\n"
         "WHERE ft.something ILIKE ft.another_thing\n"
         "GROUP BY ft.first_group, ft.second_group\n"
         "ORDER BY ft.first_order ASC, ft.second_order DESC")
    {:select   [:ft.first_column :ft.second_column],
     :from     [[:first_table :ft]],
     :where    [:ilike :ft.something :ft.another_thing],
     :group-by [:ft.first_group :ft.second_group],
     :order-by [[:ft.first_order :asc] [:ft.second_order :desc]]})
  (test-nectar
    "LIMIT/OFFSET support"
    (str "SELECT ft.first_column, ft.second_column\n"
         "FROM first_table AS ft\n"
         "WHERE ft.something ILIKE ft.another_thing\n"
         "LIMIT 10\n"
         "OFFSET 0")
    {:select [:ft.first_column :ft.second_column],
     :from   [[:first_table :ft]],
     :where  [:ilike :ft.something :ft.another_thing],
     :limit  10,
     :offset 0}))

(deftest joins
  (test-nectar
    "JOIN Support"
    (str "SELECT ft.first_column, st.second_column\n"
         "FROM first_table AS ft\n"
         "LEFT JOIN third_table AS tt "
         "ON tt.first_column = ft.first_column\n"
         "INNER JOIN fourth_table AS four_t "
         "ON four_t.first_column = ft.first_column\n"
         "FULL JOIN second_table AS st "
         "ON (st.first_column = ft.first_column) "
         "AND (st.second_column = ft.second_column)")
    {:select     [:ft.first_column :st.second_column],
     :from       [[:first_table :ft]],
     :full-join  [[:second_table :st] [:and [:= :st.first_column :ft.first_column] [:= :st.second_column :ft.second_column]]],
     :left-join  [[:third_table :tt] [:= :tt.first_column :ft.first_column]],
     :inner-join [[:fourth_table :four_t] [:= :four_t.first_column :ft.first_column]]})
  (testing "More granular join statements"
    (let [inner            "SELECT * FROM table_1 AS t1 INNER JOIN table_2 AS t2 ON t1.first = t2.first"
          regular-left     "SELECT * FROM table_1 AS t1 LEFT JOIN table_2 AS t2 on t1.first = t2.first"
          outer-left       "SELECT * FROM table_1 AS t1 LEFT OUTER JOIN table_2 AS t2 on t1.first = t2.first"
          regular-right    "SELECT * FROM table_1 AS t1 RIGHT JOIN table_2 AS t2 on t1.first = t2.first"
          outer-right      "SELECT * FROM table_1 AS t1 RIGHT OUTER JOIN table_2 AS t2 on t1.first = t2.first"
          full             "SELECT * FROM table_1 AS t1 FULL OUTER JOIN table_2 AS t2 on t1.first = t2.first"
          outer            "SELECT * FROM table_1 AS t1 OUTER JOIN table_2 AS t2 on t1.first = t2.first"
          cross            "SELECT * FROM table_1 AS t1 CROSS JOIN table_2 AS t2 on t1.first = t2.first"
          simple           "SELECT * FROM table_1 AS t1 JOIN table_2 AS t2 on t1.first = t2.first"
          shared-honey     (-> (sql/select :*)
                               (sql/from [:table_1 :t1]))
          shared-join-args (list [:table_2 :t2] [:= :t1.first :t2.first])]
      (are [sql-string honey-join-fn]
        (= (nsql/ripen sql-string) (apply honey-join-fn shared-honey shared-join-args))
        inner sql/inner-join
        regular-left sql/left-join
        outer-left sql/left-join
        regular-right sql/right-join
        outer-right sql/right-join
        full sql/full-join
        outer sql/outer-join
        cross sql/cross-join
        simple sql/join)))
  (test-nectar
    "Set operation list support (UNION, INTERSECT, EXCEPT)"
    (str "SELECT ft.first_column, ft.second_column FROM first_table AS ft "
         "UNION "
         "SELECT st.first_column, st.second_column FROM second_table AS st "
         "UNION ALL "
         "SELECT tt.first_column, tt.second_column FROM third_table AS tt "
         "INTERSECT "
         "SELECT ft.first_column, ft.second_column FROM fourth_table AS ft "
         "EXCEPT "
         "SELECT fft.first_column, fft.second_column FROM fifth_table AS fft\n"
         "ORDER BY fft.first_column ASC")
    {:except   [{:intersect [{:union-all [{:union [{:select [:ft.first_column :ft.second_column], :from [[:first_table :ft]]}
                                                   {:select [:st.first_column :st.second_column], :from [[:second_table :st]]}]}
                                          {:select [:tt.first_column :tt.second_column], :from [[:third_table :tt]]}]}
                             {:select [:ft.first_column :ft.second_column], :from [[:fourth_table :ft]]}]}
                {:select [:fft.first_column :fft.second_column], :from [[:fifth_table :fft]]}],
     :order-by [[:fft.first_column :asc]]})
  (test-nectar
    "Joins with USING statement"
    (str "SELECT *\n"
         "FROM employees\n"
         "INNER JOIN departments AS d USING (d.department_id) "
         "INNER JOIN compensation AS c USING (c.department_id, c.employee_id)")
    {:select     [:*]
     :from       [:employees]
     :inner-join [[:departments :d] [:using :d.department_id]
                  [:compensation :c] [:using :c.department_id :c.employee_id]]}))

(deftest mathematical-operations
  (test-nectar
    "Math Things"
    (str "SELECT (first_column - second_column) / 2 AS something, "
         "(first_column + second_column) * ((third_column - fourth_column) / fifth_column) AS most_math, "
         "first_column % second_column AS modulo_example\n"
         "FROM first_table AS ft")
    {:select [[[:/ [[:- :first_column :second_column]] 2] :something]
              [[:* [:+ :first_column :second_column] [:/ [[:- :third_column :fourth_column]] :fifth_column]] :most_math]
              [[:% :first_column :second_column] :modulo_example]],
     :from   [[:first_table :ft]]})
  (test-nectar
    "Math with nested operators"
    (str "SELECT (first_column - second_column) / 2 AS something, "
         "first_column + ((second_column + third_column) / fourth_column) AS another\n"
         "FROM first_table AS ft")
    {:select [[[:/ [[:- :first_column :second_column]] 2] :something]
              [[:+ :first_column [:/ [:+ :second_column :third_column] :fourth_column]] :another]],
     :from   [[:first_table :ft]]})
  (testing "Signed expression and RegEx Match Operator"
    (let [regex    "SELECT 'hello' ~ '.*lo'"
          negative "SELECT (1 - -42) AS negative_addition"
          positive "SELECT 1 + +42 AS positive_addition"
          bitwise  "SELECT ~1 AS bitwise_example"]
      (are [sql-string honey]
        (= (nsql/ripen sql-string) honey)
        regex {:select [[[:raw "'hello' ~ '.*lo'"]]]}
        negative {:select [[[[:- 1 -42]] :negative_addition]]}
        positive {:select [[[:+ 1 42] :positive_addition]]}
        bitwise {:select [[[:raw "~1"] :bitwise_example]]})
      (are [sql-string converted-sql-string]
        (= (honey->text (nsql/ripen sql-string)) converted-sql-string)
        regex regex
        negative negative
        positive "SELECT 1 + 42 AS positive_addition"       ;; Removes the unnecessary +
        bitwise bitwise))))

(deftest functions
  (test-nectar
    "Aggregate functions"
    (str "SELECT AVG(ft.first_column) AS first_column_avg\n"
         "FROM first_table AS ft")
    {:select [[[:avg :ft.first_column] :first_column_avg]],
     :from   [[:first_table :ft]]})
  (testing "More granular Aggregate Function Testing"
    (let [avg-string     "SELECT AVG(*) AS some_avg\nFROM table_1 AS t1"
          count-distinct "SELECT COUNT(DISTINCT something) AS some_distinct_count\nFROM table_1 AS t1"
          count-string   "SELECT COUNT(*) AS some_count\nFROM table_1 AS t1"
          max-string     "SELECT MAX(*) AS some_max\nFROM table_1 AS t1"
          min-string     "SELECT MIN(*) AS some_min\nFROM table_1 AS t1"
          sum-string     "SELECT SUM(*) AS some_sum\nFROM table_1 AS t1"
          shared-honey   (sql/from [:table_1 :t1])]
      (are [sql-string honey-select-args]
        (= (nsql/ripen sql-string) (apply sql/select shared-honey honey-select-args))
        avg-string [[[:avg :*] :some_avg]]
        count-distinct [[[:count [:distinct :something]] :some_distinct_count]]
        count-string [[[:count :*] :some_count]]
        max-string [[[:max :*] :some_max]]
        min-string [[[:min :*] :some_min]]
        sum-string [[[:sum :*] :some_sum]])
      (are [sql-string converted-sql-string]
        (= (honey->text (nsql/ripen sql-string)) converted-sql-string)
        avg-string avg-string
        count-string count-string
        max-string max-string
        min-string min-string
        sum-string sum-string)))
  (testing "String Function Testing"
    (let [concat       "SELECT CONCAT(first_name, ' ', last_name) AS full_name\nFROM table_1 AS t1"
          length       "SELECT LENGTH(first_name) AS first_name_length\nFROM table_1 AS t1"
          lower        "SELECT LOWER(first_name) AS lower_first\nFROM table_1 AS t1"
          upper        "SELECT UPPER(first_name) AS upper_first\nFROM table_1 AS t1"
          substring    "SELECT SUBSTRING(first_name FROM 1 FOR 3) AS sub_first\nFROM table_1 AS t1"
          trim         "SELECT TRIM(first_name) AS trim_first\nFROM table_1 AS t1"
          trim-2       "SELECT TRIM(LEADING 'x' FROM some_table) AS alternative_trim\nFROM table_1 AS t1"
          shared-honey (sql/from [:table_1 :t1])]
      (are [sql-string honey-select-args]
        (= (nsql/ripen sql-string) (apply sql/select shared-honey honey-select-args))
        concat [[[:concat :first_name " " :last_name] :full_name]]
        length [[[:length :first_name] :first_name_length]]
        lower [[[:lower :first_name] :lower_first]]
        upper [[[:upper :first_name] :upper_first]]
        substring [[[:substring :first_name :!from 1 :!for 3] :sub_first]]
        trim [[[:trim :first_name] :trim_first]]
        trim-2 [[[:trim :!leading "x" :!from :some_table] :alternative_trim]])
      (are [sql-string converted-sql-string]
        (= (honey->text (nsql/ripen sql-string)) converted-sql-string)
        concat concat
        length length
        lower lower
        upper upper
        substring substring
        trim trim
        trim-2 trim-2)))
  (testing "Date/Time Functions"
    (let [current-date "SELECT CURRENT_DATE AS my_date"
          current-time "SELECT CURRENT_TIME"
          date-part    "SELECT DATE_PART('year', date_of_birth)\nFROM table_1"
          age          "SELECT AGE(date_of_birth)\nFROM table_1"]
      (are [sql-string honey]
        (= (nsql/ripen sql-string) honey)
        current-date {:select [[[:raw "CURRENT_DATE"] :my_date]]}
        current-time {:select [[[:raw "CURRENT_TIME"]]]}
        date-part {:select [[[:date_part "year" :date_of_birth]]] :from [:table_1]}
        age {:select [[[:age :date_of_birth]]] :from [:table_1]})
      (are [sql-string converted-sql-string]
        (= (honey->text (nsql/ripen sql-string)) converted-sql-string)
        current-date current-date
        current-time current-time
        date-part date-part
        age age)))
  (test-nectar
    "Math Functions"
    (str "SELECT ABS(-42), "
         "CEIL(4.3) AS my_ceil, "
         "FLOOR(4.3) AS my_floor, "
         "ROUND(4.5) AS my_round, "
         "RANDOM(), "
         "RANDOM() AS random_2\n"
         "FROM first_table AS ft")
    {:select [[[:abs -42]]
              [[:ceil 4.3] :my_ceil]
              [[:floor 4.3] :my_floor]
              [[:round 4.5] :my_round]
              [:%random]
              [:%random :random_2]],
     :from   [[:first_table :ft]]})
  (test-nectar
    "JSON Functions"
    (str "SELECT JSONB_EXISTS(t1.data, 'key') AS jf1, "
         "JSONB_EXTRACT_PATH(t1.data, 'key', 'subkey') AS jf2, "
         "TO_JSON(t1.row) AS jf3\n"
         "FROM table_1 AS t1")
    {:select [[[:jsonb_exists :t1.data "key"] :jf1]
              [[:jsonb_extract_path :t1.data "key" "subkey"] :jf2]
              [[:to_json :t1.row] :jf3]],
     :from   [[:table_1 :t1]]})
  (test-nectar
    "Analytical Expressions"
    (str "SELECT id, "
         "AVG(salary) OVER (PARTITION BY department ORDER BY designation ASC) AS average, "
         "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary ASC) AS median_salary, "
         "COUNT(*) FILTER (WHERE something > 5) AS high_salary_employees\n"
         "FROM employee")
    {:select [:id
              [[:over [[:avg :salary] {:partition-by [:department], :order-by [[:designation :asc]]} :average]]]
              [[:within-group [:percentile_cont 0.5] {:order-by [[:salary :asc]]}] :median_salary]
              [[:filter [:count :*] {:where `(:> :something 5)}] :high_salary_employees]]
     :from   [:employee]})
  (testing "Combining Within Group and Over Analytical Expressions"
    (let [sql-string (str "SELECT percentile_cont(0.5)
                                  WITHIN GROUP (ORDER BY salary)
                                  OVER (PARTITION BY department) AS median_salary
                             FROM employee")]
      (is (= (nsql/ripen sql-string)
             {:select [[[:raw "percentile_cont(0.5) WITHIN GROUP (ORDER BY salary) OVER (PARTITION BY department )"] :median_salary]],
              :from   [:employee]}))
      (is (= (honey->text (nsql/ripen sql-string))
             (str "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY salary) "
                  "OVER (PARTITION BY department ) AS median_salary\n"
                  "FROM employee")))))
  (test-nectar
    "Window Functions And Analytical Expressions"
    (str "SELECT name, "
         "ROW_NUMBER() OVER (ORDER BY e.salary ASC) AS salary_row, "
         "RANK() OVER (ORDER BY e.salary ASC) AS salary_rank, "
         "DENSE_RANK() OVER (ORDER BY e.salary ASC) AS salary_d_rank\n"
         "FROM employees AS e")
    {:select [:name
              [[:over
                [:%row_number {:order-by [[:e.salary :asc]]} :salary_row]
                [:%rank {:order-by [[:e.salary :asc]]} :salary_rank]
                [:%dense_rank {:order-by [[:e.salary :asc]]} :salary_d_rank]]]],
     :from   [[:employees :e]]})
  (test-nectar
    "Conditional Functions"
    (str "SELECT COALESCE(middle_name, 'N/A') AS new_middle, "
         "NULLIF(column1, column2)\n"
         "FROM employees")
    {:select [[[:coalesce :middle_name "N/A"] :new_middle]
              [[:nullif :column1 :column2]]]
     :from   [:employees]})
  (test-nectar
    "Array Functions"
    (str "SELECT ARRAY_AGG(e.salary) AS sal_agg, "
         "UNNEST(e.array_column) AS unnest_sal, "
         "ARRAY_APPEND(e.array_column, 'new_element') AS new_array_column\n"
         "FROM employees AS e")
    {:select [[[:array_agg :e.salary] :sal_agg]
              [[:unnest :e.array_column] :unnest_sal]
              [[:array_append :e.array_column "new_element"] :new_array_column]],
     :from   [[:employees :e]]})
  (test-nectar
    "Geometric Functions"
    (str "SELECT POINT(1, 2) AS new_point, "
         "LINE(POINT(1, 2), POINT(3, 4)) AS new_line_segment, "
         "LSEG(POINT(1, 2), POINT(3, 4)) AS another_line_segment\n"
         "FROM arbitrary AS a")
    {:select [[[:point 1 2] :new_point]
              [[:line [:point 1 2] [:point 3 4]] :new_line_segment]
              [[:lseg [:point 1 2] [:point 3 4]] :another_line_segment]],
     :from   [[:arbitrary :a]]})
  (testing "Casts"
    (let [to-int-fn         "SELECT CAST('123' AS INTEGER)"
          to-text-fn        "SELECT CAST(123 AS TEXT)"
          to-int-type-conv  "SELECT '123'::INTEGER"
          to-text-type-conv "SELECT 123::TEXT"
          to-int-honey      {:select [[[:cast "123" :integer]]]}
          to-text-honey     {:select [[[:cast 123 :text]]]}]
      (are [sql-string honey]
        (= (nsql/ripen sql-string) honey)
        to-int-fn to-int-honey
        to-text-fn to-text-honey
        to-int-type-conv to-int-honey
        to-text-type-conv to-text-honey)
      (are [sql-string converted-sql-string]
        (= (honey->text (nsql/ripen sql-string)) converted-sql-string)
        to-int-fn to-int-fn
        to-text-fn to-text-fn
        to-int-type-conv to-int-fn
        to-text-type-conv to-text-fn)))
  (test-nectar
    "Network Address Functions"
    (str "SELECT INET('192.168.1.1') AS ipv4_address, "
         "INET('192.168.1') << INET('192.168.1.0/24') AS subnet_query, "
         "CIDR('2001:db8::') AS host_address\n"
         "FROM something AS s")
    {:select [[[:inet "192.168.1.1"] :ipv4_address]
              [[:<< [:inet "192.168.1"] [:inet "192.168.1.0/24"]] :subnet_query]
              [[:cidr "2001:db8::"] :host_address]],
     :from   [[:something :s]]})
  (test-nectar
    "Full-text search functions"
    (str "SELECT TO_TSVECTOR('english', 'The quick brown fox jumps over the lazy dog') AS tokens, "
         "TO_TSQUERY('english', 'quick & fox') AS query_output, "
         "PLAINTO_TSQUERY('english', 'The quick brown fox') AS plain_ts_query_output\n"
         "FROM something AS s")
    {:select [[[:to_tsvector "english" "The quick brown fox jumps over the lazy dog"] :tokens]
              [[:to_tsquery "english" "quick & fox"] :query_output]
              [[:plainto_tsquery "english" "The quick brown fox"] :plain_ts_query_output]],
     :from   [[:something :s]]})
  (test-nectar
    "MISC Functions"
    (str "SELECT VERSION() AS v, "
         "PG_BACKEND_PID() AS pid\n"
         "FROM something AS s")
    {:select [[:%version :v] [:%pg_backend_pid :pid]], :from [[:something :s]]}))

(deftest windowing
  (test-nectar
    "Trickier window example"
    (str "SELECT employee_id, employee_name, quarter, year, sales_amount, "
         "SUM(sales_amount) OVER ("
         "PARTITION BY employee_id "
         "ORDER BY sales_amount "
         "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS current_and_previous_2, "
         "SUM(value) OVER (ORDER BY value RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total\n"
         "FROM employer_performance")
    {:select [:employee_id
              :employee_name
              :quarter
              :year
              :sales_amount
              [[:raw
                "SUM(sales_amount) OVER (PARTITION BY employee_id ORDER BY sales_amount ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"]
               :current_and_previous_2]
              [[:raw "SUM(value) OVER (ORDER BY value RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"] :running_total]],
     :from   [:employer_performance]})
  (test-nectar
    "Using the actual WINDOW keyword"
    (str "SELECT department, employee, salary, "
         "SUM(salary) OVER rolling_window AS rolling_sum, "
         "AVG(salary) OVER department_window AS dept_avg_salary\n"
         "FROM employees\n"
         "WINDOW rolling_window AS (PARTITION BY department ORDER BY salary ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), "
         "department_window AS (PARTITION BY department)")
    {:select [:department
              :employee
              :salary
              [[:over
                [[:sum :salary] :rolling_window :rolling_sum]
                [[:avg :salary] :department_window :dept_avg_salary]]]],
     :from   [:employees],
     :window [:rolling_window [[:raw "(PARTITION BY department ORDER BY salary ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"]]
              :department_window {:partition-by [:department]}]}))

(deftest cte-and-subqueries
  (test-nectar
    "Regular CTE"
    (str "WITH sales AS (SELECT employee_id, SUM(sales_amount) AS total_sales FROM sales GROUP BY employee_id), "
         "top_sales AS (SELECT employee_id, total_sales FROM sales WHERE total_sales > 10000)\n"
         "SELECT e.employee_name, t.total_sales\nFROM employees AS e\nINNER JOIN top_sales AS t ON e.employee_id = t.employee_id")
    {:with       [[:sales {:select [:employee_id [[:sum :sales_amount] :total_sales]], :from [:sales], :group-by [:employee_id]}]
                  [:top_sales {:select [:employee_id :total_sales], :from [:sales], :where [:> :total_sales 10000]}]],
     :select     [:e.employee_name :t.total_sales],
     :from       [[:employees :e]],
     :inner-join [[:top_sales :t] [:= :e.employee_id :t.employee_id]]})
  (test-nectar
    "Recursive CTE"
    (str "WITH RECURSIVE sales AS (SELECT employee_id, SUM(sales_amount) AS total_sales FROM sales GROUP BY employee_id), "
         "top_sales AS (SELECT employee_id, total_sales FROM sales WHERE total_sales > 10000)\n"
         "SELECT e.employee_name, t.total_sales\nFROM employees AS e\nINNER JOIN top_sales AS t ON e.employee_id = t.employee_id")
    {:with-recursive [[:sales {:select [:employee_id [[:sum :sales_amount] :total_sales]], :from [:sales], :group-by [:employee_id]}]
                      [:top_sales {:select [:employee_id :total_sales], :from [:sales], :where [:> :total_sales 10000]}]],
     :select         [:e.employee_name :t.total_sales],
     :from           [[:employees :e]],
     :inner-join     [[:top_sales :t] [:= :e.employee_id :t.employee_id]]})
  (test-nectar
    "Nested Sub-query"
    (str "SELECT employee_id, first_name, last_name, department_id, "
         "("
         "SELECT department_name FROM departments WHERE departments.department_id = employees.department_id"
         ") AS department_name\n"
         "FROM employees")
    {:select [:employee_id
              :first_name
              :last_name
              :department_id
              [{:select [:department_name],
                :from   [:departments],
                :where  [:= :departments.department_id :employees.department_id]}
               :department_name]],
     :from   [:employees]}))

(deftest more-operators
  (test-nectar
    "String operators"
    (str "SELECT title || ' by ' || author || ' publisher: ' || publisher AS full_title\n"
         "FROM books")
    {:select [[[:|| :title " by " :author " publisher: " :publisher] :full_title]]
     :from   [:books]})
  (test-nectar
    "IN operator"
    (str "SELECT *\nFROM orders\nWHERE (status IN ('pending', 'shipped', 'delivered')) AND (status NOT IN ('bloop', 'skoop'))")
    {:select [:*], :from [:orders], :where [:and [:in :status ["pending" "shipped" "delivered"]]
                                            [:not-in :status ["bloop" "skoop"]]]})
  (test-nectar
    "BETWEEN and NOT BETWEEN operators"
    (str "SELECT *\nFROM orders\nWHERE amount BETWEEN 100 AND 500 AND amount NOT BETWEEN 1 AND 5")
    {:select [:*]
     :from   [:orders]
     :where  [:and
              [:between :amount 100 500]
              [:not-between :amount 1 5]]})
  (test-nectar
    "NOT operator"
    (str "SELECT *\nFROM orders\nWHERE NOT something")
    {:select [:*], :from [:orders], :where [:not :something]}))

(deftest json-operators
  (test-nectar
    "Selection operators"
    (str "SELECT json_column -> 'name', "
         "json_column -> 'name' -> 'another', "
         "CAST('[\"a\", \"b\", \"c\"]' AS JSONB) -> 1, "
         "CAST('{\"name\": \"john\", \"age\": 30}' AS JSONB) ->> 'name'")
    {:select [[[:-> :json_column "name"]]
              [[:-> :json_column "name" "another"]]
              [[:-> [:cast "[\"a\", \"b\", \"c\"]" :jsonb] 1]]
              [[:->> [:cast "{\"name\": \"john\", \"age\": 30}" :jsonb] "name"]]]})
  (testing
    "Existence operators"
    (let [sql-string (str "SELECT json_column ? 'name', "
                          "json_column_2 ?| 'name', "
                          "json_column_3 ?& 'name'")]
      (is (= (nsql/ripen sql-string)
             {:select [[[:? :json_column "name"]]
                       [[:?| :json_column_2 "name"]]
                       [[:?& :json_column_3 "name"]]]}))
      (is (= (honey->text (nsql/ripen sql-string))
             (str "SELECT json_column ?? 'name', "
                  "json_column_2 ??| 'name', "
                  "json_column_3 ??& 'name'")))))
  (test-nectar
    "Containment operators"
    (str "SELECT json_column @> '{\"age\": 30}', "
         "json_column <@ '{\"age\": 30}'")
    {:select [[[(keyword "@>") :json_column "{\"age\": 30}"]]
              [[(keyword "<@") :json_column "{\"age\": 30}"]]]})
  (test-nectar
    "Path operators"
    (str "SELECT (json_column #> '{person, name}') AS name_as_json, "
         "(json_column #>> '{person, name}') AS name_as_text")
    {:select [[[[:#> :json_column "{person, name}"]] :name_as_json]
              [[[:#>> :json_column "{person, name}"]] :name_as_text]]})
  (test-nectar
    "Deletion operators"
    (str "SELECT json_column - 'age'")
    {:select [[[:- :json_column "age"]]]})


  )
