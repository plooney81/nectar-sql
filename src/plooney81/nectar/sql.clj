(ns plooney81.nectar.sql
  (:require [honey.sql :as honey]
            [honey.sql.pg-ops]
            [plooney81.nectar.jsql :as jsql]
            [plooney81.nectar.sql.expression]
            [plooney81.nectar.sql.impl :as impl]
            [plooney81.nectar.sql.insert]
            [plooney81.nectar.sql.select]
            [plooney81.nectar.sql.select-item]
            [plooney81.nectar.sql.set-operation]
            [plooney81.nectar.sql.update]
            [plooney81.nectar.sql.delete])
  (:import (net.sf.jsqlparser.statement.delete Delete)
           (net.sf.jsqlparser.statement.insert Insert)
           (net.sf.jsqlparser.statement.select Select)
           (net.sf.jsqlparser.statement.update Update)))

(defmethod impl/jsql->honey-adapter :default [_honey jsql]
  (throw (IllegalArgumentException.
           (str "Unsupported type: For jsql->honey-adapter " (.getClass jsql)))))

(defmethod impl/jsql->honey-adapter Select [honey jsql]
  (impl/select->honey honey jsql))

(defmethod impl/jsql->honey-adapter Insert [honey jsql]
  (impl/insert->honey honey jsql))

(defmethod impl/jsql->honey-adapter Update [honey jsql]
  (impl/update->honey honey jsql))

(defmethod impl/jsql->honey-adapter Delete [honey jsql]
  (impl/delete->honey honey jsql))

(defn ripen
  "Process of turning nectar into honey. Accepts a raw-sql string and returns a honeysql map."
  [raw-sql]
  (impl/jsql->honey-adapter {} (jsql/to-nectar raw-sql)))

(comment
  (do
    (require '[honey.sql :as honey])
    (defn around-the-horn [sql-string]
      (-> (plooney81.nectar.sql/ripen sql-string)
          (honey/format {:inline true :pretty true}))))

  (around-the-horn "SELECT json_column -> 'name' -> 'another'")

  (around-the-horn "INSERT INTO users AS u (id, username, email) VALUES (1, 'john_doe', 'john@example.com')")

  (around-the-horn "INSERT INTO users AS u (id, username, email) VALUES (1, 'john_doe', 'john@example.com'), (2, 'admin', 'admin@example.com')")

  (around-the-horn "SELECT CASE WHEN title_ref.title_id IS NOT NULL THEN title_ref.title_name ELSE person.manual_title END AS title FROM person LEFT JOIN title_reference title_ref ON person.title_id = title_ref.title_id;")

  (around-the-horn "SELECT DISTINCT col_name COLLATE latin1_bin FROM X")

  (around-the-horn "WITH stuff AS MATERIALIZED (SELECT * FROM table) SELECT * FROM stuff")

  (ripen "WITH stuff AS MATERIALIZED (SELECT * FROM table) SELECT * FROM stuff")
  

  (ripen "-- Define the complex logic in a CTE first
WITH UserAggregates AS (
    SELECT 
        user_id,
        SUM(amount) AS total_spent,
        COUNT(transaction_id) AS transaction_count,
        MAX(transaction_date) AS last_purchase
    FROM Transactions
    GROUP BY user_id
)
-- Perform the multi-column update with conditional logic
UPDATE u
SET 
    u.status = CASE 
        WHEN ua.total_spent > 10000 THEN 'Platinum'
        WHEN ua.total_spent > 5000 THEN 'Gold'
        ELSE 'Silver'
    END,
    u.last_active = ua.last_purchase
FROM Users u
INNER JOIN UserAggregates ua ON u.id = ua.user_id
WHERE u.is_active = 1
  AND ua.transaction_count > 0;")


  (ripen "INSERT INTO Employees (EmployeeID, EmployeeName, Age, Department)
SELECT 101, 'Alice Corp', 29, 'Engineering'
UNION ALL
SELECT 102, 'Bob Labs', 35, 'Research'
UNION ALL
SELECT 103, 'Charlie Systems', 42, 'Operations'
UNION ALL
SELECT 
    (SELECT MAX(EmployeeID) FROM Employees) + 1, 
    'Dynamic User', 
    (SELECT AVG(Age) FROM Employees), 
    'Automated Testing';")
  
  (ripen "INSERT INTO Priority_Rewards (customer_id, customer_name, reward_level)
SELECT 
    c.customer_id, 
    c.name, 
    'Gold'
FROM Customers c
WHERE c.customer_id IN (
    -- Subquery Level 1: Find customers with many orders
    SELECT o.customer_id 
    FROM Orders o 
    WHERE o.order_id IN (
        -- Subquery Level 2: Nested even deeper to filter specific high-value items
        SELECT order_id 
        FROM Order_Items 
        WHERE price > 500
    )
    GROUP BY o.customer_id
    HAVING COUNT(o.order_id) > 5
);")
  )
