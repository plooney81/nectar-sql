# nectar-sql

[![Clojars](https://img.shields.io/clojars/v/com.github.plooney81/nectar-sql.svg)](https://clojars.org/com.github.plooney81/nectar-sql)

nectar: A sugary liquid secreted by plants, primarily in their flowers, to attract pollinators like bees, butterflies, and
hummingbirds. It serves as the raw material for honey.

`nectar-sql`: Converts raw SQL strings into HoneySQL data structures. `nectar-sql` takes a raw SQL string, parses it using [JSQLParser][JSqlParser] and converts it into [HoneySQL][honeysql].

## Supported Statements

| Statement  | Support                                        |
|------------|------------------------------------------------|
| `SELECT`   | Joins, CTEs (`WITH` / `WITH RECURSIVE` / `MATERIALIZED`), correlated subqueries, `EXISTS` / `NOT EXISTS`, window functions, aggregates, `GROUP BY` / `HAVING`, `ORDER BY`, `LIMIT` / `OFFSET`, set operations (`UNION`, `INTERSECT`, `EXCEPT`), `CASE WHEN`, `CAST`, JSON operators, `COLLATE`, and more |
| `INSERT`   | `VALUES` (single and multi-row), `INSERT … SELECT`, `INSERT … SELECT … UNION ALL` |
| `UPDATE`   | `SET`, `FROM`, `JOIN`, `WHERE` (including subqueries and `EXISTS`), `ORDER BY`, `LIMIT`, CTEs |
| `DELETE`   | `WHERE` (including subqueries and `EXISTS`), `ORDER BY`, `LIMIT` |

## Installation

Add to your `deps.edn`:

```clojure
com.github.plooney81/nectar-sql {:mvn/version "1.0.32"}
```

## Try It from Your REPL

The Clojure REPL includes functions for [downloading and adding libraries][clj-add-lib] at runtime:

```clojure
user> (add-lib 'com.github.plooney81/nectar-sql {:mvn/version "1.0.32"})
;;==> [com.github.jsqlparser/jsqlparser
;;==>  com.github.plooney81/nectar-sql
;;==>  com.github.seancorfield/honeysql]
```

## Usage

```clojure
(require '[plooney81.nectar.sql :as nsql])

;; SELECT
(nsql/ripen "SELECT * FROM people WHERE age > 25 ORDER BY age DESC")
;;=> {:select   [:*]
;;=>  :from     [:people]
;;=>  :where    [:> :age 25]
;;=>  :order-by [[:age :desc]]}

;; INSERT
(nsql/ripen "INSERT INTO users (id, name) VALUES (1, 'Alice')")
;;=> {:insert-into [:users [:id :name]]
;;=>  :values      [[1 "Alice"]]}

;; UPDATE
(nsql/ripen "UPDATE users SET name = 'Bob' WHERE id = 1")
;;=> {:update :users
;;=>  :set    {:name "Bob"}
;;=>  :where  [:= :id 1]}

;; DELETE
(nsql/ripen "DELETE FROM users WHERE id = 1")
;;=> {:delete-from [:users]
;;=>  :where       [:= :id 1]}
```

This is handy when you have existing SQL and want to know what the HoneySQL equivalent looks like — without trial and error.

## Development

Run tests:

    $ make test

Run the REPL (with test classpath):

    $ make repl

Build a JAR:

    $ make ci

Deploy to Clojars (requires `CLOJARS_USERNAME` and `CLOJARS_PASSWORD`):

    $ make deploy

Or run CI + deploy in one step:

    $ make release

## Versioning

The version is managed manually in the [`version`](version) file. To release a new version:

1. Update `version` with the new version string
2. Add an entry to `CHANGELOG.md`
3. Commit and push to `main` — CI will deploy automatically

## Limitations

- `nectar-sql` relies on [JSQLParser][JSqlParser]. Some SQL features are not yet supported by JSQLParser (e.g. implicit casting in PostgreSQL). See the [list of unsupported features][jsql-doesnt-support].
- Multi-table `DELETE` with `JOIN` (MySQL-style) is not yet supported.

## Credit

This project is proudly sponsored by [Luminare][luminare]!

----
[luminare]: https://luminare.io
[honeysql]: https://github.com/seancorfield/honeysql
[JSqlParser]: https://github.com/JSQLParser/JSqlParser
[jsql-doesnt-support]: https://jsqlparser.github.io/JSqlParser/unsupported.html
[clj-add-lib]: https://github.com/clojure/clojure/blob/13a2f67b91ab81cd109ea3152fce1ae76d212453/src/clj/clojure/repl/deps.clj#L59-L73

## License

Copyright © 2024 Plooney

Distributed under the Eclipse Public License version 1.0.
