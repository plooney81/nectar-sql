.PHONY: test repl ci build deploy clean

test:
	clojure -M:test -m cognitect.test-runner

repl:
	clojure -M:test -r

ci:
	clojure -T:build ci

build:
	clojure -T:build ci

deploy:
	clojure -T:build deploy

release: ci deploy

clean:
	rm -rf target
