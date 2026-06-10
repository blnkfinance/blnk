# Copyright 2024 Blnk Finance Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

PROJECT=blnk

printProject:
	echo ${PROJECT}

init:
	go get ./...

generate:
	go generate ./...

test:
	go test -short  ./...

# Mutation testing on the money-critical fast packages (model, filter).
# Plants hundreds of one-line bugs and re-runs the tests against each one;
# a "LIVED" mutant is a bug the test suite would not catch. Fails when test
# efficacy (killed/viable mutants) drops below the threshold. Takes ~10 min.
# Triage survivors by reading the LIVED lines; model/mutation_killers_test.go
# has examples of boundary tests written to kill them.
# (The threshold is enforced here by parsing the score: gremlins' own
# --threshold-efficacy flag does not affect its exit code.)
MUTATION_THRESHOLD=80
mutate:
	@command -v gremlins >/dev/null 2>&1 || go install github.com/go-gremlins/gremlins/cmd/gremlins@latest
	@for pkg in model internal/filter; do \
		echo "==> mutation testing $$pkg (threshold ${MUTATION_THRESHOLD}%)"; \
		log=/tmp/gremlins-$$(basename $$pkg).log; \
		(cd $$pkg && gremlins unleash --workers 2 --timeout-coefficient 8) | tee $$log; \
		eff=$$(grep -o 'Test efficacy: [0-9.]*' $$log | grep -o '[0-9.]*'); \
		awk -v e="$$eff" -v t="${MUTATION_THRESHOLD}" 'BEGIN { exit (e+0 < t+0) ? 1 : 0 }' \
			|| { echo "MUTATION GATE FAILED: $$pkg efficacy $$eff% is below ${MUTATION_THRESHOLD}% — see LIVED lines in $$log"; exit 1; }; \
		echo "PASS: $$pkg efficacy $$eff%"; \
	done

build:
	go build -o ${PROJECT} ./cmd/*.go

docker_run:
	docker run -v `pwd`/blnk.json:/blnk.json -p 4300:4100 jerryenebeli/blnk:main

run:
	./${PROJECT} start

run_workers:
	./${PROJECT} workers

build_run:
	make build
	make run

build_test_run:
	make build
	make test
	make run

migrate_up:
	./${PROJECT} migrate up

migrate_down:
	./${PROJECT} migrate down

backup:
	./${PROJECT} backup drive

backup_s3:
	./${PROJECT} backup s3