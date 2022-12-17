PROJECT=saifu

printProject:
	echo ${PROJECT}

init:
	go get ./...

generate:
	go generate ./...

test:
	go test -short  ./...

build:
	go build -o ${PROJECT} ./cmd/*.go

docker_run:
	docker run -v `pwd`/saifu.json:/saifu.json -p 4300:4100 jerryenebeli/saifu:main

run:
	./${PROJECT}

build_run:
	make build
	make run

build_test_run:
	make build
	make test
	make run
