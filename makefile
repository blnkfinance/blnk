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

run:
	./${PROJECT}

build_run:
	make build
	make run

build_test_run:
	make build
	make test
	make run
