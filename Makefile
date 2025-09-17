all: update build

build:
	go build -o . ./...

build-race:
	go build -race -o . ./...

build-escape:
	go build -gcflags '-m' -o . ./...

update:
	go get -u ./...

.PHONY: all build build-race build-escape update
