init: modules

modules:
	go mod tidy

test:
	gosec ./...
	go fmt ./...
	go test -race -timeout 5s --cover ./...
	go vet ./...

test-integration:
	go test -tags=integration -timeout 20s -cover -coverprofile cover.out ./...

open-cover-report:
	go tool cover -html=cover.out -o cover.html
	open cover.html

lint:
	golangci-lint run ./...

lint-fix:
	golangci-lint run --fix ./...

bump-common-lib:
	go get github.com/slackmgr/types@latest
	go mod tidy
