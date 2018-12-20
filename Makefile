all: test

# PHONY used to mitigate conflict with dir name test
.PHONY: test
test:
	go mod tidy
	go fmt ./...
	go vet ./...
	golint ./...
	go test ./...

integration:
	$(info INFO: Running all tests including integration tests. This may take some time.)
	go test ./... -tags=integration

coverage:
	go test ./... -coverprofile=coverage.out
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out

doc:
	godoc -http=":6060"