test:
	CGO_ENABLED=0 go test ./...

test.race:
	CGO_ENABLED=1 go test -race -v ./...

fmt:
	goimports -w -l ./pkg