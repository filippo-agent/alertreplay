.PHONY: build test integration tools
build:
	go build -o alertreplay .
test:
	go test ./...
integration:
	ALERTREPLAY_INTEGRATION=1 go test ./integration -v -count=1
tools:
	./scripts/fetch-prometheus.sh
