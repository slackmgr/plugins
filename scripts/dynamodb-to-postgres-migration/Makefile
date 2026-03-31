.PHONY: run dry-run tidy

## run: tidy dependencies, load .env, and execute the migration
run: tidy
	set -a && . ./.env && GOWORK=off go run .

## dry-run: same as run but forces DRYRUN=true regardless of .env
dry-run: tidy
	set -a && . ./.env && GOWORK=off DRYRUN=true go run .

## tidy: update go.mod and go.sum
tidy:
	GOWORK=off go mod tidy
