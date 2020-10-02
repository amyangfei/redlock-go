#!/bin/bash -e
#
# Run all tests


PACKAGES=$(go list ./... | grep -vE 'vendor|examples')
FILES=$(find . -name "*.go" | grep -vE "vendor|examples")

echo "Running tests..."
GO111MODULE=on go test -race -count=1 -v -cover ${PACKAGES}

echo "Checking gofmt..."
gofmt -s -l -w ${FILES} 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'

echo "Checking govet..."
go vet -all ${PACKAGES} 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'

# GO111MODULE=off go get github.com/kisielk/errcheck
# echo "errcheck"
# errcheck -blank ${PACKAGES} | grep -v "_test\.go" | awk '{print} END{if(NR>0) {exit 1}}'

GO111MODULE=off go get golang.org/x/lint/golint
echo "Checking golint..."
golint -set_exit_status ${PACKAGES}

echo "Success"
