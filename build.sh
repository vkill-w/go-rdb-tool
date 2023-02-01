#!/usr/bin/env bash

CGO_ENABLED=0  GOOS=linux GOARCH=amd64
go build -o target/go-rdb-tool-darwin ./




CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o target/go-rdb-tool-win.go