#!/bin/bash

protoc --proto_path=proto --python_out=build/gen proto/bakdata/corporate/v1/corporate.proto

protoc --proto_path=proto --python_out=build/gen proto/bakdata/corporate/v1/wd_company.proto
