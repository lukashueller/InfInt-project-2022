#!/bin/bash

protoc --proto_path=proto --python_out=build/gen proto/rb_announcement.proto

protoc --proto_path=proto --python_out=build/gen proto/wd_company.proto
