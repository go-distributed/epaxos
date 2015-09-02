.PHONY: all
all: message.pb.go

message.pb.go: message.proto
	protoc --proto_path=${GOPATH}/src:${GOPATH}/src/github.com/gogo/protobuf/protobuf:. --gogo_out=. *.proto
