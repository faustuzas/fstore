PROTO_FILES = ./raft/raftpb/*.proto

gen_proto:
	protoc \
		--gofast_out=. \
		--proto_path=$(GOPATH)/src:$(GOPATH)/src/github.com/gogo/protobuf/protobuf:. \
		$(PROTO_FILES)