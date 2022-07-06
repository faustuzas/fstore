package transport

import (
	"fmt"
	"io"

	pb "github.com/faustuzas/distributed-kv/raft/raftpb"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
)

type Encoder interface {
	Encode(w io.Writer, msg *pb.Message) error
	Decode(r io.Reader, msg *pb.Message) error

	ContentType() string
}

type jsonEncoder struct {
	enc jsonpb.Marshaler
}

func NewJsonEncoder() Encoder {
	return &jsonEncoder{
		enc: jsonpb.Marshaler{
			EnumsAsInts: true,
		},
	}
}

func (e *jsonEncoder) Encode(w io.Writer, msg *pb.Message) error {
	return e.enc.Marshal(w, msg)
}

func (e *jsonEncoder) Decode(r io.Reader, msg *pb.Message) error {
	return jsonpb.Unmarshal(r, msg)
}

func (e *jsonEncoder) ContentType() string {
	return "application/json"
}

type protobufEncoder struct {
}

func NewProtobufEncoder() Encoder {
	return &protobufEncoder{}
}

func (e *protobufEncoder) Encode(w io.Writer, msg *pb.Message) error {
	bytes, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("marshaling message: %w", err)
	}

	if _, err = w.Write(bytes); err != nil {
		return fmt.Errorf("writing message bytes: %w", err)
	}

	return nil
}

func (e *protobufEncoder) Decode(r io.Reader, msg *pb.Message) error {
	bytes, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("reading bytes: %w", err)
	}

	return proto.Unmarshal(bytes, msg)
}

func (e *protobufEncoder) ContentType() string {
	return "application/protobuf"
}
