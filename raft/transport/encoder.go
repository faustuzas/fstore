package transport

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
)

type Encoder interface {
	Encode(w io.Writer, msg interface{}) error
	Decode(r io.Reader, s interface{}) error

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

func (e *jsonEncoder) Encode(w io.Writer, msg interface{}) error {
	if m, ok := msg.(proto.Message); ok {
		return e.enc.Marshal(w, m)
	}

	return json.NewEncoder(w).Encode(msg)
}

func (e *jsonEncoder) Decode(r io.Reader, s interface{}) error {
	if m, ok := s.(proto.Message); ok {
		return jsonpb.Unmarshal(r, m)
	}

	return json.NewDecoder(r).Decode(s)
}

func (e *jsonEncoder) ContentType() string {
	return "application/json"
}

type protobufEncoder struct {
}

func NewProtobufEncoder() Encoder {
	return &protobufEncoder{}
}

func (e *protobufEncoder) Encode(w io.Writer, msg interface{}) error {
	m, ok := msg.(proto.Marshaler)
	if !ok {
		return fmt.Errorf("only protobuf struct can be encoded using protobuf encoder")
	}

	bytes, err := m.Marshal()
	if err != nil {
		return fmt.Errorf("marshaling message: %w", err)
	}

	if _, err = w.Write(bytes); err != nil {
		return fmt.Errorf("writing message bytes: %w", err)
	}

	return nil
}

func (e *protobufEncoder) Decode(r io.Reader, s interface{}) error {
	msg, ok := s.(proto.Message)
	if !ok {
		return fmt.Errorf("only protobuf struct can be decoded using protobuf encoder")
	}

	bytes, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("reading bytes: %w", err)
	}

	return proto.Unmarshal(bytes, msg)
}

func (e *protobufEncoder) ContentType() string {
	return "application/protobuf"
}
