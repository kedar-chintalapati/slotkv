package lincheck

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/anishathalye/porcupine"
)

const historyVersion = 1

type historyDocument struct {
	Version    int                `json:"version"`
	Operations []historyOperation `json:"operations"`
}

type historyOperation struct {
	ClientID int           `json:"client_id"`
	Call     int64         `json:"call"`
	Return   int64         `json:"return"`
	Input    historyInput  `json:"input"`
	Output   historyOutput `json:"output"`
}

type historyInput struct {
	Op    Op     `json:"op"`
	Key   []byte `json:"key"`
	Value []byte `json:"value,omitempty"`
}

type historyOutput struct {
	Value   []byte `json:"value,omitempty"`
	Found   bool   `json:"found,omitempty"`
	Existed bool   `json:"existed,omitempty"`
	Unknown bool   `json:"unknown,omitempty"`
}

// MarshalHistory encodes operations from Recorder.Operations as typed JSON.
func MarshalHistory(ops []porcupine.Operation) ([]byte, error) {
	doc := historyDocument{Version: historyVersion, Operations: make([]historyOperation, 0, len(ops))}
	for i, op := range ops {
		in, ok := op.Input.(Input)
		if !ok {
			return nil, fmt.Errorf("lincheck: operation %d input has type %T, want lincheck.Input", i, op.Input)
		}
		out, ok := op.Output.(Output)
		if !ok {
			return nil, fmt.Errorf("lincheck: operation %d output has type %T, want lincheck.Output", i, op.Output)
		}
		doc.Operations = append(doc.Operations, historyOperation{
			ClientID: op.ClientId,
			Call:     op.Call,
			Return:   op.Return,
			Input:    historyInput{Op: in.Op, Key: []byte(in.Key), Value: []byte(in.Value)},
			Output:   historyOutput{Value: []byte(out.Value), Found: out.Found, Existed: out.Existed, Unknown: out.Unknown},
		})
	}
	return json.Marshal(doc)
}

// UnmarshalHistory decodes typed JSON into operations accepted by Check.
func UnmarshalHistory(data []byte) ([]porcupine.Operation, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	var doc historyDocument
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("lincheck: decode history: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("lincheck: history contains multiple JSON values")
		}
		return nil, fmt.Errorf("lincheck: decode trailing history data: %w", err)
	}
	if doc.Version != historyVersion {
		return nil, fmt.Errorf("lincheck: unsupported history version %d", doc.Version)
	}
	ops := make([]porcupine.Operation, 0, len(doc.Operations))
	for i, op := range doc.Operations {
		if op.Input.Op > Delete {
			return nil, fmt.Errorf("lincheck: operation %d has invalid op %d", i, op.Input.Op)
		}
		if op.Call >= op.Return {
			return nil, fmt.Errorf("lincheck: operation %d call %d must precede return %d", i, op.Call, op.Return)
		}
		ops = append(ops, porcupine.Operation{
			ClientId: op.ClientID,
			Call:     op.Call,
			Return:   op.Return,
			Input:    Input{Op: op.Input.Op, Key: string(op.Input.Key), Value: string(op.Input.Value)},
			Output:   Output{Value: string(op.Output.Value), Found: op.Output.Found, Existed: op.Output.Existed, Unknown: op.Output.Unknown},
		})
	}
	return ops, nil
}

// WriteHistory writes one compact JSON history value to w.
func WriteHistory(w io.Writer, ops []porcupine.Operation) error {
	data, err := MarshalHistory(ops)
	if err != nil {
		return err
	}
	n, err := w.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

// ReadHistory reads one compact JSON history value from r.
func ReadHistory(r io.Reader) ([]porcupine.Operation, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return UnmarshalHistory(data)
}
