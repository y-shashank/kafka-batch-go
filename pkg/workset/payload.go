package workset

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
)

// Payload encoding markers stored alongside the Kafka body in the workset Entry.
// Empty / missing Encoding = legacy raw body (Go json []byte / Ruby base64).
const (
	encodingGzip = "gzip"
	// Compress payloads larger than this; tiny jobs stay uncompressed.
	compressMinBytes = 256
)

// marshalEntryJSON encodes an Entry for Redis, gzip-compressing large payloads
// to shrink workset memory (v2 contract; reclaim dual-reads legacy).
func marshalEntryJSON(e *Entry) ([]byte, error) {
	if e == nil {
		return nil, nil
	}
	out := *e
	if len(out.Payload) >= compressMinBytes && out.Encoding != encodingGzip {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(out.Payload); err != nil {
			_ = zw.Close()
			return json.Marshal(e)
		}
		if err := zw.Close(); err != nil {
			return json.Marshal(e)
		}
		out.Payload = buf.Bytes()
		out.Encoding = encodingGzip
	}
	return json.Marshal(&out)
}

// decodeEntryPayload expands Entry.Payload to the raw Kafka value for reclaim.
func decodeEntryPayload(e *Entry) ([]byte, error) {
	if e == nil {
		return nil, nil
	}
	if e.Encoding != encodingGzip {
		return e.Payload, nil
	}
	zr, err := gzip.NewReader(bytes.NewReader(e.Payload))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}

// PayloadForReclaim returns the Kafka body suitable for markReclaimPayload.
func PayloadForReclaim(e *Entry) ([]byte, error) {
	return decodeEntryPayload(e)
}
