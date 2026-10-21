package materialize

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

var canonicalKeys = []string{
	"canonical_format_version",
	"effective_time",
	"grading_mode",
	"query_id",
	"terminal",
	"top",
	"trigger_event_id",
	"trigger_source_line",
	"trigger_type",
}

func canonicalBaseLine(value []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	var transport map[string]any
	if err := decoder.Decode(&transport); err != nil {
		return nil, err
	}
	version, ok := transport["canonical_format_version"].(json.Number)
	if !ok || version.String() != "1" {
		return nil, errors.New("unsupported canonical_format_version")
	}
	canonical := make(map[string]any, len(canonicalKeys))
	for _, key := range canonicalKeys {
		field, exists := transport[key]
		if !exists {
			return nil, fmt.Errorf("canonical result is missing %s", key)
		}
		canonical[key] = field
	}
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(canonical); err != nil {
		return nil, err
	}
	return canonicalUTF8Line(output.Bytes()), nil
}

func canonicalUTF8Line(encoded []byte) []byte {
	result := make([]byte, 0, len(encoded))
	for index := 0; index < len(encoded); {
		if encoded[index] != '\\' {
			result = append(result, encoded[index])
			index++
			continue
		}
		start := index
		for index < len(encoded) && encoded[index] == '\\' {
			index++
		}
		count := index - start
		if count%2 == 1 && index+5 <= len(encoded) && encoded[index] == 'u' {
			code := string(encoded[index : index+5])
			if code == "u2028" || code == "u2029" {
				result = append(result, encoded[start:index-1]...)
				if code == "u2028" {
					result = append(result, 0xe2, 0x80, 0xa8)
				} else {
					result = append(result, 0xe2, 0x80, 0xa9)
				}
				index += 5
				continue
			}
		}
		result = append(result, encoded[start:index]...)
	}
	return result
}

func canonicalDigest(value []byte) (string, error) {
	line, err := canonicalBaseLine(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(line)
	return hex.EncodeToString(digest[:]), nil
}
