package msgp

import (
	"bytes"
	"strings"
	"testing"

	textingx "github.com/octohelm/x/testing"
)

func TestStream(t *testing.T) {
	inputs := []any{int32(1), strings.Repeat("v", 10000), false}

	buf := bytes.NewBuffer(nil)

	encoder := NewEncoder(buf)
	for i := range inputs {
		if err := encoder.Encode(inputs[i]); err != nil {
			textingx.Expect(t, err, textingx.Be[error](nil))
		}
	}

	outputs := make([]any, len(inputs))
	decoder := NewDecoder(buf)

	for i := range outputs {
		if err := decoder.Decode(&outputs[i]); err != nil {
			textingx.Expect(t, err, textingx.Be[error](nil))
		}
	}

	textingx.Expect(t, outputs, textingx.Equal(inputs))
}
