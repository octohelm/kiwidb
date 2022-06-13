package msgp

import (
	"io"
)

// NewEncoder returns a new encoder that writes to w.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w}
}

type Encoder struct {
	w   io.Writer
	err error
}

func (enc *Encoder) Encode(v any) error {
	if enc.err != nil {
		return enc.err
	}
	e := newEncodeState()
	err := e.marshal(v)
	if err != nil {
		return err
	}
	if _, err = enc.w.Write(e.Bytes()); err != nil {
		enc.err = err
	}
	encodeStatePool.Put(e)
	return err
}

// NewDecoder returns a new decoder that reads from r.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

type Decoder struct {
	r   io.Reader
	d   decodeState
	err error
}

func (dec *Decoder) Decode(v any) error {
	if dec.err != nil {
		return dec.err
	}
	dec.d.init(dec.r)
	return dec.d.unmarshal(v)
}
