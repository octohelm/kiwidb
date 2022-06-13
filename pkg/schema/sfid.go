package schema

import (
	"fmt"
	"strconv"
)

// openapi:strfmt id
type SFID uint64

func (s *SFID) UnmarshalText(text []byte) error {
	if len(text) != 18 {
		return fmt.Errorf("invalid sfid: %q", text)
	}
	id, err := strconv.ParseUint(string(text), 10, 64)
	if err != nil {
		return err
	}
	*s = SFID(id)
	return nil
}

func (s SFID) MarshalText() (text []byte, err error) {
	return []byte(strconv.FormatUint(uint64(s), 10)), nil
}
