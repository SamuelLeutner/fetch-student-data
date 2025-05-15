package models

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Date time.Time

const desiredAPILayout = "2006-01-02T15:04:05Z0700"

func (d *Date) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" {

		*d = Date(time.Time{})
		return nil
	}

	t, err := time.Parse(desiredAPILayout, s)
	if err != nil {

		return fmt.Errorf("error parsing date '%s' with layout '%s': %w", s, desiredAPILayout, err)
	}

	*d = Date(t)
	return nil
}

func (d Date) MarshalJSON() ([]byte, error) {

	t := time.Time(d)

	if t.IsZero() {
		return json.Marshal("")
	}

	return json.Marshal(t.Format(desiredAPILayout))
}

func (d Date) GoString() string {

	t := time.Time(d)

	if t.IsZero() {
		return "Date{}"
	}

	return fmt.Sprintf("Date{%s}", t.Format(desiredAPILayout))
}
