package utils

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Date time.Time

func (d *Date) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" {
		*d = Date(time.Time{})
		return nil
	}

	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return fmt.Errorf("error parsing date '%s': %w", s, err)
	}
	*d = Date(t)
	return nil
}

func (d Date) MarshalJSON() ([]byte, error) {
	t := time.Time(d)
	if t.IsZero() {
		return json.Marshal("")
	}
	return json.Marshal(t.Format("2006-01-02"))
}

func (d Date) GoString() string {
	t := time.Time(d)
	if t.IsZero() {
		return "Date{}"
	}
	return fmt.Sprintf("Date{%s}", t.Format("2006-01-02"))
}

func GetStringOrEmpty(s *string) interface{} {
	if s != nil {
		return *s
	}
	return ""
}

func GetTimeOrNilDate(d *Date) interface{} {
	if d != nil && !time.Time(*d).IsZero() {
		return time.Time(*d)
	}
	return nil
}