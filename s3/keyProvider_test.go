package s3_test

import (
	"github.com/hill-daniel/influx-backup/s3"
	"testing"
)

func Test_should_create_hex_from_string_and_append_original_value(t *testing.T) {
	symbol := "key"
	hexKeyProvider := s3.HexKeyProvider{}

	actualKey := hexKeyProvider.CreateKeyFor(symbol)

	expectedKey := "6b6579_key"
	if actualKey != expectedKey {
		t.Fatalf("actual: %s expected: %s", actualKey, expectedKey)
	}
}

func Test_should_truncate_hex_if_longer_than_8char_and_append_original_value(t *testing.T) {
	symbol := "thisIsTheValue"
	hexKeyProvider := s3.HexKeyProvider{}

	actualKey := hexKeyProvider.CreateKeyFor(symbol)

	expectedKey := "74686973_thisIsTheValue"
	if actualKey != expectedKey {
		t.Fatalf("actual: %s expected: %s", actualKey, expectedKey)
	}
}
