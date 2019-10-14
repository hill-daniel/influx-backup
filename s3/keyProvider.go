package s3

import (
	"encoding/hex"
	"fmt"
)

// BucketKeyProvider is an abstraction for creating keys for an s3 bucket.
type BucketKeyProvider interface {
	CreateKeyFor(symbol string) string
}

// HexKeyProvider adds a hex prefix for a given string.
type HexKeyProvider struct{}

// CreateKeyFor creates a hex prefix for the given symbol to optimize storage on S3.
// No more than eight chars will be used as the prefix.
// Example: input: thisIsTheValue output: 74686973_thisIsTheValue
func (HexKeyProvider) CreateKeyFor(symbol string) string {
	bytes := []byte(symbol)
	hexEncodedSymbol := hex.EncodeToString(bytes)
	runes := []rune(hexEncodedSymbol)
	var key string
	if len(runes) > 8 {
		key = string(runes[0:8])
	} else {
		key = hexEncodedSymbol
	}
	return fmt.Sprintf("%s_%s", key, symbol)
}
