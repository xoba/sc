package sc

import "github.com/shengdoushi/base58"

// avoids problematic characters for both people and filesystems
func Base58Encode(buf []byte) string {
	return base58.Encode(buf, base58.BitcoinAlphabet)
}

func Base58Decode(enc string) ([]byte, error) {
	return base58.Decode(enc, base58.BitcoinAlphabet)
}
