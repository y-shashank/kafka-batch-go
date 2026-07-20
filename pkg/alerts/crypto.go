package alerts

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// Decrypt matches Ruby KafkaBatch::Ai::Crypto (AES-256-GCM, key=SHA256(salt)).
// Blob is Base64(JSON{v,iv,tag,ct}) with binary fields Base64-encoded.
func Decrypt(salt, blob string) (string, error) {
	if salt == "" || blob == "" {
		return "", nil
	}
	raw, err := base64.StdEncoding.DecodeString(blob)
	if err != nil {
		return "", err
	}
	var payload struct {
		V   int    `json:"v"`
		IV  string `json:"iv"`
		Tag string `json:"tag"`
		CT  string `json:"ct"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return "", err
	}
	iv, err := base64.StdEncoding.DecodeString(payload.IV)
	if err != nil {
		return "", err
	}
	tag, err := base64.StdEncoding.DecodeString(payload.Tag)
	if err != nil {
		return "", err
	}
	ct, err := base64.StdEncoding.DecodeString(payload.CT)
	if err != nil {
		return "", err
	}
	key := sha256.Sum256([]byte(salt))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	if len(iv) != gcm.NonceSize() {
		return "", fmt.Errorf("alerts crypto: bad nonce size %d", len(iv))
	}
	// Go GCM expects ciphertext||tag
	plain, err := gcm.Open(nil, iv, append(ct, tag...), nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}
