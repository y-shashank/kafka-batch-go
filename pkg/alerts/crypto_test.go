package alerts

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"testing"
)

func encryptBlob(t *testing.T, salt, plain string) string {
	t.Helper()
	key := sha256.Sum256([]byte(salt))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	iv := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(iv); err != nil {
		t.Fatal(err)
	}
	sealed := gcm.Seal(nil, iv, []byte(plain), nil)
	ct, tag := sealed[:len(sealed)-gcm.Overhead()], sealed[len(sealed)-gcm.Overhead():]
	payload, _ := json.Marshal(map[string]interface{}{
		"v": 1,
		"iv":  base64.StdEncoding.EncodeToString(iv),
		"tag": base64.StdEncoding.EncodeToString(tag),
		"ct":  base64.StdEncoding.EncodeToString(ct),
	})
	return base64.StdEncoding.EncodeToString(payload)
}

func TestDecrypt(t *testing.T) {
	if s, err := Decrypt("", "x"); s != "" || err != nil {
		t.Fatalf("empty salt: %q %v", s, err)
	}
	if s, err := Decrypt("salt", ""); s != "" || err != nil {
		t.Fatalf("empty blob: %q %v", s, err)
	}
	if _, err := Decrypt("salt", "!!!not-b64!!!"); err == nil {
		t.Fatal("expected b64 error")
	}
	if _, err := Decrypt("salt", base64.StdEncoding.EncodeToString([]byte("not-json"))); err == nil {
		t.Fatal("expected json error")
	}

	blob := encryptBlob(t, "test-salt", "https://hooks.slack.com/x")
	got, err := Decrypt("test-salt", blob)
	if err != nil || got != "https://hooks.slack.com/x" {
		t.Fatalf("got %q err=%v", got, err)
	}
	if _, err := Decrypt("wrong-salt", blob); err == nil {
		t.Fatal("expected auth failure")
	}
}
