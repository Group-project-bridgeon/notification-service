package email

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
)

// ParseEncryptionKey extracts or derives a 32-byte AES key from the configured key string.
func ParseEncryptionKey(rawKey string) ([]byte, error) {
	if rawKey == "" {
		return nil, errors.New("empty email encryption key")
	}

	// 1. Try decoding standard base64
	if decoded, err := base64.StdEncoding.DecodeString(rawKey); err == nil && len(decoded) == 32 {
		return decoded, nil
	}

	// 2. Try raw byte length of 32
	if len(rawKey) == 32 {
		return []byte(rawKey), nil
	}

	// 3. Fallback: derive 32-byte key using SHA-256
	h := sha256.Sum256([]byte(rawKey))
	return h[:], nil
}

// EncryptPassword encrypts a plaintext password using AES-256-GCM.
// Returns a slice structured as: [12-byte nonce][ciphertext + 16-byte tag].
func EncryptPassword(plaintext string, key []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("invalid AES-256 key length: expected 32 bytes, got %d", len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed creating AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed creating GCM block: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed generating random GCM nonce: %w", err)
	}

	// Prepend nonce to ciphertext
	sealed := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return sealed, nil
}

// DecryptPassword decrypts an AES-256-GCM encrypted password byte slice.
func DecryptPassword(ciphertext []byte, key []byte) (string, error) {
	if len(ciphertext) == 0 {
		return "", nil
	}
	if len(key) != 32 {
		return "", fmt.Errorf("invalid AES-256 key length: expected 32 bytes, got %d", len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("failed creating AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed creating GCM block: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return "", errors.New("encrypted payload too short to contain nonce")
	}

	nonce := ciphertext[:nonceSize]
	rawCiphertext := ciphertext[nonceSize:]

	plaintext, err := gcm.Open(nil, nonce, rawCiphertext, nil)
	if err != nil {
		return "", fmt.Errorf("failed decrypting password: %w", err)
	}

	return string(plaintext), nil
}
