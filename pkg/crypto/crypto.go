package crypto

import (
	"crypto/rand"
	"fmt"

	"golang.org/x/crypto/chacha20poly1305"
)

const KeySize = 32 // 32 bytes / 256 bits for encryption key

// GenerateKey creates a new random 256-bit encryption key and returns it
func GenerateKey() ([]byte, error) {
	// Allocate byte slice for key
	key := make([]byte, KeySize)
	// Fill with cryptographically secure random bytes
	_, err := rand.Read(key)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}
	
	return key, nil
}

// EncryptChunk encrypts a chunk with XChaCha20-Poly1305 AEAD
// Returns: [nonce|ciphertext|authentication_tag]
func EncryptChunk(plaintext []byte, key []byte) ([]byte, error) {
	// Validate key size
	if len(key) != KeySize {
		return nil, fmt.Errorf("invalid key size: expected %d, got %d", KeySize, len(key))
	}

	// Create AEAD (Authenticated Encryption with Associated Data) cipher
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Generate random nonce
	nonce := make([]byte, aead.NonceSize())    // 24 bytes (192 bits) for XChaCha20
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonc e: %w", err)
	}

	// Encrypt: output = nonce + ciphertext + tag
	// Seal prepends nonce and appends tag automatically
	ciphertext := aead.Seal(nonce, nonce, plaintext, nil) // seal(dst, nonce, plaintext, additionalData) (output = nonce || ciphertext || tag) where nonce is used for encryption/decryption

	return ciphertext, nil
}


// DecryptChunk decrypts a chunk encrypted with EncryptChunk
func DecryptChunk(ciphertext []byte, key []byte) ([]byte, error) {
	// Validate key size
	if len(key) != KeySize {
		return nil, fmt.Errorf("invalid key size: expected %d, got %d", KeySize, len(key))
	}

	// Create AEAD (Authenticated Encryption with Associated Data) cipher
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Validate ciphertext length
	if len(ciphertext) < aead.NonceSize() {
		return nil, fmt.Errorf("ciphertext too short: expected at least %d bytes, got %d", aead.NonceSize(), len(ciphertext))
	}

	// Split nonce and actual ciphertext
	nonce := ciphertext[:aead.NonceSize()]
	ciphertext = ciphertext[aead.NonceSize():]

	// Decrypt and verify authentication tag
	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decryption failed (wrong key or tampered data): %w", err)
	}

	return plaintext, nil
}