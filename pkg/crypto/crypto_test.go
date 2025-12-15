package crypto

import (
	"bytes"
	"testing"
)

func TestGenerateKey(t *testing.T) {
	key, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}

	if len(key) != KeySize {
		t.Errorf("Expected key size %d, got %d", KeySize, len(key))
	}

	// Generate another key, should be different
	key2, _ := GenerateKey()
	if bytes.Equal(key, key2) {
		t.Error("Generated keys should be different (randomness check)")
	}
}

func TestEncryptDecrypt_Basic(t *testing.T) {
	key, _ := GenerateKey()
	plaintext := []byte("Hello, BTNX Protocol! This is a secret message.")

	ciphertext, err := EncryptChunk(plaintext, key)
	if err != nil {
		t.Fatalf("Encryption failed: %v", err)
	}

	// Ciphertext should be larger (nonce + tag overhead)
	if len(ciphertext) <= len(plaintext) {
		t.Error("Ciphertext should be larger than plaintext")
	}

	decrypted, err := DecryptChunk(ciphertext, key)
	if err != nil {
		t.Fatalf("Decryption failed: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Error("Decrypted text doesn't match original")
	}
}

func TestEncrypt_DifferentNonces(t *testing.T) {
	key, _ := GenerateKey()
	plaintext := []byte("same plaintext")

	c1, _ := EncryptChunk(plaintext, key)
	c2, _ := EncryptChunk(plaintext, key)

	// Same plaintext encrypted twice should produce different ciphertexts
	// (due to different random nonces)
	if bytes.Equal(c1, c2) {
		t.Error("Same plaintext should produce different ciphertexts (nonce randomness)")
	}
}

func TestDecrypt_WrongKey(t *testing.T) {
	key1, _ := GenerateKey()
	key2, _ := GenerateKey()

	plaintext := []byte("secret message")
	ciphertext, _ := EncryptChunk(plaintext, key1)

	// Try to decrypt with wrong key
	_, err := DecryptChunk(ciphertext, key2)
	if err == nil {
		t.Error("Decryption should fail with wrong key")
	}
}

func TestDecrypt_TamperedCiphertext(t *testing.T) {
	key, _ := GenerateKey()
	plaintext := []byte("authentic message")
	ciphertext, _ := EncryptChunk(plaintext, key)

	// Tamper with ciphertext (flip a bit in the middle)
	ciphertext[len(ciphertext)/2] ^= 0xFF

	// Decryption should fail due to authentication tag mismatch
	_, err := DecryptChunk(ciphertext, key)
	if err == nil {
		t.Error("Decryption should fail for tampered ciphertext")
	}
}

func TestEncrypt_LargeChunk(t *testing.T) {
	key, _ := GenerateKey()
	// Test with 1MB chunk (typical chunk size)
	plaintext := make([]byte, 1024*1024)
	for i := range plaintext {
		plaintext[i] = byte(i % 256)
	}

	ciphertext, err := EncryptChunk(plaintext, key)
	if err != nil {
		t.Fatalf("Encryption of large chunk failed: %v", err)
	}

	decrypted, err := DecryptChunk(ciphertext, key)
	if err != nil {
		t.Fatalf("Decryption of large chunk failed: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Error("Large chunk decryption failed")
	}
}

func TestEncrypt_EmptyPlaintext(t *testing.T) {
	key, _ := GenerateKey()
	plaintext := []byte{}

	ciphertext, err := EncryptChunk(plaintext, key)
	if err != nil {
		t.Fatalf("Encryption of empty plaintext failed: %v", err)
	}

	decrypted, err := DecryptChunk(ciphertext, key)
	if err != nil {
		t.Fatalf("Decryption of empty plaintext failed: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Error("Empty plaintext decryption failed")
	}
}

func TestEncrypt_InvalidKeySize(t *testing.T) {
	shortKey := []byte("too-short")
	plaintext := []byte("test")

	_, err := EncryptChunk(plaintext, shortKey)
	if err == nil {
		t.Error("Should fail with invalid key size")
	}
}

func TestDecrypt_InvalidKeySize(t *testing.T) {
	shortKey := []byte("too-short")
	ciphertext := []byte("test-ciphertext")

	_, err := DecryptChunk(ciphertext, shortKey)
	if err == nil {
		t.Error("Should fail with invalid key size")
	}
}

func TestDecrypt_TooShortCiphertext(t *testing.T) {
	key, _ := GenerateKey()
	// Ciphertext shorter than nonce size
	shortCiphertext := []byte("short")

	_, err := DecryptChunk(shortCiphertext, key)
	if err == nil {
		t.Error("Should fail with ciphertext shorter than nonce size")
	}
}
