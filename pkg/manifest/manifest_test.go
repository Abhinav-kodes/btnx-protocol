package manifest

import (
	"os"
	"testing"
)

func TestNew(t *testing.T) {
	chunks := []ChunkMeta{
		{Index: 0, Hash: "hash0", Size: 1024},
		{Index: 1, Hash: "hash1", Size: 1024},
	}

	key := []byte("test-encryption-key-32-bytes!!")

	m := New("test.bin", 2048, "filehash", chunks, key, "0xPublisher")

	if m.Version != "1.0" {
		t.Errorf("Wrong version: %s", m.Version)
	}

	if m.BlobID == "" {
		t.Error("BlobID is empty")
	}

	if m.FileName != "test.bin" {
		t.Errorf("Wrong filename: %s", m.FileName)
	}

	if m.ChunkCount != 2 {
		t.Errorf("Expected 2 chunks, got %d", m.ChunkCount)
	}

	if m.PublisherAddress != "0xPublisher" {
		t.Error("Publisher address mismatch")
	}
}

func TestBlobID_Uniqueness(t *testing.T) {
	key := []byte("test-encryption-key-32-bytes!!")
	chunks := []ChunkMeta{{Index: 0, Hash: "hash0", Size: 1024}}

	m1 := New("test.bin", 1024, "hash", chunks, key, "0xPub")
	m2 := New("test.bin", 1024, "hash", chunks, key, "0xPub")

	if m1.BlobID == m2.BlobID {
		t.Error("BlobIDs should be unique")
	}
}

func TestSaveLoad(t *testing.T) {
	chunks := []ChunkMeta{
		{Index: 0, Hash: "hash0", Size: 1024},
		{Index: 1, Hash: "hash1", Size: 2048},
	}

	key := []byte("test-key-32-bytes-long-padding!!")
	m := New("test.bin", 3072, "filehash", chunks, key, "0xPublisher")

	testFile := "test-manifest.json"
	defer os.Remove(testFile)

	// Save
	err := m.Save(testFile)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Load
	loaded, err := Load(testFile)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Verify
	if loaded.BlobID != m.BlobID {
		t.Error("BlobID mismatch")
	}

	if loaded.FileName != m.FileName {
		t.Error("FileName mismatch")
	}

	if loaded.ChunkCount != m.ChunkCount {
		t.Error("ChunkCount mismatch")
	}

	if loaded.EncryptionKey != m.EncryptionKey {
		t.Error("EncryptionKey mismatch")
	}

	if len(loaded.Chunks) != len(m.Chunks) {
		t.Error("Chunks length mismatch")
	}
}

func TestGetChunkHash(t *testing.T) {
	chunks := []ChunkMeta{
		{Index: 0, Hash: "hash0", Size: 1024},
		{Index: 1, Hash: "hash1", Size: 1024},
		{Index: 2, Hash: "hash2", Size: 1024},
	}

	key := []byte("test-key-32-bytes-long-padding!!")
	m := New("test.bin", 3072, "filehash", chunks, key, "0xPublisher")

	if m.GetChunkHash(1) != "hash1" {
		t.Error("GetChunkHash(1) failed")
	}

	if m.GetChunkHash(2) != "hash2" {
		t.Error("GetChunkHash(2) failed")
	}

	if m.GetChunkHash(99) != "" {
		t.Error("Should return empty string for non-existent chunk")
	}
}

func TestGetEncryptionKey(t *testing.T) {
	key := []byte("test-key-32-bytes-long-padding!!")
	chunks := []ChunkMeta{{Index: 0, Hash: "hash0", Size: 1024}}
	
	m := New("test.bin", 1024, "hash", chunks, key, "0xPub")

	retrievedKey, err := m.GetEncryptionKey()
	if err != nil {
		t.Fatalf("GetEncryptionKey failed: %v", err)
	}

	if len(retrievedKey) != 32 {
		t.Errorf("Expected key size 32, got %d", len(retrievedKey))
	}

	// Compare with original
	for i := range key {
		if key[i] != retrievedKey[i] {
			t.Error("Retrieved key doesn't match original")
			break
		}
	}
}

func TestCalculateFileHash(t *testing.T) {
	testFile := "test-hash.bin"
	testData := []byte("test data for hashing")
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(testFile)

	hash, err := CalculateFileHash(testFile)
	if err != nil {
		t.Fatalf("CalculateFileHash failed: %v", err)
	}

	if hash == "" {
		t.Error("Hash is empty")
	}

	// Calculate hash again, should be the same
	hash2, _ := CalculateFileHash(testFile)
	if hash != hash2 {
		t.Error("Hash should be deterministic")
	}
}

func TestLoad_NonExistent(t *testing.T) {
	_, err := Load("nonexistent-manifest.json")
	if err == nil {
		t.Error("Expected error for non-existent manifest")
	}
}
