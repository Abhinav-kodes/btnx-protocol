package chunker

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"
)

func TestChunkFile_SubChunkSize(t *testing.T) {
	// Create 100 byte test file
	testFile := "test-small.bin"
	testData := make([]byte, 100)
	for i := range testData {
		testData[i] = byte(i)
	}
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(testFile)

	chunks, err := ChunkFile(testFile)
	if err != nil {
		t.Fatalf("ChunkFile failed: %v", err)
	}

	if len(chunks) != 1 {
		t.Errorf("Expected 1 chunk, got %d", len(chunks))
	}

	if chunks[0].Size != 100 {
		t.Errorf("Expected size 100, got %d", chunks[0].Size)
	}

	if !bytes.Equal(chunks[0].Data, testData) {
		t.Error("Chunk data doesn't match original")
	}
}

func TestChunkFile_ExactChunkSize(t *testing.T) {
	testFile := "test-1mb.bin"
	testData := make([]byte, ChunkSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(testFile)

	chunks, err := ChunkFile(testFile)
	if err != nil {
		t.Fatalf("ChunkFile failed: %v", err)
	}

	if len(chunks) != 1 {
		t.Errorf("Expected 1 chunk, got %d", len(chunks))
	}

	if chunks[0].Size != ChunkSize {
		t.Errorf("Expected size %d, got %d", ChunkSize, chunks[0].Size)
	}
}

func TestChunkFile_MultipleChunks(t *testing.T) {
	testFile := "test-5mb.bin"
	testData := make([]byte, 5*ChunkSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(testFile)

	chunks, err := ChunkFile(testFile)
	if err != nil {
		t.Fatalf("ChunkFile failed: %v", err)
	}

	if len(chunks) != 5 {
		t.Errorf("Expected 5 chunks, got %d", len(chunks))
	}

	// Verify each chunk
	for i, chunk := range chunks {
		if chunk.Index != i {
			t.Errorf("Chunk %d has wrong index: %d", i, chunk.Index)
		}
		if chunk.Size != ChunkSize {
			t.Errorf("Chunk %d has wrong size: %d", i, chunk.Size)
		}
		if chunk.Hash == "" {
			t.Errorf("Chunk %d has empty hash", i)
		}
	}
}

func TestChunkFile_PartialLastChunk(t *testing.T) {
	testFile := "test-partial.bin"
	// 3.5MB file
	testData := make([]byte, 3*ChunkSize+ChunkSize/2)
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(testFile)

	chunks, err := ChunkFile(testFile)
	if err != nil {
		t.Fatalf("ChunkFile failed: %v", err)
	}

	if len(chunks) != 4 {
		t.Errorf("Expected 4 chunks, got %d", len(chunks))
	}

	// Last chunk should be half size
	lastChunk := chunks[len(chunks)-1]
	if lastChunk.Size != ChunkSize/2 {
		t.Errorf("Expected last chunk size %d, got %d", ChunkSize/2, lastChunk.Size)
	}
}

func TestAssembleChunks(t *testing.T) {
	// Create original file
	original := "test-original.bin"
	testData := make([]byte, 3*ChunkSize+500) // 3.5MB
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	if err := os.WriteFile(original, testData, 0644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(original)

	// Chunk it
	chunks, err := ChunkFile(original)
	if err != nil {
		t.Fatal(err)
	}

	// Reassemble
	assembled := "test-assembled.bin"
	defer os.Remove(assembled)

	err = AssembleChunks(chunks, assembled)
	if err != nil {
		t.Fatalf("AssembleChunks failed: %v", err)
	}

	// Verify
	assembledData, err := os.ReadFile(assembled)
	if err != nil {
		t.Fatal(err)
	}

	if len(assembledData) != len(testData) {
		t.Errorf("Size mismatch: expected %d, got %d", len(testData), len(assembledData))
	}

	if !bytes.Equal(assembledData, testData) {
		t.Error("Assembled data doesn't match original")
	}
}

func TestVerifyChunk(t *testing.T) {
	data := []byte("test data for verification")
	
	// Calculate actual hash
	hash := sha256.Sum256(data)
	correctHash := hex.EncodeToString(hash[:])

	if !VerifyChunk(data, correctHash) {
		t.Error("Verification failed for correct hash")
	}

	if VerifyChunk(data, "wronghash123456") {
		t.Error("Verification passed for wrong hash")
	}
}

func TestChunkFile_EmptyFile(t *testing.T) {
	testFile := "test-empty.bin"
	if err := os.WriteFile(testFile, []byte{}, 0644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(testFile)

	chunks, err := ChunkFile(testFile)
	if err != nil {
		t.Fatalf("ChunkFile failed: %v", err)
	}

	if len(chunks) != 0 {
		t.Errorf("Expected 0 chunks for empty file, got %d", len(chunks))
	}
}

func TestChunkFile_NonExistent(t *testing.T) {
	_, err := ChunkFile("nonexistent-file.bin")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}