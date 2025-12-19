package chunker

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"
)

// ============================================================================
// STREAMING CHUNK TESTS
// ============================================================================

func TestStreamChunkFile_SubChunkSize(t *testing.T) {
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

	// Stream chunks
	chunkStream := StreamChunkFile(testFile)
	var chunks []Chunk

	for result := range chunkStream {
		if result.Err != nil {
			t.Fatalf("StreamChunkFile failed: %v", result.Err)
		}
		chunks = append(chunks, result.Chunk)
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

func TestStreamChunkFile_ExactChunkSize(t *testing.T) {
	testFile := "test-1mb.bin"
	testData := make([]byte, ChunkSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(testFile)

	chunkStream := StreamChunkFile(testFile)
	var chunks []Chunk

	for result := range chunkStream {
		if result.Err != nil {
			t.Fatalf("StreamChunkFile failed: %v", result.Err)
		}
		chunks = append(chunks, result.Chunk)
	}

	if len(chunks) != 1 {
		t.Errorf("Expected 1 chunk, got %d", len(chunks))
	}

	if chunks[0].Size != ChunkSize {
		t.Errorf("Expected size %d, got %d", ChunkSize, chunks[0].Size)
	}
}

func TestStreamChunkFile_MultipleChunks(t *testing.T) {
	testFile := "test-5mb.bin"
	testData := make([]byte, 5*ChunkSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(testFile)

	chunkStream := StreamChunkFile(testFile)
	var chunks []Chunk

	for result := range chunkStream {
		if result.Err != nil {
			t.Fatalf("StreamChunkFile failed: %v", result.Err)
		}
		chunks = append(chunks, result.Chunk)
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

func TestStreamChunkFile_PartialLastChunk(t *testing.T) {
	testFile := "test-partial.bin"
	// 3.5MB file
	testData := make([]byte, 3*ChunkSize+ChunkSize/2)
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(testFile)

	chunkStream := StreamChunkFile(testFile)
	var chunks []Chunk

	for result := range chunkStream {
		if result.Err != nil {
			t.Fatalf("StreamChunkFile failed: %v", result.Err)
		}
		chunks = append(chunks, result.Chunk)
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

func TestStreamChunkFile_EmptyFile(t *testing.T) {
	testFile := "test-empty.bin"
	if err := os.WriteFile(testFile, []byte{}, 0644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(testFile)

	chunkStream := StreamChunkFile(testFile)
	var chunks []Chunk

	for result := range chunkStream {
		if result.Err != nil {
			t.Fatalf("StreamChunkFile failed: %v", result.Err)
		}
		chunks = append(chunks, result.Chunk)
	}

	if len(chunks) != 0 {
		t.Errorf("Expected 0 chunks for empty file, got %d", len(chunks))
	}
}

func TestStreamChunkFile_NonExistent(t *testing.T) {
	chunkStream := StreamChunkFile("nonexistent-file.bin")

	for result := range chunkStream {
		if result.Err == nil {
			t.Error("Expected error for non-existent file")
			return // Should get error immediately
		}
	}
}

// ============================================================================
// ERASURE CODING TESTS
// ============================================================================

func TestShardChunk_Basic(t *testing.T) {
	// Create test chunk with random data
	testData := make([]byte, ChunkSize)
	rand.Read(testData)

	hash := sha256.Sum256(testData)
	chunk := Chunk{
		Index: 0,
		Data:  testData,
		Hash:  hex.EncodeToString(hash[:]),
		Size:  len(testData),
	}

	// Shard the chunk
	shards, err := ShardChunk(chunk, testData)
	if err != nil {
		t.Fatalf("ShardChunk failed: %v", err)
	}

	// Verify we got 6 shards
	if len(shards) != TotalShards {
		t.Errorf("Expected %d shards, got %d", TotalShards, len(shards))
	}

	// Verify shard metadata
	for i, shard := range shards {
		if shard.ChunkIndex != 0 {
			t.Errorf("Shard %d has wrong chunk index: %d", i, shard.ChunkIndex)
		}
		if shard.ShardIndex != i {
			t.Errorf("Shard %d has wrong shard index: %d", i, shard.ShardIndex)
		}
		if shard.Size <= 0 {
			t.Errorf("Shard %d has invalid size: %d", i, shard.Size)
		}
		if shard.Hash == "" {
			t.Errorf("Shard %d has empty hash", i)
		}
		// Verify hash matches data
		if !VerifyShard(shard.Data, shard.Hash) {
			t.Errorf("Shard %d hash verification failed", i)
		}
	}
}

func TestShardChunk_SizeMismatch(t *testing.T) {
	testData := make([]byte, 100)
	chunk := Chunk{
		Index: 0,
		Data:  testData,
		Hash:  "",
		Size:  50, // Wrong size!
	}

	_, err := ShardChunk(chunk, testData)
	if err == nil {
		t.Error("Expected error for size mismatch")
	}
}

func TestReconstructChunk_AllShards(t *testing.T) {
	// Create test data
	testData := make([]byte, ChunkSize)
	rand.Read(testData)

	hash := sha256.Sum256(testData)
	chunk := Chunk{
		Index: 5,
		Data:  testData,
		Hash:  hex.EncodeToString(hash[:]),
		Size:  len(testData),
	}

	// Shard the chunk
	shards, err := ShardChunk(chunk, testData)
	if err != nil {
		t.Fatal(err)
	}

	// Reconstruct from all 6 shards
	reconstructed, err := ReconstructChunk(shards, len(testData))
	if err != nil {
		t.Fatalf("ReconstructChunk failed: %v", err)
	}

	// Verify reconstruction matches original
	if !bytes.Equal(reconstructed, testData) {
		t.Error("Reconstructed data doesn't match original")
	}
}

func TestReconstructChunk_MinimumShards(t *testing.T) {
	// Create test data
	testData := make([]byte, ChunkSize)
	rand.Read(testData)

	hash := sha256.Sum256(testData)
	chunk := Chunk{
		Index: 3,
		Data:  testData,
		Hash:  hex.EncodeToString(hash[:]),
		Size:  len(testData),
	}

	// Shard the chunk
	allShards, err := ShardChunk(chunk, testData)
	if err != nil {
		t.Fatal(err)
	}

	// Test all combinations of 4 shards
	testCases := []struct {
		name    string
		indices []int
	}{
		{"First 4 (all data)", []int{0, 1, 2, 3}},
		{"Last 4 (2 data + 2 parity)", []int{2, 3, 4, 5}},
		{"Mixed", []int{0, 2, 4, 5}},
		{"Scattered", []int{1, 2, 4, 5}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Select only specific shards
			selectedShards := make([]Shard, len(tc.indices))
			for i, idx := range tc.indices {
				selectedShards[i] = allShards[idx]
			}

			// Reconstruct
			reconstructed, err := ReconstructChunk(selectedShards, len(testData))
			if err != nil {
				t.Fatalf("ReconstructChunk failed: %v", err)
			}

			// Verify
			if !bytes.Equal(reconstructed, testData) {
				t.Error("Reconstructed data doesn't match original")
			}
		})
	}
}

func TestReconstructChunk_InsufficientShards(t *testing.T) {
	// Create test data
	testData := make([]byte, ChunkSize)
	rand.Read(testData)

	hash := sha256.Sum256(testData)
	chunk := Chunk{
		Index: 0,
		Data:  testData,
		Hash:  hex.EncodeToString(hash[:]),
		Size:  len(testData),
	}

	// Shard the chunk
	allShards, err := ShardChunk(chunk, testData)
	if err != nil {
		t.Fatal(err)
	}

	// Try with only 3 shards (need 4 minimum)
	insufficientShards := allShards[:3]

	_, err = ReconstructChunk(insufficientShards, len(testData))
	if err == nil {
		t.Error("Expected error for insufficient shards")
	}
}

func TestReconstructChunk_CorruptedShard(t *testing.T) {
	// Create test data
	testData := make([]byte, ChunkSize)
	rand.Read(testData)

	hash := sha256.Sum256(testData)
	chunk := Chunk{
		Index: 0,
		Data:  testData,
		Hash:  hex.EncodeToString(hash[:]),
		Size:  len(testData),
	}

	// Shard the chunk
	shards, err := ShardChunk(chunk, testData)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt one shard's data (but keep wrong hash)
	shards[0].Data[0] ^= 0xFF

	// Should fail hash verification
	_, err = ReconstructChunk(shards[:4], len(testData))
	if err == nil {
		t.Error("Expected error for corrupted shard")
	}
}

func TestReconstructChunk_MixedChunks(t *testing.T) {
	// Create two different chunks
	testData1 := make([]byte, ChunkSize)
	testData2 := make([]byte, ChunkSize)
	rand.Read(testData1)
	rand.Read(testData2)

	hash1 := sha256.Sum256(testData1)
	chunk1 := Chunk{Index: 0, Data: testData1, Hash: hex.EncodeToString(hash1[:]), Size: len(testData1)}

	hash2 := sha256.Sum256(testData2)
	chunk2 := Chunk{Index: 1, Data: testData2, Hash: hex.EncodeToString(hash2[:]), Size: len(testData2)}

	// Shard both
	shards1, _ := ShardChunk(chunk1, testData1)
	shards2, _ := ShardChunk(chunk2, testData2)

	// Mix shards from different chunks
	mixedShards := []Shard{
		shards1[0],
		shards1[1],
		shards2[2], // Wrong chunk!
		shards1[3],
	}

	// Should fail
	_, err := ReconstructChunk(mixedShards, len(testData1))
	if err == nil {
		t.Error("Expected error for mixed chunk shards")
	}
}

// ============================================================================
// ASSEMBLE CHUNKS TESTS (with channels)
// ============================================================================

func TestAssembleChunks_InOrder(t *testing.T) {
	// Create original file
	original := "test-original.bin"
	testData := make([]byte, 3*ChunkSize+500)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	if err := os.WriteFile(original, testData, 0644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(original)

	// Stream chunks
	chunkStream := StreamChunkFile(original)
	var chunks []Chunk

	for result := range chunkStream {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
		chunks = append(chunks, result.Chunk)
	}

	// Create channel and send chunks in order
	outStream := make(chan Chunk, len(chunks))
	for _, chunk := range chunks {
		outStream <- chunk
	}
	close(outStream)

	// Reassemble
	assembled := "test-assembled.bin"
	defer os.Remove(assembled)

	err := AssembleChunks(outStream, assembled, len(chunks))
	if err != nil {
		t.Fatalf("AssembleChunks failed: %v", err)
	}

	// Verify
	assembledData, err := os.ReadFile(assembled)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(assembledData, testData) {
		t.Error("Assembled data doesn't match original")
	}
}

func TestAssembleChunks_OutOfOrder(t *testing.T) {
	// Create test data
	testData := make([]byte, 5*ChunkSize)
	rand.Read(testData)

	original := "test-random.bin"
	if err := os.WriteFile(original, testData, 0644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(original)

	// Stream chunks
	chunkStream := StreamChunkFile(original)
	var chunks []Chunk

	for result := range chunkStream {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
		chunks = append(chunks, result.Chunk)
	}

	// Send chunks in reverse order
	outStream := make(chan Chunk, len(chunks))
	for i := len(chunks) - 1; i >= 0; i-- {
		outStream <- chunks[i]
	}
	close(outStream)

	// Reassemble
	assembled := "test-assembled-random.bin"
	defer os.Remove(assembled)

	err := AssembleChunks(outStream, assembled, len(chunks))
	if err != nil {
		t.Fatalf("AssembleChunks failed: %v", err)
	}

	// Verify
	assembledData, err := os.ReadFile(assembled)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(assembledData, testData) {
		t.Error("Assembled data doesn't match original (out of order)")
	}
}

func TestAssembleChunks_MissingChunk(t *testing.T) {
	// Create 3 chunks
	chunks := make([]Chunk, 3)
	for i := 0; i < 3; i++ {
		data := make([]byte, ChunkSize)
		rand.Read(data)
		chunks[i] = Chunk{Index: i, Data: data, Size: ChunkSize}
	}

	// Send only 2 chunks (missing chunk 1)
	outStream := make(chan Chunk, 2)
	outStream <- chunks[0]
	outStream <- chunks[2] // Skip chunk 1
	close(outStream)

	// Should fail
	assembled := "test-missing.bin"
	defer os.Remove(assembled)

	err := AssembleChunks(outStream, assembled, 3)
	if err == nil {
		t.Error("Expected error for missing chunk")
	}
}

// ============================================================================
// VERIFY FUNCTIONS TESTS
// ============================================================================

func TestVerifyChunk(t *testing.T) {
	data := []byte("test data for verification")

	hash := sha256.Sum256(data)
	correctHash := hex.EncodeToString(hash[:])

	if !VerifyChunk(data, correctHash) {
		t.Error("Verification failed for correct hash")
	}

	if VerifyChunk(data, "wronghash123456") {
		t.Error("Verification passed for wrong hash")
	}
}

func TestVerifyShard(t *testing.T) {
	data := []byte("shard data for verification")

	hash := sha256.Sum256(data)
	correctHash := hex.EncodeToString(hash[:])

	if !VerifyShard(data, correctHash) {
		t.Error("Shard verification failed for correct hash")
	}

	if VerifyShard(data, "wronghash789abc") {
		t.Error("Shard verification passed for wrong hash")
	}
}

// ============================================================================
// FULL ROUND-TRIP TEST (Most Important!)
// ============================================================================

func TestFullRoundTrip_ChunkShardReconstruct(t *testing.T) {
	// 1. Create original file (3.5 MB)
	original := "test-roundtrip.bin"
	testData := make([]byte, 3*ChunkSize+ChunkSize/2)
	rand.Read(testData)
	if err := os.WriteFile(original, testData, 0644); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(original)

	// 2. Stream and chunk file
	chunkStream := StreamChunkFile(original)
	var chunks []Chunk

	for result := range chunkStream {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
		chunks = append(chunks, result.Chunk)
	}

	t.Logf("Created %d chunks", len(chunks))

	// 3. Shard each chunk and collect all shards
	var allShards [][]Shard
	for _, chunk := range chunks {
		// In real system, chunk.Data would be encrypted here
		shards, err := ShardChunk(chunk, chunk.Data)
		if err != nil {
			t.Fatalf("Failed to shard chunk %d: %v", chunk.Index, err)
		}
		allShards = append(allShards, shards)
		t.Logf("Chunk %d sharded into %d shards", chunk.Index, len(shards))
	}

	// 4. Simulate download: reconstruct each chunk from random 4 shards
	reconstructedChunks := make(chan Chunk, len(chunks))

	for chunkIdx, shards := range allShards {
		// Pick first 4 shards (in real system, would download from different farmers)
		selectedShards := shards[:4]

		reconstructed, err := ReconstructChunk(selectedShards, chunks[chunkIdx].Size)
		if err != nil {
			t.Fatalf("Failed to reconstruct chunk %d: %v", chunkIdx, err)
		}

		// Verify reconstructed data matches original chunk
		if !bytes.Equal(reconstructed, chunks[chunkIdx].Data) {
			t.Errorf("Chunk %d reconstruction mismatch", chunkIdx)
		}

		// Send to assembly channel
		reconstructedChunks <- Chunk{
			Index: chunkIdx,
			Data:  reconstructed,
			Hash:  chunks[chunkIdx].Hash,
			Size:  len(reconstructed),
		}
	}
	close(reconstructedChunks)

	// 5. Reassemble file
	assembled := "test-roundtrip-assembled.bin"
	defer os.Remove(assembled)

	err := AssembleChunks(reconstructedChunks, assembled, len(chunks))
	if err != nil {
		t.Fatalf("Failed to assemble: %v", err)
	}

	// 6. Verify final file matches original
	assembledData, err := os.ReadFile(assembled)
	if err != nil {
		t.Fatal(err)
	}

	if len(assembledData) != len(testData) {
		t.Errorf("Size mismatch: expected %d, got %d", len(testData), len(assembledData))
	}

	if !bytes.Equal(assembledData, testData) {
		t.Error("Final assembled file doesn't match original!")
	}

	t.Log("✅ Full round-trip successful: chunk → shard → reconstruct → assemble")
}
