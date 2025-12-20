package manifest

import (
	"bytes"
	"os"
	"testing"
)

// ============================================================================
// BASIC MANIFEST CREATION TESTS
// ============================================================================

func TestNew(t *testing.T) {
	chunks := []ChunkMeta{
		{Index: 0, Hash: "hash0", Size: 1048576},
		{Index: 1, Hash: "hash1", Size: 1048576},
	}

	shards := []ShardMeta{
		{ChunkIndex: 0, ShardIndex: 0, Hash: "shard00", Size: 262144, FarmerIndex: 0},
		{ChunkIndex: 0, ShardIndex: 1, Hash: "shard01", Size: 262144, FarmerIndex: 1},
		{ChunkIndex: 1, ShardIndex: 0, Hash: "shard10", Size: 262144, FarmerIndex: 0},
		{ChunkIndex: 1, ShardIndex: 1, Hash: "shard11", Size: 262144, FarmerIndex: 1},
	}

	farmers := []FarmerInfo{
		{Index: 0, Address: "0xFarmer1", Endpoint: "https://f1.btnx.io:4433", Region: "us-east-1"},
		{Index: 1, Address: "0xFarmer2", Endpoint: "https://f2.btnx.io:4433", Region: "us-west-1"},
	}

	key := []byte("test-encryption-key-32-bytes!!")

	m := New("test.bin", 2097152, "filehash", chunks, shards, farmers, key, "0xPublisher")

	// Verify basic fields
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

	// Verify erasure coding fields
	if m.DataShards != 4 {
		t.Errorf("Expected DataShards=4, got %d", m.DataShards)
	}

	if m.ParityShards != 2 {
		t.Errorf("Expected ParityShards=2, got %d", m.ParityShards)
	}

	if m.TotalShards != 6 {
		t.Errorf("Expected TotalShards=6, got %d", m.TotalShards)
	}

	// Verify collections
	if len(m.Chunks) != 2 {
		t.Errorf("Expected 2 chunks, got %d", len(m.Chunks))
	}

	if len(m.Shards) != 4 {
		t.Errorf("Expected 4 shards, got %d", len(m.Shards))
	}

	if len(m.Farmers) != 2 {
		t.Errorf("Expected 2 farmers, got %d", len(m.Farmers))
	}
}

func TestBlobID_Uniqueness(t *testing.T) {
	key := []byte("test-encryption-key-32-bytes!!")
	chunks := []ChunkMeta{{Index: 0, Hash: "hash0", Size: 1024}}
	shards := []ShardMeta{{ChunkIndex: 0, ShardIndex: 0, Hash: "s0", Size: 256, FarmerIndex: 0}}
	farmers := []FarmerInfo{{Index: 0, Address: "0xF1", Endpoint: "https://f1.io", Region: "us"}}

	m1 := New("test.bin", 1024, "hash", chunks, shards, farmers, key, "0xPub")
	m2 := New("test.bin", 1024, "hash", chunks, shards, farmers, key, "0xPub")

	if m1.BlobID == m2.BlobID {
		t.Error("BlobIDs should be unique")
	}

	// Verify format (0x + 64 hex chars)
	if len(m1.BlobID) != 66 { // "0x" + 64 hex chars
		t.Errorf("BlobID has wrong length: %d", len(m1.BlobID))
	}

	if m1.BlobID[:2] != "0x" {
		t.Error("BlobID should start with 0x")
	}
}

// ============================================================================
// SAVE/LOAD TESTS
// ============================================================================

func TestSaveLoad(t *testing.T) {
	chunks := []ChunkMeta{
		{Index: 0, Hash: "hash0", Size: 1048576},
		{Index: 1, Hash: "hash1", Size: 1048576},
	}

	shards := []ShardMeta{
		{ChunkIndex: 0, ShardIndex: 0, Hash: "shard00", Size: 262144, FarmerIndex: 0},
		{ChunkIndex: 0, ShardIndex: 1, Hash: "shard01", Size: 262144, FarmerIndex: 1},
		{ChunkIndex: 0, ShardIndex: 2, Hash: "shard02", Size: 262144, FarmerIndex: 2},
		{ChunkIndex: 1, ShardIndex: 0, Hash: "shard10", Size: 262144, FarmerIndex: 0},
		{ChunkIndex: 1, ShardIndex: 1, Hash: "shard11", Size: 262144, FarmerIndex: 1},
		{ChunkIndex: 1, ShardIndex: 2, Hash: "shard12", Size: 262144, FarmerIndex: 2},
	}

	farmers := []FarmerInfo{
		{Index: 0, Address: "0xFarmer1", Endpoint: "https://f1.btnx.io:4433", Region: "us-east-1"},
		{Index: 1, Address: "0xFarmer2", Endpoint: "https://f2.btnx.io:4433", Region: "us-west-1"},
		{Index: 2, Address: "0xFarmer3", Endpoint: "https://f3.btnx.io:4433", Region: "eu-west-1"},
	}

	key := []byte("test-key-32-bytes-long-padding!!")
	m := New("test.bin", 2097152, "filehash", chunks, shards, farmers, key, "0xPublisher")

	testFile := "test-manifest.json"
	defer os.Remove(testFile)

	// Save
	err := m.Save(testFile)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Fatal("Manifest file was not created")
	}

	// Load
	loaded, err := Load(testFile)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Verify all fields
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

	if loaded.DataShards != m.DataShards {
		t.Error("DataShards mismatch")
	}

	if loaded.ParityShards != m.ParityShards {
		t.Error("ParityShards mismatch")
	}

	if loaded.TotalShards != m.TotalShards {
		t.Error("TotalShards mismatch")
	}

	if len(loaded.Chunks) != len(m.Chunks) {
		t.Errorf("Chunks length mismatch: expected %d, got %d", len(m.Chunks), len(loaded.Chunks))
	}

	if len(loaded.Shards) != len(m.Shards) {
		t.Errorf("Shards length mismatch: expected %d, got %d", len(m.Shards), len(loaded.Shards))
	}

	if len(loaded.Farmers) != len(m.Farmers) {
		t.Errorf("Farmers length mismatch: expected %d, got %d", len(m.Farmers), len(loaded.Farmers))
	}

	// Verify chunk data
	for i, chunk := range loaded.Chunks {
		if chunk.Index != m.Chunks[i].Index {
			t.Errorf("Chunk %d index mismatch", i)
		}
		if chunk.Hash != m.Chunks[i].Hash {
			t.Errorf("Chunk %d hash mismatch", i)
		}
	}

	// Verify shard data
	for i, shard := range loaded.Shards {
		if shard.ChunkIndex != m.Shards[i].ChunkIndex {
			t.Errorf("Shard %d ChunkIndex mismatch", i)
		}
		if shard.ShardIndex != m.Shards[i].ShardIndex {
			t.Errorf("Shard %d ShardIndex mismatch", i)
		}
		if shard.FarmerIndex != m.Shards[i].FarmerIndex {
			t.Errorf("Shard %d FarmerIndex mismatch", i)
		}
	}

	// Verify farmer data
	for i, farmer := range loaded.Farmers {
		if farmer.Address != m.Farmers[i].Address {
			t.Errorf("Farmer %d address mismatch", i)
		}
		if farmer.Endpoint != m.Farmers[i].Endpoint {
			t.Errorf("Farmer %d endpoint mismatch", i)
		}
	}
}

func TestLoad_NonExistent(t *testing.T) {
	_, err := Load("nonexistent-manifest.json")
	if err == nil {
		t.Error("Expected error for non-existent manifest")
	}
}

// ============================================================================
// CHUNK QUERY TESTS
// ============================================================================

func TestGetChunkHash(t *testing.T) {
	chunks := []ChunkMeta{
		{Index: 0, Hash: "hash0", Size: 1024},
		{Index: 1, Hash: "hash1", Size: 1024},
		{Index: 2, Hash: "hash2", Size: 1024},
	}

	shards := []ShardMeta{}
	farmers := []FarmerInfo{}
	key := []byte("test-key-32-bytes-long-padding!!")
	
	m := New("test.bin", 3072, "filehash", chunks, shards, farmers, key, "0xPublisher")

	if m.GetChunkHash(0) != "hash0" {
		t.Error("GetChunkHash(0) failed")
	}

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

// ============================================================================
// SHARD QUERY TESTS
// ============================================================================

func TestGetShardsForChunk(t *testing.T) {
	chunks := []ChunkMeta{
		{Index: 0, Hash: "hash0", Size: 1048576},
		{Index: 1, Hash: "hash1", Size: 1048576},
	}

	shards := []ShardMeta{
		// Chunk 0 shards
		{ChunkIndex: 0, ShardIndex: 0, Hash: "c0s0", Size: 262144, FarmerIndex: 0},
		{ChunkIndex: 0, ShardIndex: 1, Hash: "c0s1", Size: 262144, FarmerIndex: 1},
		{ChunkIndex: 0, ShardIndex: 2, Hash: "c0s2", Size: 262144, FarmerIndex: 2},
		{ChunkIndex: 0, ShardIndex: 3, Hash: "c0s3", Size: 262144, FarmerIndex: 3},
		{ChunkIndex: 0, ShardIndex: 4, Hash: "c0s4", Size: 262144, FarmerIndex: 4},
		{ChunkIndex: 0, ShardIndex: 5, Hash: "c0s5", Size: 262144, FarmerIndex: 5},
		// Chunk 1 shards
		{ChunkIndex: 1, ShardIndex: 0, Hash: "c1s0", Size: 262144, FarmerIndex: 0},
		{ChunkIndex: 1, ShardIndex: 1, Hash: "c1s1", Size: 262144, FarmerIndex: 1},
		{ChunkIndex: 1, ShardIndex: 2, Hash: "c1s2", Size: 262144, FarmerIndex: 2},
		{ChunkIndex: 1, ShardIndex: 3, Hash: "c1s3", Size: 262144, FarmerIndex: 3},
	}

	farmers := []FarmerInfo{
		{Index: 0, Address: "0xF0", Endpoint: "https://f0.io", Region: "us-east"},
		{Index: 1, Address: "0xF1", Endpoint: "https://f1.io", Region: "us-west"},
		{Index: 2, Address: "0xF2", Endpoint: "https://f2.io", Region: "eu-west"},
		{Index: 3, Address: "0xF3", Endpoint: "https://f3.io", Region: "ap-south"},
		{Index: 4, Address: "0xF4", Endpoint: "https://f4.io", Region: "us-east-2"},
		{Index: 5, Address: "0xF5", Endpoint: "https://f5.io", Region: "eu-central"},
	}

	key := []byte("test-key-32-bytes-long-padding!!")
	m := New("test.bin", 2097152, "filehash", chunks, shards, farmers, key, "0xPublisher")

	// Test chunk 0 shards
	chunk0Shards := m.GetShardsForChunk(0)
	if len(chunk0Shards) != 6 {
		t.Errorf("Expected 6 shards for chunk 0, got %d", len(chunk0Shards))
	}

	for i, shard := range chunk0Shards {
		if shard.ChunkIndex != 0 {
			t.Errorf("Shard %d has wrong chunk index: %d", i, shard.ChunkIndex)
		}
		if shard.ShardIndex != i {
			t.Errorf("Expected shard index %d, got %d", i, shard.ShardIndex)
		}
	}

	// Test chunk 1 shards
	chunk1Shards := m.GetShardsForChunk(1)
	if len(chunk1Shards) != 4 {
		t.Errorf("Expected 4 shards for chunk 1, got %d", len(chunk1Shards))
	}

	// Test non-existent chunk
	chunk99Shards := m.GetShardsForChunk(99)
	if len(chunk99Shards) != 0 {
		t.Errorf("Expected 0 shards for non-existent chunk, got %d", len(chunk99Shards))
	}
}

func TestGetFarmerForShard(t *testing.T) {
	farmers := []FarmerInfo{
		{Index: 0, Address: "0xFarmer1", Endpoint: "https://f1.io", Region: "us-east-1"},
		{Index: 1, Address: "0xFarmer2", Endpoint: "https://f2.io", Region: "us-west-1"},
		{Index: 2, Address: "0xFarmer3", Endpoint: "https://f3.io", Region: "eu-west-1"},
	}

	shards := []ShardMeta{
		{ChunkIndex: 0, ShardIndex: 0, Hash: "s0", Size: 256, FarmerIndex: 0},
		{ChunkIndex: 0, ShardIndex: 1, Hash: "s1", Size: 256, FarmerIndex: 1},
		{ChunkIndex: 0, ShardIndex: 2, Hash: "s2", Size: 256, FarmerIndex: 2},
	}

	chunks := []ChunkMeta{{Index: 0, Hash: "hash0", Size: 1024}}
	key := []byte("test-key-32-bytes-long-padding!!")

	m := New("test.bin", 1024, "hash", chunks, shards, farmers, key, "0xPublisher")

	// Test valid farmer lookup
	farmer := m.GetFarmerForShard(shards[0])
	if farmer == nil {
		t.Fatal("GetFarmerForShard returned nil for valid shard")
	}
	if farmer.Address != "0xFarmer1" {
		t.Errorf("Expected 0xFarmer1, got %s", farmer.Address)
	}

	farmer2 := m.GetFarmerForShard(shards[1])
	if farmer2 == nil {
		t.Fatal("GetFarmerForShard returned nil")
	}
	if farmer2.Endpoint != "https://f2.io" {
		t.Errorf("Expected https://f2.io, got %s", farmer2.Endpoint)
	}

	// Test invalid farmer index
	invalidShard := ShardMeta{ChunkIndex: 0, ShardIndex: 0, Hash: "s", Size: 256, FarmerIndex: 99}
	farmer99 := m.GetFarmerForShard(invalidShard)
	if farmer99 != nil {
		t.Error("Expected nil for invalid farmer index")
	}

	// Test negative farmer index
	negativeShard := ShardMeta{ChunkIndex: 0, ShardIndex: 0, Hash: "s", Size: 256, FarmerIndex: -1}
	farmerNeg := m.GetFarmerForShard(negativeShard)
	if farmerNeg != nil {
		t.Error("Expected nil for negative farmer index")
	}
}

func TestGetFarmersForChunk(t *testing.T) {
	farmers := []FarmerInfo{
		{Index: 0, Address: "0xF0", Endpoint: "https://f0.io", Region: "us-east"},
		{Index: 1, Address: "0xF1", Endpoint: "https://f1.io", Region: "us-west"},
		{Index: 2, Address: "0xF2", Endpoint: "https://f2.io", Region: "eu-west"},
		{Index: 3, Address: "0xF3", Endpoint: "https://f3.io", Region: "ap-south"},
	}

	shards := []ShardMeta{
		// Chunk 0: stored on farmers 0, 1, 2, 3
		{ChunkIndex: 0, ShardIndex: 0, Hash: "c0s0", Size: 256, FarmerIndex: 0},
		{ChunkIndex: 0, ShardIndex: 1, Hash: "c0s1", Size: 256, FarmerIndex: 1},
		{ChunkIndex: 0, ShardIndex: 2, Hash: "c0s2", Size: 256, FarmerIndex: 2},
		{ChunkIndex: 0, ShardIndex: 3, Hash: "c0s3", Size: 256, FarmerIndex: 3},
		// Chunk 1: stored on farmers 0, 2 (only 2 shards)
		{ChunkIndex: 1, ShardIndex: 0, Hash: "c1s0", Size: 256, FarmerIndex: 0},
		{ChunkIndex: 1, ShardIndex: 1, Hash: "c1s1", Size: 256, FarmerIndex: 2},
	}

	chunks := []ChunkMeta{
		{Index: 0, Hash: "hash0", Size: 1024},
		{Index: 1, Hash: "hash1", Size: 1024},
	}

	key := []byte("test-key-32-bytes-long-padding!!")
	m := New("test.bin", 2048, "hash", chunks, shards, farmers, key, "0xPublisher")

	// Test chunk 0 farmers
	chunk0Farmers := m.GetFarmersForChunk(0)
	if len(chunk0Farmers) != 4 {
		t.Errorf("Expected 4 unique farmers for chunk 0, got %d", len(chunk0Farmers))
	}

	// Verify farmers are unique
	farmerAddresses := make(map[string]bool)
	for _, farmer := range chunk0Farmers {
		if farmerAddresses[farmer.Address] {
			t.Errorf("Duplicate farmer: %s", farmer.Address)
		}
		farmerAddresses[farmer.Address] = true
	}

	// Test chunk 1 farmers
	chunk1Farmers := m.GetFarmersForChunk(1)
	if len(chunk1Farmers) != 2 {
		t.Errorf("Expected 2 unique farmers for chunk 1, got %d", len(chunk1Farmers))
	}

	// Test non-existent chunk
	chunk99Farmers := m.GetFarmersForChunk(99)
	if len(chunk99Farmers) != 0 {
		t.Errorf("Expected 0 farmers for non-existent chunk, got %d", len(chunk99Farmers))
	}
}

func TestGetFarmersForChunk_Deduplication(t *testing.T) {
	// Test that duplicate farmer indices are properly deduplicated
	farmers := []FarmerInfo{
		{Index: 0, Address: "0xF0", Endpoint: "https://f0.io", Region: "us"},
		{Index: 1, Address: "0xF1", Endpoint: "https://f1.io", Region: "eu"},
	}

	// Multiple shards on same farmer
	shards := []ShardMeta{
		{ChunkIndex: 0, ShardIndex: 0, Hash: "s0", Size: 256, FarmerIndex: 0},
		{ChunkIndex: 0, ShardIndex: 1, Hash: "s1", Size: 256, FarmerIndex: 0}, // Same farmer!
		{ChunkIndex: 0, ShardIndex: 2, Hash: "s2", Size: 256, FarmerIndex: 1},
		{ChunkIndex: 0, ShardIndex: 3, Hash: "s3", Size: 256, FarmerIndex: 0}, // Same farmer again!
	}

	chunks := []ChunkMeta{{Index: 0, Hash: "hash0", Size: 1024}}
	key := []byte("test-key-32-bytes-long-padding!!")

	m := New("test.bin", 1024, "hash", chunks, shards, farmers, key, "0xPublisher")

	chunk0Farmers := m.GetFarmersForChunk(0)

	// Should only return 2 unique farmers, not 4
	if len(chunk0Farmers) != 2 {
		t.Errorf("Expected 2 unique farmers after deduplication, got %d", len(chunk0Farmers))
	}
}

// ============================================================================
// ENCRYPTION KEY TESTS
// ============================================================================

func TestGetEncryptionKey(t *testing.T) {
	key := []byte("test-key-32-bytes-long-padding!!")
	chunks := []ChunkMeta{{Index: 0, Hash: "hash0", Size: 1024}}
	shards := []ShardMeta{}
	farmers := []FarmerInfo{}

	m := New("test.bin", 1024, "hash", chunks, shards, farmers, key, "0xPub")

	retrievedKey, err := m.GetEncryptionKey()
	if err != nil {
		t.Fatalf("GetEncryptionKey failed: %v", err)
	}

	if len(retrievedKey) != 32 {
		t.Errorf("Expected key size 32, got %d", len(retrievedKey))
	}

	// Compare with original
	if !bytes.Equal(key, retrievedKey) {
		t.Error("Retrieved key doesn't match original")
	}
}

func TestGetEncryptionKey_InvalidHex(t *testing.T) {
	m := &Manifest{
		EncryptionKey: "invalid-hex-string", // Not valid hex
	}

	_, err := m.GetEncryptionKey()
	if err == nil {
		t.Error("Expected error for invalid hex string")
	}
}

// ============================================================================
// FILE HASH TESTS
// ============================================================================

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

	// Calculate hash again, should be deterministic
	hash2, _ := CalculateFileHash(testFile)
	if hash != hash2 {
		t.Error("Hash should be deterministic")
	}

	// Verify hash length (64 hex chars for SHA256)
	if len(hash) != 64 {
		t.Errorf("Expected hash length 64, got %d", len(hash))
	}
}

func TestCalculateFileHash_NonExistent(t *testing.T) {
	_, err := CalculateFileHash("nonexistent-file.bin")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

// ============================================================================
// INTEGRATION TEST
// ============================================================================

func TestManifest_CompleteWorkflow(t *testing.T) {
	// Simulate a complete manifest creation for a 2-chunk file
	
	// Step 1: Define chunks
	chunks := []ChunkMeta{
		{Index: 0, Hash: "abc123def456", Size: 1048576},
		{Index: 1, Hash: "789ghi012jkl", Size: 524288}, // Partial last chunk
	}

	// Step 2: Define shards (6 shards per chunk)
	var shards []ShardMeta
	for chunkIdx := 0; chunkIdx < 2; chunkIdx++ {
		for shardIdx := 0; shardIdx < 6; shardIdx++ {
			shards = append(shards, ShardMeta{
				ChunkIndex:  chunkIdx,
				ShardIndex:  shardIdx,
				Hash:        "shard_hash_placeholder",
				Size:        262144,
				FarmerIndex: shardIdx, // Farmer i stores all shard_i
			})
		}
	}

	// Step 3: Define farmers
	farmers := []FarmerInfo{
		{Index: 0, Address: "0xF0", Endpoint: "https://f0.btnx.io:4433", Region: "us-east-1"},
		{Index: 1, Address: "0xF1", Endpoint: "https://f1.btnx.io:4433", Region: "us-west-1"},
		{Index: 2, Address: "0xF2", Endpoint: "https://f2.btnx.io:4433", Region: "eu-west-1"},
		{Index: 3, Address: "0xF3", Endpoint: "https://f3.btnx.io:4433", Region: "ap-south-1"},
		{Index: 4, Address: "0xF4", Endpoint: "https://f4.btnx.io:4433", Region: "us-east-2"},
		{Index: 5, Address: "0xF5", Endpoint: "https://f5.btnx.io:4433", Region: "eu-central-1"},
	}

	// Step 4: Create manifest
	key := []byte("strong-encryption-key-32-bytes!!")
	m := New(
		"video.mp4",
		1572864, // 1.5 MB
		"original_file_hash_abc123",
		chunks,
		shards,
		farmers,
		key,
		"0xPublisher123",
	)

	// Step 5: Save to file
	testFile := "test-complete-manifest.json"
	defer os.Remove(testFile)

	if err := m.Save(testFile); err != nil {
		t.Fatalf("Failed to save manifest: %v", err)
	}

	// Step 6: Load from file
	loaded, err := Load(testFile)
	if err != nil {
		t.Fatalf("Failed to load manifest: %v", err)
	}

	// Step 7: Verify queries work correctly
	
	// Query chunk 0 shards
	chunk0Shards := loaded.GetShardsForChunk(0)
	if len(chunk0Shards) != 6 {
		t.Errorf("Expected 6 shards for chunk 0, got %d", len(chunk0Shards))
	}

	// Query farmers for chunk 1
	chunk1Farmers := loaded.GetFarmersForChunk(1)
	if len(chunk1Farmers) != 6 {
		t.Errorf("Expected 6 farmers for chunk 1, got %d", len(chunk1Farmers))
	}

	// Verify farmer lookup for specific shard
	shard := chunk0Shards[0]
	farmer := loaded.GetFarmerForShard(shard)
	if farmer == nil {
		t.Fatal("Expected farmer, got nil")
	}
	if farmer.Index != shard.FarmerIndex {
		t.Errorf("Farmer index mismatch: expected %d, got %d", shard.FarmerIndex, farmer.Index)
	}

	// Verify encryption key
	retrievedKey, err := loaded.GetEncryptionKey()
	if err != nil {
		t.Fatalf("Failed to retrieve key: %v", err)
	}
	if !bytes.Equal(retrievedKey, key) {
		t.Error("Encryption key mismatch")
	}

	t.Log("✅ Complete workflow test passed")
}
