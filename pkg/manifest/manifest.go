package manifest

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Manifest struct {
	Version          string      `json:"version"` 				// manifest version
	BlobID           string      `json:"blob_id"` 				// unique blob identifier
	FileName         string      `json:"file_name"` 			// original file name
	FileSize         int64       `json:"file_size"`				// original file size in bytes
	OriginalFileHash string      `json:"original_file_hash"`	// SHA256 hash of original file
	ChunkSize        int         `json:"chunk_size"`			// size of each chunk in bytes
	ChunkCount       int         `json:"chunk_count"`			// total number of chunks

	DataShards       int          `json:"data_shards"`   // 4
    ParityShards     int          `json:"parity_shards"` // 2
    TotalShards      int          `json:"total_shards"`  // 6
	Chunks           []ChunkMeta `json:"chunks"`  				// metadata for each chunk
	Shards           []ShardMeta  `json:"shards"`				// metadata for each shard
	Farmers          []FarmerInfo `json:"farmers"`				// list of farmers storing the chunks
	EncryptionKey    string      `json:"encryption_key"`		// hex-encoded encryption key for chunks
	CreatedAt        time.Time   `json:"created_at"`			// timestamp of manifest creation
	PublisherAddress string      `json:"publisher_address"`		// address of the publisher
}

// ChunkMeta represents metadata for a file chunk
type ChunkMeta struct {
	Index int    `json:"index"` // chunk index
	Hash  string `json:"hash"`  // SHA256 of plaintext chunk
	Size  int    `json:"size"`  // size of chunk in bytes
}

// ShardMeta represents metadata for an erasure-coded shard
type ShardMeta struct {
    ChunkIndex   int    `json:"chunk_index"`   // which chunk (0-99)
    ShardIndex   int    `json:"shard_index"`   // which shard (0-5)
    Hash         string `json:"hash"`          // SHA256 of shard
    Size         int    `json:"size"`          // shard size in bytes
    FarmerIndex  int    `json:"farmer_index"`  // which farmer stores this
}

type FarmerInfo struct {
    Index    int    `json:"index"`    // farmer index (0-5)
    Address  string `json:"address"`  // farmer wallet address
    Endpoint string `json:"endpoint"` // HTTP endpoint (e.g., "https://farmer1.dbxn.io:4433")
    Region   string `json:"region"`   // geographic region (e.g., "us-east-1")
}

// New creates a new manifest
func New(
	fileName string,
	fileSize int64,
	originalHash string,
	chunks []ChunkMeta,
	shards []ShardMeta,
    farmers []FarmerInfo, 
	encKey []byte,
	publisher string,
) *Manifest {
	return &Manifest{
		Version:          "1.0",
		BlobID:           generateBlobID(),
		FileName:         fileName,
		FileSize:         fileSize,
		OriginalFileHash: originalHash,
		ChunkSize:        1024 * 1024, // 1MB
		ChunkCount:       len(chunks),
		DataShards:       4,
        ParityShards:     2,
        TotalShards:      6,
		Chunks:           chunks,
		Shards:           shards,
		Farmers:          farmers,
		EncryptionKey:    hex.EncodeToString(encKey),
		CreatedAt:        time.Now(),
		PublisherAddress: publisher,
	}
}


// generateBlobID creates a random 32-byte blob ID
func generateBlobID() string {
	b := make([]byte, 32)
	rand.Read(b)
	return "0x" + hex.EncodeToString(b)
}

// Save writes manifest to JSON file
func (m *Manifest) Save(path string) error {
	// Serialize the manifest structure into human-readable JSON
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	// Write the JSON manifest to the path with Owner-writable, world-readable permissions
	err = os.WriteFile(path, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write manifest: %w", err)
	}

	return nil
}


// Load reads manifest from JSON file
func Load(path string) (*Manifest, error) {
	// Read the JSON manifest from the specified path
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest: %w", err)
	}

	var m Manifest
	// Deserialize the JSON data into a Manifest structure
	err = json.Unmarshal(data, &m)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal manifest: %w", err)
	}

	return &m, nil
}

// GetChunkHash returns hash for a given chunk index
func (m *Manifest) GetChunkHash(index int) string {
	// Iterate through chunks to find the hash for the specified index
	for _, chunk := range m.Chunks {
		if chunk.Index == index {
			return chunk.Hash
		}
	}
	return ""
}

// GetShardsForChunk returns all shards metadata for a given chunk index
func (m *Manifest) GetShardsForChunk(chunkIndex int) []ShardMeta {
    var shards []ShardMeta
    for _, shard := range m.Shards {
        if shard.ChunkIndex == chunkIndex {
            shards = append(shards, shard)
        }
    }
    return shards
}

// GetFarmerForShard returns the FarmerInfo for a given shard
func (m *Manifest) GetFarmerForShard(shard ShardMeta) *FarmerInfo {
    if shard.FarmerIndex >= 0 && shard.FarmerIndex < len(m.Farmers) {
        return &m.Farmers[shard.FarmerIndex]
    }
    return nil
}

// GetFarmersForChunk returns unique farmers storing shards for a given chunk index
func (m *Manifest) GetFarmersForChunk(chunkIndex int) []FarmerInfo {
    shards := m.GetShardsForChunk(chunkIndex)
    farmerMap := make(map[int]bool)
    var farmers []FarmerInfo

    for _, shard := range shards {
        if !farmerMap[shard.FarmerIndex] {
            farmerMap[shard.FarmerIndex] = true
            if farmer := m.GetFarmerForShard(shard); farmer != nil {
                farmers = append(farmers, *farmer)
            }
        }
    }

    return farmers
}

// GetEncryptionKey returns the encryption key as bytes
func (m *Manifest) GetEncryptionKey() ([]byte, error) {
	return hex.DecodeString(m.EncryptionKey)
}

// CalculateFileHash computes SHA256 hash of entire file
func CalculateFileHash(filePath string) (string, error) {
	// Read the JSON manifest from the specified path
	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read file: %w", err)
	}

	// Compute SHA256 hash of the file data
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}