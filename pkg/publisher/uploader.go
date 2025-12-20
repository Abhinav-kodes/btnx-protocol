package publisher

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/Abhinav-kodes/dbxn/pkg/crypto"
	"github.com/Abhinav-kodes/dbxn/pkg/chunker"
	"github.com/Abhinav-kodes/dbxn/pkg/manifest"
)

// UploadConfig holds configuration for file upload
type UploadConfig struct {
	FilePath         string   // Path to file to upload
	FarmerEndpoints  []string // List of farmer HTTP endpoints
	PublisherAddress string   // Publisher's wallet address
	OutputPath       string   // Where to save manifest.json
	Parallelism      int      // Number of parallel uploads (default: 4)
}

// UploadStats tracks upload progress
type UploadStats struct {
	ChunksProcessed  int // Total chunks processed
	ShardsCreated    int // Total shards created
	ShardsUploaded   int // Total shards uploaded
	BytesUploaded    int64 // Total bytes uploaded
	StartTime        time.Time // Upload start time
	EndTime          time.Time // Upload end time
	Errors           []error // List of errors encountered during upload
}

// ShardUploadRequest is the JSON payload sent to farmers
type ShardUploadRequest struct {
	BlobID     string `json:"blob_id"`    // ID for the file
	ChunkIndex int    `json:"chunk_index"`
	ShardIndex int    `json:"shard_index"`
	Data       []byte `json:"data"`       // base64 encoded by json.Marshal
	Hash       string `json:"hash"` 	// SHA256 of shard
	Size       int    `json:"size"` // size of shard in bytes
}

// ShardUploadResponse is returned by farmers
type ShardUploadResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Hash    string `json:"hash"` // Farmer confirms hash
}

// Upload orchestrates the complete file upload process 
func Upload(config UploadConfig) (*manifest.Manifest, *UploadStats, error) {
	stats := &UploadStats{
		StartTime: time.Now(),
		Errors:    make([]error, 0),
	}

	// Validate config
	if err := validateConfig(config); err != nil {
		return nil, stats, fmt.Errorf("invalid config: %w", err)
	}

	fmt.Printf("📦 Starting upload: %s\n", filepath.Base(config.FilePath))
	fmt.Printf("🌐 Farmers: %d endpoints\n", len(config.FarmerEndpoints))

	// Step 1: Calculate original file hash
	fmt.Println("\n📊 Calculating file hash...")
	fileHash, err := manifest.CalculateFileHash(config.FilePath)
	if err != nil {
		return nil, stats, fmt.Errorf("failed to hash file: %w", err)
	}
	fmt.Printf("✓ File hash: %s\n", fileHash[:16]+"...")

	// Step 2: Generate encryption key
	fmt.Println("\n🔐 Generating encryption key...")
	encKey, err := crypto.GenerateKey()
	if err != nil {
		return nil, stats, fmt.Errorf("failed to generate key: %w", err)
	}
	fmt.Println("✓ Encryption key generated")

	// Step 3: Process file (chunk → encrypt → shard)
	fmt.Println("\n⚙️  Processing file...")
	chunks, allShards, err := processFile(config.FilePath, encKey, stats)
	if err != nil {
		return nil, stats, fmt.Errorf("failed to process file: %w", err)
	}

	fmt.Printf("✓ Processed: %d chunks → %d shards\n", len(chunks), len(allShards))

	// Step 4: Build manifest with farmer assignments
	fmt.Println("\n📋 Building manifest...")
	farmers := buildFarmerInfo(config.FarmerEndpoints)
	m := buildManifest(
		config.FilePath,
		chunks,
		allShards,
		farmers,
		encKey,
		config.PublisherAddress,
	)
	fmt.Printf("✓ Manifest created (Blob ID: %s)\n", m.BlobID[:16]+"...")

	// Step 5: Distribute shards to farmers
	fmt.Println("\n🚀 Uploading shards to farmers...")
	if err := distributeShardsParallel(m, allShards, farmers, config.Parallelism, stats); err != nil {
		return nil, stats, fmt.Errorf("failed to distribute shards: %w", err)
	}

	// Step 6: Save manifest
	fmt.Println("\n💾 Saving manifest...")
	if err := m.Save(config.OutputPath); err != nil {
		return nil, stats, fmt.Errorf("failed to save manifest: %w", err)
	}
	fmt.Printf("✓ Manifest saved: %s\n", config.OutputPath)

	stats.EndTime = time.Now()
	printStats(stats)

	return m, stats, nil
}