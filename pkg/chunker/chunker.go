package chunker

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/reedsolomon"
)

const ChunkSize = 1024 * 1024 					// 1MB (1 * 1024 * 1024 bytes)
const DataShards = 4          					// 4 data shards per chunk
const ParityShards = 2        					// 2 parity shards per chunk
const TotalShards = DataShards + ParityShards 	// 6 total shards

// Chunk represents a file chunk struct with its metadata
type Chunk struct {
	Index int    `json:"index"` // chunk index
	Data  []byte `json:"-"`     // exclude raw data from JSON
	Hash  string `json:"hash"`  // SHA256 hash of the chunk
	Size  int    `json:"size"`  // size of the chunk in bytes
}

// ChunkResult is used for streaming to pass both data and potential read errors
type ChunkResult struct {
	Chunk Chunk
	Err   error
}

// Shard represents an erasure-coded shard of a chunk
type Shard struct {
    ChunkIndex int    `json:"chunk_index"` // which chunk this shard belongs to
    ShardIndex int    `json:"shard_index"` // which shard (0-5)
    Data       []byte `json:"-"`           // shard data (not in JSON)
    Hash       string `json:"hash"`        // SHA256 of shard data
    Size       int    `json:"size"`        // shard size in bytes
}

// StreamChunkFile reads a file and streams chunks to a returned channel.
// This allows processing huge files without loading them entirely into memory.
func StreamChunkFile(filePath string) <-chan ChunkResult {

	// Create a buffered channel to keep the pipeline busy
	out := make(chan ChunkResult, 4) // buffer of 4 chunks

	go func() {
		defer close(out)

		// open file
		file, err := os.Open(filePath)
		if err != nil {
			out <- ChunkResult{Err: fmt.Errorf("failed to open file: %w", err)}
			return
		}
		defer file.Close()

		index := 0                        // index to track chunk number
		buffer := make([]byte, ChunkSize) // a reusable buffer allocation of 1MB

	// read file in a loop
		for {
			n, err := io.ReadFull(file, buffer)

			if err == io.EOF {
				break // Exact EOF, we are done
			}
			if err == io.ErrUnexpectedEOF {
				// This is the last chunk (partial size). 
				// It's not a real error for us, just the end of file.
				err = nil
			}
			if err != nil {
				out <- ChunkResult{Err: fmt.Errorf("failed to read chunk %d: %w", index, err)}
				return
			}

			// Copy data to new slice (don't reuse buffer)
			chunkData := make([]byte, n)
			copy(chunkData, buffer[:n])

			hash := sha256.Sum256(chunkData) // Calculate SHA256 hash of plaintext

			// create chunk metadata
			chunk := Chunk{
				Index: index,
				Data:  chunkData,
				Hash:  hex.EncodeToString(hash[:]),
				Size:  n,
			}

			// Send to channel
			out <- ChunkResult{Chunk: chunk, Err: nil}
			index++

			// If we hit the partial chunk case (ErrUnexpectedEOF previously), we break now.
			if n < ChunkSize {
				break
			}
		}
	}()
	// return all chunks
	return out
}

// ShardChunk applies erasure coding to a single encrypted chunk
// Returns 6 shards: 4 data + 2 parity (any 4 can reconstruct)
// takes Chunk metadata and encrypted chunk data as input and returns slice of Shard structs
func ShardChunk(chunk Chunk, encryptedData []byte) ([]Shard, error) {
	
	// SAFETY CHECK: Ensure data matches metadata
	if len(encryptedData) != chunk.Size {
		return nil, fmt.Errorf("data size mismatch: expected %d, got %d", chunk.Size, len(encryptedData))
	}

    // Create Reed-Solomon encoder (4 data shards, 2 parity shards)
    enc, err := reedsolomon.New(DataShards, ParityShards)
    if err != nil {
        return nil, fmt.Errorf("failed to create encoder: %w", err)
    }

    // Split encrypted data into 4 equal parts
    shards, err := enc.Split(encryptedData) // returns [][]byte with length TotalShards
    if err != nil {
        return nil, fmt.Errorf("failed to split data: %w", err)
    }

    // Generate parity shards
    err = enc.Encode(shards)
    if err != nil {
        return nil, fmt.Errorf("failed to encode shards: %w", err)
    }

    // Create shard metadata
    var shardList []Shard
	// Calculate hash for each shard and create Shard struct
    for i := 0; i < TotalShards; i++ {
        shardHash := sha256.Sum256(shards[i]) // returns [32]byte
        
        shard := Shard{
            ChunkIndex: chunk.Index,
            ShardIndex: i,
            Data:       shards[i],
            Hash:       hex.EncodeToString(shardHash[:] /* convert to slice*/),
            Size:       len(shards[i]), // size in bytes
        }
        shardList = append(shardList, shard) // append to shard list []shard
    }

    return shardList, nil
}

// ReconstructChunk rebuilds original encrypted chunk from any 4+ shards
func ReconstructChunk(shards []Shard, dataSize int) ([]byte, error) {

	if len(shards) < DataShards {
		return nil, fmt.Errorf("need at least %d shards, got %d", DataShards, len(shards))
	}

	if dataSize <= 0 {
		return nil, fmt.Errorf("invalid data size")
	}

	expectedChunk := shards[0].ChunkIndex
	for _, s := range shards {
		if s.ChunkIndex != expectedChunk {
			return nil, fmt.Errorf("shards belong to different chunks")
		}
		if !VerifyShard(s.Data, s.Hash) {
            return nil, fmt.Errorf("shard %d failed hash verification", s.ShardIndex)
        }
	}

    // Create encoder
    enc, err := reedsolomon.New(DataShards, ParityShards)
    if err != nil {
        return nil, fmt.Errorf("failed to create encoder: %w", err)
    }

    // Prepare nil shard array 
    shardData := make([][]byte, TotalShards)

    // Fill in available shards
    for _, shard := range shards {
        if shard.ShardIndex < 0 || shard.ShardIndex >= TotalShards {
            return nil, fmt.Errorf("invalid shard index %d", shard.ShardIndex)
        }
        if shardData[shard.ShardIndex] != nil {
            return nil, fmt.Errorf("duplicate shard index %d", shard.ShardIndex)
        }
        shardData[shard.ShardIndex] = shard.Data	
    }

    // Reconstruct missing shards
    err = enc.Reconstruct(shardData)
    if err != nil {
        return nil, fmt.Errorf("failed to reconstruct: %w", err)
    }

    // Verify reconstruction
    ok, err := enc.Verify(shardData)
    if err != nil {
        return nil, fmt.Errorf("verification failed: %w", err)
    }
    if !ok {
        return nil, fmt.Errorf("reconstructed data failed verification")
    }

    // Create a buffer to act as the io.Writer
    var buf bytes.Buffer

    // Join combines the shards and writes them to buf.
    // Ideally, pass the original data size. If dataSize is passed, 
    // Join will automatically strip the zero-padding bytes.
    err = enc.Join(&buf, shardData, dataSize)
    if err != nil {
        return nil, fmt.Errorf("failed to join shards: %w", err)
    }

    return buf.Bytes(), nil
}

// AssembleChunks consumes a stream of chunks and writes them to the output file.
// Uses WriteAt, so chunks can arrive out of order (good for parallel downloads).
func AssembleChunks(chunkStream <-chan Chunk, outputPath string, totalChunks int) error {
	// create output file / overwrite to 0 byte if exists
	output, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer output.Close()

	// Track received chunks to prevent sparse files (holes)
	received := make([]bool, totalChunks)
    uniqueCount := 0

	// write chunks in order
	for chunk := range chunkStream {

		// Skip duplicates
		if chunk.Index < 0 || chunk.Index >= totalChunks {
            return fmt.Errorf("chunk index %d out of bounds (max %d)", chunk.Index, totalChunks-1)
        }
		// Skip if already received
		if received[chunk.Index] {
            continue 
        }

		// Calculate offset based on index (Index * 1MB)
		offset := int64(chunk.Index) * int64(ChunkSize)

		// WriteAt allows random access writing
		_, err := output.WriteAt(chunk.Data, offset)
		if err != nil {
			return fmt.Errorf("failed to write chunk %d: %w", chunk.Index, err)
		}
		// Mark as received
        received[chunk.Index] = true
        uniqueCount++
	}

	// VALIDATION: Ensure we actually got everything
	if uniqueCount != totalChunks {
		return fmt.Errorf("incomplete file: expected %d chunks, got %d", totalChunks, uniqueCount)
	}
	return nil
}

// VerifyChunk checks if chunk hash matches expected
func VerifyChunk(data []byte, expectedHash string) bool {
	actualHash := sha256.Sum256(data)
	return hex.EncodeToString(actualHash[:]) == expectedHash
}

// VerifyShard checks if shard hash matches expected
func VerifyShard(data []byte, expectedHash string) bool {
    actualHash := sha256.Sum256(data)
    return hex.EncodeToString(actualHash[:]) == expectedHash
}