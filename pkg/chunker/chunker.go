package chunker

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

const ChunkSize = 1024 * 1024 // 1MB (1 * 1024 * 1024 bytes)

// Chunk represents a file chunk struct with its metadata
type Chunk struct {
	Index int    `json:"index"` // chunk index
	Data  []byte `json:"-"`     // exclude raw data from JSON
	Hash  string `json:"hash"`  // SHA256 hash of the chunk
	Size  int    `json:"size"`  // size of the chunk in bytes
}

// ChunkFile splits a file into 1MB chunks, accumulating them into a slice and returning it
func ChunkFile(filePath string) ([]Chunk, error) {

	// open file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	var chunks []Chunk                // chunks will be accumulated in chunks slice
	buffer := make([]byte, ChunkSize) // a reusable buffer allocation of 1MB
	index := 0                        // index to track chunk number

	// read file in a loop
	for {
		n, err := file.Read(buffer) // read up to ChunkSize bytes (1MB)

		// break on EOF
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk %d: %w", index, err)
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

		chunks = append(chunks, chunk) // append chunk to chunks slice
		index++
	}

	// return all chunks
	return chunks, nil
}

// AssembleChunks reassembles chunks into a file
func AssembleChunks(chunks []Chunk, outputPath string) error {
	// create output file / overwrite to 0 byte if exists
	output, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer output.Close()

	// write chunks in order
	for i, chunk := range chunks {
		// verify chunk index
		if chunk.Index != i {
			return fmt.Errorf("chunk %d missing or out of order", i)
		}

		// write chunk data to output file
		_, err := output.Write(chunk.Data)
		if err != nil {
			return fmt.Errorf("failed to write chunk %d: %w", i, err)
		}
	}
	return nil
}

// VerifyChunk checks if chunk hash matches expected
func VerifyChunk(data []byte, expectedHash string) bool {
	actualHash := sha256.Sum256(data)
	return hex.EncodeToString(actualHash[:]) == expectedHash
}
