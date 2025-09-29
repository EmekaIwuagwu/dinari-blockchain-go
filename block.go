package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// =============================================================================
// BLOCK STRUCTURE & CREATION
// =============================================================================

// Block represents a blockchain block containing transactions
type Block struct {
	Number        int64          `json:"number"`         // Block height/number
	Timestamp     int64          `json:"timestamp"`      // When block was created
	PreviousHash  string         `json:"previous_hash"`  // Hash of previous block
	Transactions  []*Transaction `json:"transactions"`   // List of transactions
	MerkleRoot    string         `json:"merkle_root"`    // Merkle root of transactions
	Nonce         int64          `json:"nonce"`          // Proof of Work nonce
	Hash          string         `json:"hash"`           // Block hash (calculated)
	Difficulty    int            `json:"difficulty"`     // Mining difficulty
	Miner         string         `json:"miner"`          // Miner's DT address
	Reward        float64        `json:"reward"`         // Block reward for miner
	TotalFees     float64        `json:"total_fees"`     // Total transaction fees
	Size          int            `json:"size"`           // Block size in bytes
}

// BlockHeader represents just the header information for efficient validation
type BlockHeader struct {
	Number       int64  `json:"number"`
	Timestamp    int64  `json:"timestamp"`
	PreviousHash string `json:"previous_hash"`
	MerkleRoot   string `json:"merkle_root"`
	Nonce        int64  `json:"nonce"`
	Hash         string `json:"hash"`
	Difficulty   int    `json:"difficulty"`
}

// CreateBlock creates a new block with given transactions
func CreateBlock(number int64, previousHash string, transactions []*Transaction, minerAddress string, difficulty int, reward float64) *Block {
	block := &Block{
		Number:       number,
		Timestamp:    time.Now().Unix(),
		PreviousHash: previousHash,
		Transactions: transactions,
		Nonce:        0,
		Hash:         "",
		Difficulty:   difficulty,
		Miner:        minerAddress,
		Reward:       reward,
	}
	
	// Calculate derived fields
	block.MerkleRoot = block.calculateMerkleRoot()
	block.TotalFees = block.calculateTotalFees()
	block.Size = block.calculateSize()
	
	return block
}

// CreateGenesisBlock creates the first block in the blockchain
func CreateGenesisBlock(minerAddress string) *Block {
	genesisBlock := &Block{
		Number:       0,
		Timestamp:    time.Now().Unix(),
		PreviousHash: "0000000000000000000000000000000000000000000000000000000000000000",
		Transactions: []*Transaction{},
		MerkleRoot:   "0000000000000000000000000000000000000000000000000000000000000000",
		Nonce:        0,
		Hash:         "",
		Difficulty:   4,
		Miner:        minerAddress,
		Reward:       0.0,
		TotalFees:    0.0,
		Size:         0,
	}
	
	return genesisBlock
}

// =============================================================================
// BLOCK HASHING & MERKLE TREE
// =============================================================================

// CalculateHash calculates the hash of the block
func (b *Block) CalculateHash() string {
	data := fmt.Sprintf("%d%d%s%s%s%d%d%.8f",
		b.Number,
		b.Timestamp,
		b.PreviousHash,
		b.MerkleRoot,
		b.Miner,
		b.Nonce,
		b.Difficulty,
		b.Reward,
	)
	
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

// calculateMerkleRoot calculates the Merkle root of all transactions
func (b *Block) calculateMerkleRoot() string {
	if len(b.Transactions) == 0 {
		return "0000000000000000000000000000000000000000000000000000000000000000"
	}
	
	var hashes []string
	for _, tx := range b.Transactions {
		hashes = append(hashes, tx.CalculateTransactionHash())
	}
	
	// Build Merkle tree bottom-up
	for len(hashes) > 1 {
		var nextLevel []string
		
		for i := 0; i < len(hashes); i += 2 {
			var combined string
			
			if i+1 < len(hashes) {
				combined = hashes[i] + hashes[i+1]
			} else {
				combined = hashes[i] + hashes[i]
			}
			
			hash := sha256.Sum256([]byte(combined))
			nextLevel = append(nextLevel, hex.EncodeToString(hash[:]))
		}
		
		hashes = nextLevel
	}
	
	return hashes[0]
}

// calculateTotalFees calculates total transaction fees in the block
func (b *Block) calculateTotalFees() float64 {
	total := 0.0
	for _, tx := range b.Transactions {
		total += tx.Fee
	}
	return total
}

// calculateSize calculates the approximate size of the block in bytes
func (b *Block) calculateSize() int {
	jsonData, _ := json.Marshal(b)
	return len(jsonData)
}

// =============================================================================
// PROOF OF WORK MINING
// =============================================================================

// MiningResult contains the result of a mining operation
type MiningResult struct {
	Success     bool          `json:"success"`
	Block       *Block        `json:"block,omitempty"`
	Hash        string        `json:"hash,omitempty"`
	Nonce       int64         `json:"nonce,omitempty"`
	Attempts    int64         `json:"attempts"`
	Duration    time.Duration `json:"duration"`
	HashRate    float64       `json:"hash_rate"` // Hashes per second
	Error       string        `json:"error,omitempty"`
}

// MineBlock performs Proof of Work mining on the block
func (b *Block) MineBlock() *MiningResult {
	target := strings.Repeat("0", b.Difficulty)
	startTime := time.Now()
	attempts := int64(0)
	
	for {
		hash := b.CalculateHash()
		attempts++
		
		if strings.HasPrefix(hash, target) {
			b.Hash = hash
			duration := time.Since(startTime)
			hashRate := float64(attempts) / duration.Seconds()
			
			return &MiningResult{
				Success:  true,
				Block:    b,
				Hash:     hash,
				Nonce:    b.Nonce,
				Attempts: attempts,
				Duration: duration,
				HashRate: hashRate,
			}
		}
		
		b.Nonce++
	}
}

// MineBlockWithTimeout performs mining with a timeout
func (b *Block) MineBlockWithTimeout(timeout time.Duration) *MiningResult {
	target := strings.Repeat("0", b.Difficulty)
	startTime := time.Now()
	attempts := int64(0)
	
	for time.Since(startTime) < timeout {
		hash := b.CalculateHash()
		attempts++
		
		if strings.HasPrefix(hash, target) {
			b.Hash = hash
			duration := time.Since(startTime)
			hashRate := float64(attempts) / duration.Seconds()
			
			return &MiningResult{
				Success:  true,
				Block:    b,
				Hash:     hash,
				Nonce:    b.Nonce,
				Attempts: attempts,
				Duration: duration,
				HashRate: hashRate,
			}
		}
		
		b.Nonce++
	}
	
	// Timeout reached
	duration := time.Since(startTime)
	hashRate := float64(attempts) / duration.Seconds()
	
	return &MiningResult{
		Success:  false,
		Attempts: attempts,
		Duration: duration,
		HashRate: hashRate,
		Error:    "mining timeout reached",
	}
}

// MineBlockConcurrent performs concurrent mining with multiple goroutines
func (b *Block) MineBlockConcurrent(workers int) *MiningResult {
	target := strings.Repeat("0", b.Difficulty)
	startTime := time.Now()
	
	// Channels for communication
	found := make(chan *MiningResult, 1)
	quit := make(chan bool, workers)
	
	var totalAttempts int64
	var mutex sync.Mutex
	
	// Start worker goroutines
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			localNonce := int64(workerID)
			attempts := int64(0)
			
			for {
				select {
				case <-quit:
					return
				default:
					// Create a copy of the block for this worker
					blockCopy := *b
					blockCopy.Nonce = localNonce
					hash := blockCopy.CalculateHash()
					attempts++
					
					if strings.HasPrefix(hash, target) {
						duration := time.Since(startTime)
						
						mutex.Lock()
						totalAttempts += attempts
						mutex.Unlock()
						
						hashRate := float64(totalAttempts) / duration.Seconds()
						
						// Update original block
						b.Nonce = localNonce
						b.Hash = hash
						
						result := &MiningResult{
							Success:  true,
							Block:    b,
							Hash:     hash,
							Nonce:    localNonce,
							Attempts: totalAttempts,
							Duration: duration,
							HashRate: hashRate,
						}
						
						select {
						case found <- result:
						default:
						}
						return
					}
					
					localNonce += int64(workers)
				}
			}
		}(i)
	}
	
	// Wait for result
	result := <-found
	
	// Stop all workers
	for i := 0; i < workers; i++ {
		quit <- true
	}
	
	return result
}

// =============================================================================
// BLOCK VALIDATION
// =============================================================================

// IsValidBlock performs comprehensive block validation
func (b *Block) IsValidBlock(previousBlock *Block) error {
	// Validate basic fields
	if b.Number < 0 {
		return fmt.Errorf("invalid block number: %d", b.Number)
	}
	
	if b.Hash == "" {
		return fmt.Errorf("block hash is empty")
	}
	
	// Validate previous hash link (except for genesis block)
	if b.Number > 0 {
		if previousBlock == nil {
			return fmt.Errorf("previous block is required for block %d", b.Number)
		}
		
		if b.Number != previousBlock.Number+1 {
			return fmt.Errorf("invalid block number sequence: expected %d, got %d", 
				previousBlock.Number+1, b.Number)
		}
		
		if b.PreviousHash != previousBlock.Hash {
			return fmt.Errorf("invalid previous hash: expected %s, got %s", 
				previousBlock.Hash, b.PreviousHash)
		}
	}
	
	// Validate hash meets difficulty target
	target := strings.Repeat("0", b.Difficulty)
	if !strings.HasPrefix(b.Hash, target) {
		return fmt.Errorf("block hash does not meet difficulty target")
	}
	
	// Verify hash is correct
	calculatedHash := b.CalculateHash()
	if b.Hash != calculatedHash {
		return fmt.Errorf("invalid block hash: expected %s, got %s", 
			calculatedHash, b.Hash)
	}
	
	// Validate Merkle root
	calculatedMerkleRoot := b.calculateMerkleRoot()
	if b.MerkleRoot != calculatedMerkleRoot {
		return fmt.Errorf("invalid Merkle root: expected %s, got %s", 
			calculatedMerkleRoot, b.MerkleRoot)
	}
	
	// Validate all transactions
	for i, tx := range b.Transactions {
		if err := tx.IsValidTransaction(); err != nil {
			return fmt.Errorf("invalid transaction %d: %v", i, err)
		}
	}
	
	// Validate miner address
	if !ValidateAddress(b.Miner) {
		return fmt.Errorf("invalid miner address: %s", b.Miner)
	}
	
	// Validate total fees calculation
	calculatedFees := b.calculateTotalFees()
	if b.TotalFees != calculatedFees {
		return fmt.Errorf("invalid total fees: expected %.8f, got %.8f", 
			calculatedFees, b.TotalFees)
	}
	
	return nil
}

// =============================================================================
// BLOCK UTILITIES & SERIALIZATION
// =============================================================================

// GetHeader returns just the block header
func (b *Block) GetHeader() *BlockHeader {
	return &BlockHeader{
		Number:       b.Number,
		Timestamp:    b.Timestamp,
		PreviousHash: b.PreviousHash,
		MerkleRoot:   b.MerkleRoot,
		Nonce:        b.Nonce,
		Hash:         b.Hash,
		Difficulty:   b.Difficulty,
	}
}

// GetTransactionByID finds a transaction in the block by ID
func (b *Block) GetTransactionByID(txID string) *Transaction {
	for _, tx := range b.Transactions {
		if tx.ID == txID {
			return tx
		}
	}
	return nil
}

// ToJSON converts block to JSON string
func (b *Block) ToJSON() (string, error) {
	jsonBytes, err := json.Marshal(b)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

// BlockFromJSON creates block from JSON string
func BlockFromJSON(jsonStr string) (*Block, error) {
	var block Block
	err := json.Unmarshal([]byte(jsonStr), &block)
	if err != nil {
		return nil, err
	}
	return &block, nil
}

// UpdateMerkleRoot recalculates and updates the Merkle root
func (b *Block) UpdateMerkleRoot() {
	b.MerkleRoot = b.calculateMerkleRoot()
}

// UpdateTotalFees recalculates and updates the total fees
func (b *Block) UpdateTotalFees() {
	b.TotalFees = b.calculateTotalFees()
}

// UpdateSize recalculates and updates the block size
func (b *Block) UpdateSize() {
	b.Size = b.calculateSize()
}