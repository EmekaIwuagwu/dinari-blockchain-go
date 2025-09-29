package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// =============================================================================
// DATABASE STORAGE KEYS
// =============================================================================

const (
	// Blockchain data prefixes
	BlockPrefix      = "block:"
	BlockHashPrefix  = "blockhash:"
	BlockHeightKey   = "chainheight"
	GenesisHashKey   = "genesishash"
	
	// Transaction data prefixes
	TransactionPrefix = "tx:"
	TxBlockPrefix    = "txblock:"
	AddressTxPrefix  = "addrtx:"
	
	// Balance and state prefixes
	BalancePrefix = "balance:"
	NoncePrefix   = "nonce:"
	
	// Mempool data prefixes (optional persistence)
	MempoolTxPrefix = "mempool:"
	MempoolCountKey = "mempoolcount"
	
	// Metadata
	MetadataPrefix   = "meta:"
	LastUpdateKey    = "lastupdate"
	NodeVersionKey   = "nodeversion"
	
	// Statistics
	StatsPrefix = "stats:"
	TotalTxKey  = "totaltx"
	TotalValueKey = "totalvalue"
)

// =============================================================================
// PERSISTENCE MANAGER
// =============================================================================

// PersistenceManager handles all database operations
type PersistenceManager struct {
	db           *leveldb.DB
	dataDir      string
	enableMempool bool
	mutex        sync.RWMutex
	writeBuffer  map[string][]byte // Batch write buffer
	batchSize    int
	autoSync     bool
}

// PersistenceConfig contains persistence configuration
type PersistenceConfig struct {
	DataDir       string `json:"data_dir"`
	EnableMempool bool   `json:"enable_mempool_persist"`
	BatchSize     int    `json:"batch_size"`
	AutoSync      bool   `json:"auto_sync"`
}

// NewPersistenceManager creates a new persistence manager
func NewPersistenceManager(config *PersistenceConfig) (*PersistenceManager, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	// Open LevelDB database
	dbPath := filepath.Join(config.DataDir, "blockchain.db")
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	pm := &PersistenceManager{
		db:            db,
		dataDir:       config.DataDir,
		enableMempool: config.EnableMempool,
		writeBuffer:   make(map[string][]byte),
		batchSize:     config.BatchSize,
		autoSync:      config.AutoSync,
	}

	return pm, nil
}

// Close closes the database connection
func (pm *PersistenceManager) Close() error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	// Flush any pending writes
	if len(pm.writeBuffer) > 0 {
		pm.flushBatch()
	}

	return pm.db.Close()
}

// =============================================================================
// BLOCK STORAGE OPERATIONS
// =============================================================================

// SaveBlock saves a block to the database
func (pm *PersistenceManager) SaveBlock(block *Block) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	// Serialize block to JSON
	blockData, err := json.Marshal(block)
	if err != nil {
		return fmt.Errorf("failed to serialize block: %v", err)
	}

	// Prepare batch writes
	batch := make(map[string][]byte)
	
	// Store block by number
	blockKey := BlockPrefix + strconv.FormatInt(block.Number, 10)
	batch[blockKey] = blockData
	
	// Store block by hash for quick lookup
	hashKey := BlockHashPrefix + block.Hash
	batch[hashKey] = []byte(strconv.FormatInt(block.Number, 10))
	
	// Update chain height
	batch[BlockHeightKey] = []byte(strconv.FormatInt(block.Number, 10))
	
	// Store genesis hash if this is genesis block
	if block.Number == 0 {
		batch[GenesisHashKey] = []byte(block.Hash)
	}

	// Save all transactions in the block
	for _, tx := range block.Transactions {
		if err := pm.saveTransactionBatch(tx, block.Number, batch); err != nil {
			return fmt.Errorf("failed to save transaction %s: %v", tx.ID, err)
		}
	}

	// Update statistics
	pm.updateStatsBatch(block, batch)

	// Execute batch write
	return pm.executeBatch(batch)
}

// LoadBlock loads a block by number
func (pm *PersistenceManager) LoadBlock(number int64) (*Block, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	key := BlockPrefix + strconv.FormatInt(number, 10)
	data, err := pm.db.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil // Block not found
		}
		return nil, fmt.Errorf("failed to load block: %v", err)
	}

	var block Block
	if err := json.Unmarshal(data, &block); err != nil {
		return nil, fmt.Errorf("failed to deserialize block: %v", err)
	}

	return &block, nil
}

// LoadBlockByHash loads a block by hash
func (pm *PersistenceManager) LoadBlockByHash(hash string) (*Block, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	// Get block number from hash
	hashKey := BlockHashPrefix + hash
	numberData, err := pm.db.Get([]byte(hashKey), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get block number: %v", err)
	}

	number, err := strconv.ParseInt(string(numberData), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid block number: %v", err)
	}

	return pm.LoadBlock(number)
}

// LoadBlockchain loads the entire blockchain from database
func (pm *PersistenceManager) LoadBlockchain() ([]*Block, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	// Get chain height
	heightData, err := pm.db.Get([]byte(BlockHeightKey), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return []*Block{}, nil // Empty blockchain
		}
		return nil, fmt.Errorf("failed to get chain height: %v", err)
	}

	height, err := strconv.ParseInt(string(heightData), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid chain height: %v", err)
	}

	// Load all blocks
	blocks := make([]*Block, height+1)
	for i := int64(0); i <= height; i++ {
		block, err := pm.LoadBlock(i)
		if err != nil {
			return nil, fmt.Errorf("failed to load block %d: %v", i, err)
		}
		if block == nil {
			return nil, fmt.Errorf("missing block %d", i)
		}
		blocks[i] = block
	}

	return blocks, nil
}

// GetChainHeight returns the current blockchain height
func (pm *PersistenceManager) GetChainHeight() (int64, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	data, err := pm.db.Get([]byte(BlockHeightKey), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return -1, nil // No blocks
		}
		return -1, fmt.Errorf("failed to get chain height: %v", err)
	}

	return strconv.ParseInt(string(data), 10, 64)
}

// =============================================================================
// TRANSACTION STORAGE OPERATIONS
// =============================================================================

// saveTransactionBatch adds transaction to batch for saving
func (pm *PersistenceManager) saveTransactionBatch(tx *Transaction, blockNumber int64, batch map[string][]byte) error {
	// Serialize transaction
	txData, err := json.Marshal(tx)
	if err != nil {
		return fmt.Errorf("failed to serialize transaction: %v", err)
	}

	// Store transaction by ID
	txKey := TransactionPrefix + tx.ID
	batch[txKey] = txData

	// Store transaction-to-block mapping
	txBlockKey := TxBlockPrefix + tx.ID
	batch[txBlockKey] = []byte(strconv.FormatInt(blockNumber, 10))

	// Index transactions by address
	fromKey := AddressTxPrefix + tx.From + ":" + tx.ID
	batch[fromKey] = []byte(strconv.FormatInt(blockNumber, 10))

	toKey := AddressTxPrefix + tx.To + ":" + tx.ID
	batch[toKey] = []byte(strconv.FormatInt(blockNumber, 10))

	return nil
}

// LoadTransaction loads a transaction by ID
func (pm *PersistenceManager) LoadTransaction(txID string) (*Transaction, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	key := TransactionPrefix + txID
	data, err := pm.db.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to load transaction: %v", err)
	}

	var tx Transaction
	if err := json.Unmarshal(data, &tx); err != nil {
		return nil, fmt.Errorf("failed to deserialize transaction: %v", err)
	}

	return &tx, nil
}

// LoadTransactionsByAddress loads all transactions for an address
func (pm *PersistenceManager) LoadTransactionsByAddress(address string) ([]*Transaction, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	prefix := AddressTxPrefix + address + ":"
	iter := pm.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	defer iter.Release()

	var transactions []*Transaction
	for iter.Next() {
		// Extract transaction ID from key
		key := string(iter.Key())
		txID := key[len(prefix):]

		// Load transaction
		tx, err := pm.LoadTransaction(txID)
		if err != nil {
			continue // Skip invalid transactions
		}
		if tx != nil {
			transactions = append(transactions, tx)
		}
	}

	return transactions, iter.Error()
}

// =============================================================================
// BALANCE AND STATE STORAGE
// =============================================================================

// SaveBalance saves an address balance
func (pm *PersistenceManager) SaveBalance(address string, balance float64) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	key := BalancePrefix + address
	value := []byte(fmt.Sprintf("%.8f", balance))

	return pm.db.Put([]byte(key), value, nil)
}

// LoadBalance loads an address balance
func (pm *PersistenceManager) LoadBalance(address string) (float64, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	key := BalancePrefix + address
	data, err := pm.db.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return 0.0, nil // No balance found
		}
		return 0.0, fmt.Errorf("failed to load balance: %v", err)
	}

	return strconv.ParseFloat(string(data), 64)
}

// SaveNonce saves an address nonce
func (pm *PersistenceManager) SaveNonce(address string, nonce int64) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	key := NoncePrefix + address
	value := []byte(strconv.FormatInt(nonce, 10))

	return pm.db.Put([]byte(key), value, nil)
}

// LoadNonce loads an address nonce
func (pm *PersistenceManager) LoadNonce(address string) (int64, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	key := NoncePrefix + address
	data, err := pm.db.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return 0, nil // No nonce found
		}
		return 0, fmt.Errorf("failed to load nonce: %v", err)
	}

	return strconv.ParseInt(string(data), 10, 64)
}

// SaveBalancesAndNonces saves all balances and nonces in batch
func (pm *PersistenceManager) SaveBalancesAndNonces(balances map[string]float64, nonces map[string]int64) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	batch := make(map[string][]byte)

	// Add balances to batch
	for address, balance := range balances {
		key := BalancePrefix + address
		value := []byte(fmt.Sprintf("%.8f", balance))
		batch[key] = value
	}

	// Add nonces to batch
	for address, nonce := range nonces {
		key := NoncePrefix + address
		value := []byte(strconv.FormatInt(nonce, 10))
		batch[key] = value
	}

	return pm.executeBatch(batch)
}

// =============================================================================
// MEMPOOL PERSISTENCE (OPTIONAL)
// =============================================================================

// SaveMempoolTransaction saves a mempool transaction
func (pm *PersistenceManager) SaveMempoolTransaction(tx *Transaction) error {
	if !pm.enableMempool {
		return nil // Mempool persistence disabled
	}

	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	txData, err := json.Marshal(tx)
	if err != nil {
		return fmt.Errorf("failed to serialize mempool transaction: %v", err)
	}

	key := MempoolTxPrefix + tx.ID
	return pm.db.Put([]byte(key), txData, nil)
}

// LoadMempoolTransactions loads all mempool transactions
func (pm *PersistenceManager) LoadMempoolTransactions() ([]*Transaction, error) {
	if !pm.enableMempool {
		return []*Transaction{}, nil
	}

	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	prefix := MempoolTxPrefix
	iter := pm.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	defer iter.Release()

	var transactions []*Transaction
	for iter.Next() {
		var tx Transaction
		if err := json.Unmarshal(iter.Value(), &tx); err != nil {
			continue // Skip invalid transactions
		}
		transactions = append(transactions, &tx)
	}

	return transactions, iter.Error()
}

// RemoveMempoolTransaction removes a transaction from mempool storage
func (pm *PersistenceManager) RemoveMempoolTransaction(txID string) error {
	if !pm.enableMempool {
		return nil
	}

	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	key := MempoolTxPrefix + txID
	return pm.db.Delete([]byte(key), nil)
}

// ClearMempool clears all mempool transactions
func (pm *PersistenceManager) ClearMempool() error {
	if !pm.enableMempool {
		return nil
	}

	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	prefix := MempoolTxPrefix
	iter := pm.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	defer iter.Release()

	batch := make(map[string][]byte)
	for iter.Next() {
		// Mark for deletion by setting nil value
		batch[string(iter.Key())] = nil
	}

	return pm.executeBatch(batch)
}

// =============================================================================
// STATISTICS AND METADATA
// =============================================================================

// updateStatsBatch updates blockchain statistics in batch
func (pm *PersistenceManager) updateStatsBatch(block *Block, batch map[string][]byte) {
	// Update total transaction count
	// totalTxKey := StatsPrefix + TotalTxKey  // ✅ FIXED: Commented out unused variable
	// This would need current value + new transactions
	// Simplified for this implementation
	
	// Update total value
	totalValue := 0.0
	for _, tx := range block.Transactions {
		totalValue += tx.Amount
	}
	
	totalValueKey := StatsPrefix + TotalValueKey
	batch[totalValueKey] = []byte(fmt.Sprintf("%.8f", totalValue))
	
	// Update last update timestamp
	batch[MetadataPrefix + LastUpdateKey] = []byte(strconv.FormatInt(block.Timestamp, 10))
}

// SaveMetadata saves metadata key-value pair
func (pm *PersistenceManager) SaveMetadata(key, value string) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	fullKey := MetadataPrefix + key
	return pm.db.Put([]byte(fullKey), []byte(value), nil)
}

// LoadMetadata loads metadata by key
func (pm *PersistenceManager) LoadMetadata(key string) (string, error) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	fullKey := MetadataPrefix + key
	data, err := pm.db.Get([]byte(fullKey), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return "", nil
		}
		return "", fmt.Errorf("failed to load metadata: %v", err)
	}

	return string(data), nil
}

// =============================================================================
// BATCH OPERATIONS
// =============================================================================

// executeBatch executes a batch write operation
func (pm *PersistenceManager) executeBatch(batch map[string][]byte) error {
	if len(batch) == 0 {
		return nil
	}

	// Create LevelDB batch
	levelBatch := new(leveldb.Batch)

	for key, value := range batch {
		if value == nil {
			// Delete operation
			levelBatch.Delete([]byte(key))
		} else {
			// Put operation
			levelBatch.Put([]byte(key), value)
		}
	}

	// Execute batch
	return pm.db.Write(levelBatch, nil)
}

// flushBatch flushes the write buffer
func (pm *PersistenceManager) flushBatch() error {
	if len(pm.writeBuffer) == 0 {
		return nil
	}

	err := pm.executeBatch(pm.writeBuffer)
	pm.writeBuffer = make(map[string][]byte)
	return err
}

// =============================================================================
// DATABASE MAINTENANCE
// =============================================================================

// CompactDatabase compacts the database to optimize storage
func (pm *PersistenceManager) CompactDatabase() error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	return pm.db.CompactRange(util.Range{})
}

// GetDatabaseStats returns database statistics
func (pm *PersistenceManager) GetDatabaseStats() map[string]interface{} {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	stats := make(map[string]interface{})
	
	// Count different types of records
	blockCount := 0
	txCount := 0
	balanceCount := 0
	mempoolCount := 0

	iter := pm.db.NewIterator(nil, nil)
	defer iter.Release()

	for iter.Next() {
		key := string(iter.Key())
		if len(key) > 6 {
			prefix := key[:6]
			switch prefix {
			case BlockPrefix[:6]:
				blockCount++
			case TransactionPrefix[:3]:
				txCount++
			case BalancePrefix[:8]:
				balanceCount++
			case MempoolTxPrefix[:8]:
				mempoolCount++
			}
		}
	}

	stats["blocks"] = blockCount
	stats["transactions"] = txCount
	stats["addresses"] = balanceCount
	stats["mempool_transactions"] = mempoolCount
	stats["data_directory"] = pm.dataDir

	return stats
}

// =============================================================================
// DEFAULT CONFIGURATION
// =============================================================================

// DefaultPersistenceConfig returns default persistence configuration
func DefaultPersistenceConfig() *PersistenceConfig {
	return &PersistenceConfig{
		DataDir:       "./data",
		EnableMempool: false, // Mempool persistence optional
		BatchSize:     1000,
		AutoSync:      true,
	}
}