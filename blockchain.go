// =============================================================================
// ENHANCED BLOCKCHAIN.GO - WITH TREASURY, SECURITY & AUDIT LOGGING
// =============================================================================

package main

import (
	"fmt"
	"sync"
	"time"
	"strings"
	"log"
)

// =============================================================================
// SECURITY & PERMISSIONS
// =============================================================================

// Permission types for role-based access control
type Permission string

const (
	PermMint     Permission = "MINT"
	PermBurn     Permission = "BURN"
	PermAdmin    Permission = "ADMIN"
	PermTransfer Permission = "TRANSFER"
)

// TreasuryConfig holds treasury settings
type TreasuryConfig struct {
	TreasuryAddress string             `json:"treasury_address"`
	InitialSupply   float64            `json:"initial_supply"`
	MaxSupply       float64            `json:"max_supply"`
	MintAuthority   map[string]bool    `json:"-"` // Authorized minters
	AdminAuthority  map[string]bool    `json:"-"` // Admins who can add/remove minters
}

// AuditLog represents an audit trail entry
type AuditLog struct {
	Timestamp   time.Time  `json:"timestamp"`
	Action      string     `json:"action"`
	Initiator   string     `json:"initiator"`
	Target      string     `json:"target"`
	Amount      float64    `json:"amount,omitempty"`
	Success     bool       `json:"success"`
	FailReason  string     `json:"fail_reason,omitempty"`
	TxID        string     `json:"tx_id,omitempty"`
}

// Updated Blockchain struct with treasury and security
type Blockchain struct {
	Blocks             []*Block                `json:"blocks"`
	Difficulty         int                     `json:"difficulty"`
	BlockReward        float64                 `json:"block_reward"`
	TargetBlockTime    int64                   `json:"target_block_time"`
	MaxBlockSize       int                     `json:"max_block_size"`
	TransactionPool    *TransactionPool        `json:"-"`
	balanceCache       map[string]float64      `json:"-"`
	nonceCache         map[string]int64        `json:"-"`
	mutex              sync.RWMutex            `json:"-"`
	cacheValid         bool                    `json:"-"`
	persistenceManager *PersistenceManager     `json:"-"`
	
	// NEW: Treasury and security features
	Treasury           *TreasuryConfig         `json:"-"`
	auditLog           []*AuditLog             `json:"-"`
	totalMinted        float64                 `json:"-"`
	totalBurned        float64                 `json:"-"`
	auditMutex         sync.RWMutex            `json:"-"`
}

// =============================================================================
// INITIALIZATION
// =============================================================================

// NewBlockchain creates a new blockchain without persistence
func NewBlockchain(genesisAddress string) *Blockchain {
	blockchain := &Blockchain{
		Blocks:          []*Block{},
		Difficulty:      4,
		BlockReward:     50.0,
		TargetBlockTime: 30,
		MaxBlockSize:    1048576,
		TransactionPool: NewTransactionPool(),
		balanceCache:    make(map[string]float64),
		nonceCache:      make(map[string]int64),
		cacheValid:      false,
		auditLog:        make([]*AuditLog, 0),
		totalMinted:     0,
		totalBurned:     0,
	}
	
	// Initialize default treasury
	blockchain.initializeDefaultTreasury()
	
	// Create genesis block
	genesisBlock := CreateGenesisBlock(genesisAddress)
	genesisBlock.Hash = genesisBlock.CalculateHash()
	blockchain.Blocks = append(blockchain.Blocks, genesisBlock)
	blockchain.rebuildCache()
	
	return blockchain
}

// NewBlockchainWithPersistence creates blockchain with database
func NewBlockchainWithPersistence(genesisAddress string, persistenceManager *PersistenceManager) *Blockchain {
	blockchain := &Blockchain{
		Blocks:             []*Block{},
		Difficulty:         4,
		BlockReward:        50.0,
		TargetBlockTime:    30,
		MaxBlockSize:       1048576,
		TransactionPool:    NewTransactionPool(),
		balanceCache:       make(map[string]float64),
		nonceCache:         make(map[string]int64),
		cacheValid:         false,
		persistenceManager: persistenceManager,
		auditLog:           make([]*AuditLog, 0),
		totalMinted:        0,
		totalBurned:        0,
	}
	
	// Initialize treasury
	blockchain.initializeDefaultTreasury()
	
	// Try to load existing blockchain from database
	existingBlocks, err := persistenceManager.LoadBlockchain()
	if err != nil {
		fmt.Printf("Error loading blockchain from database: %v\n", err)
	}
	
	if len(existingBlocks) > 0 {
		blockchain.Blocks = existingBlocks
		fmt.Printf("✅ Loaded %d blocks from database\n", len(existingBlocks))
		blockchain.rebuildCacheFromDatabase()
	} else {
		genesisBlock := CreateGenesisBlock(genesisAddress)
		genesisBlock.Hash = genesisBlock.CalculateHash()
		blockchain.Blocks = append(blockchain.Blocks, genesisBlock)
		
		if err := persistenceManager.SaveBlock(genesisBlock); err != nil {
			fmt.Printf("Error saving genesis block: %v\n", err)
		}
		
		blockchain.rebuildCache()
		fmt.Printf("✅ Created new blockchain with genesis block\n")
	}
	
	return blockchain
}

// initializeDefaultTreasury sets up default treasury configuration
func (bc *Blockchain) initializeDefaultTreasury() {
	bc.Treasury = &TreasuryConfig{
		TreasuryAddress: "DTTreasury000000000000000000",
		InitialSupply:   1000000000.0, // 1 billion DINARI
		MaxSupply:       10000000000.0, // 10 billion max
		MintAuthority:   make(map[string]bool),
		AdminAuthority:  make(map[string]bool),
	}
	
	// Set initial treasury balance
	bc.balanceCache[bc.Treasury.TreasuryAddress] = bc.Treasury.InitialSupply
	
	log.Printf("✅ Treasury initialized: %s with %.0f DINARI", 
		bc.Treasury.TreasuryAddress, bc.Treasury.InitialSupply)
}

// =============================================================================
// TREASURY MINT FUNCTION
// =============================================================================

// MintTokens mints tokens from treasury to user wallet (for purchases)
func (bc *Blockchain) MintTokens(toAddress string, amount float64, authorizedBy string, reason string) (*Transaction, error) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()
	
	// Input validation
	if err := bc.validateMintRequest(toAddress, amount, authorizedBy); err != nil {
		bc.logAuditEvent("MINT_REJECTED", authorizedBy, toAddress, amount, false, err.Error(), "")
		return nil, err
	}
	
	// Check treasury has sufficient balance
	treasuryBalance := bc.balanceCache[bc.Treasury.TreasuryAddress]
	if treasuryBalance < amount {
		err := fmt.Errorf("insufficient treasury balance: have %.8f, need %.8f", treasuryBalance, amount)
		bc.logAuditEvent("MINT_REJECTED", authorizedBy, toAddress, amount, false, err.Error(), "")
		return nil, err
	}
	
	// Check max supply limit
	currentSupply := bc.getTotalSupply()
	if currentSupply + amount > bc.Treasury.MaxSupply {
		err := fmt.Errorf("minting %.8f would exceed max supply %.0f", amount, bc.Treasury.MaxSupply)
		bc.logAuditEvent("MINT_REJECTED", authorizedBy, toAddress, amount, false, err.Error(), "")
		return nil, err
	}
	
	// Create mint transaction
	tx := &Transaction{
		From:      bc.Treasury.TreasuryAddress,
		To:        toAddress,
		Amount:    amount,
		Fee:       0.0, // Treasury mints have no fee
		Nonce:     bc.nonceCache[bc.Treasury.TreasuryAddress],
		Timestamp: time.Now().Unix(),
	}
	tx.ID = tx.generateID()
	tx.Signature = fmt.Sprintf("MINT_AUTH_%s_%s", authorizedBy, reason)
	
	// Execute transfer
	bc.balanceCache[bc.Treasury.TreasuryAddress] -= amount
	bc.balanceCache[toAddress] += amount
	bc.nonceCache[bc.Treasury.TreasuryAddress]++
	bc.totalMinted += amount
	
	// Save to database
	if bc.persistenceManager != nil {
		bc.saveBalancesToDatabase()
	}
	
	// Log successful mint
	bc.logAuditEvent("MINT_SUCCESS", authorizedBy, toAddress, amount, true, "", tx.ID)
	
	log.Printf("💰 Minted %.8f DINARI to %s (authorized by %s, reason: %s)", 
		amount, toAddress, authorizedBy, reason)
	
	return tx, nil
}

// validateMintRequest validates mint operation parameters
func (bc *Blockchain) validateMintRequest(toAddress string, amount float64, authorizedBy string) error {
	// Check authorization
	if !bc.isMintAuthorized(authorizedBy) {
		return fmt.Errorf("address %s is not authorized to mint tokens", authorizedBy)
	}
	
	// Validate recipient address
	if !ValidateAddress(toAddress) {
		return fmt.Errorf("invalid recipient address: %s", toAddress)
	}
	
	// Prevent minting to treasury itself
	if toAddress == bc.Treasury.TreasuryAddress {
		return fmt.Errorf("cannot mint to treasury address")
	}
	
	// Validate amount
	if amount <= 0 {
		return fmt.Errorf("mint amount must be positive, got: %.8f", amount)
	}
	
	// Prevent overflow attacks
	if amount > 1e15 {
		return fmt.Errorf("mint amount too large: %.8f", amount)
	}
	
	return nil
}

// =============================================================================
// BURN FUNCTION (Optional - for deflationary mechanics)
// =============================================================================

// BurnTokens burns tokens by sending them to a null address
func (bc *Blockchain) BurnTokens(fromAddress string, amount float64, authorizedBy string, reason string) error {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()
	
	// Validate authorization
	if !bc.isBurnAuthorized(authorizedBy) {
		err := fmt.Errorf("address %s is not authorized to burn tokens", authorizedBy)
		bc.logAuditEvent("BURN_REJECTED", authorizedBy, fromAddress, amount, false, err.Error(), "")
		return err
	}
	
	// Check balance
	balance := bc.balanceCache[fromAddress]
	if balance < amount {
		err := fmt.Errorf("insufficient balance: have %.8f, need %.8f", balance, amount)
		bc.logAuditEvent("BURN_REJECTED", authorizedBy, fromAddress, amount, false, err.Error(), "")
		return err
	}
	
	// Execute burn
	bc.balanceCache[fromAddress] -= amount
	bc.totalBurned += amount
	
	// Save to database
	if bc.persistenceManager != nil {
		bc.saveBalancesToDatabase()
	}
	
	bc.logAuditEvent("BURN_SUCCESS", authorizedBy, fromAddress, amount, true, "", "")
	log.Printf("🔥 Burned %.8f DINARI from %s (authorized by %s, reason: %s)", 
		amount, fromAddress, authorizedBy, reason)
	
	return nil
}

// =============================================================================
// PERMISSION MANAGEMENT
// =============================================================================

// AddMintAuthority grants mint permission to an address (admin only)
func (bc *Blockchain) AddMintAuthority(address string, adminAddress string) error {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()
	
	if !bc.isAdmin(adminAddress) {
		return fmt.Errorf("only admins can add mint authority")
	}
	
	bc.Treasury.MintAuthority[address] = true
	bc.logAuditEvent("MINT_AUTH_ADDED", adminAddress, address, 0, true, "", "")
	log.Printf("✅ Mint authority granted to %s by admin %s", address, adminAddress)
	
	return nil
}

// RemoveMintAuthority revokes mint permission (admin only)
func (bc *Blockchain) RemoveMintAuthority(address string, adminAddress string) error {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()
	
	if !bc.isAdmin(adminAddress) {
		return fmt.Errorf("only admins can remove mint authority")
	}
	
	delete(bc.Treasury.MintAuthority, address)
	bc.logAuditEvent("MINT_AUTH_REMOVED", adminAddress, address, 0, true, "", "")
	log.Printf("⚠️  Mint authority revoked from %s by admin %s", address, adminAddress)
	
	return nil
}

// AddAdmin grants admin privileges (existing admin only)
func (bc *Blockchain) AddAdmin(address string, existingAdmin string) error {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()
	
	if !bc.isAdmin(existingAdmin) {
		return fmt.Errorf("only admins can add other admins")
	}
	
	bc.Treasury.AdminAuthority[address] = true
	bc.logAuditEvent("ADMIN_ADDED", existingAdmin, address, 0, true, "", "")
	log.Printf("✅ Admin privileges granted to %s by %s", address, existingAdmin)
	
	return nil
}

// Permission check functions
func (bc *Blockchain) isMintAuthorized(address string) bool {
	return bc.Treasury.MintAuthority[address] || bc.isAdmin(address)
}

func (bc *Blockchain) isBurnAuthorized(address string) bool {
	return bc.Treasury.MintAuthority[address] || bc.isAdmin(address)
}

func (bc *Blockchain) isAdmin(address string) bool {
	return bc.Treasury.AdminAuthority[address]
}

// =============================================================================
// AUDIT LOGGING
// =============================================================================

// logAuditEvent records security-sensitive operations
func (bc *Blockchain) logAuditEvent(action, initiator, target string, amount float64, success bool, failReason, txID string) {
	bc.auditMutex.Lock()
	defer bc.auditMutex.Unlock()
	
	event := &AuditLog{
		Timestamp:  time.Now(),
		Action:     action,
		Initiator:  initiator,
		Target:     target,
		Amount:     amount,
		Success:    success,
		FailReason: failReason,
		TxID:       txID,
	}
	
	bc.auditLog = append(bc.auditLog, event)
	
	// Keep only last 10000 audit events in memory
	if len(bc.auditLog) > 10000 {
		bc.auditLog = bc.auditLog[len(bc.auditLog)-10000:]
	}
	
	// Log to file/database in production
	if bc.persistenceManager != nil {
		// TODO: Save to audit log table in database
	}
}

// GetAuditLog returns recent audit events (admin only)
func (bc *Blockchain) GetAuditLog(adminAddress string, limit int) ([]*AuditLog, error) {
	if !bc.isAdmin(adminAddress) {
		return nil, fmt.Errorf("only admins can view audit logs")
	}
	
	bc.auditMutex.RLock()
	defer bc.auditMutex.RUnlock()
	
	if limit <= 0 || limit > len(bc.auditLog) {
		limit = len(bc.auditLog)
	}
	
	// Return most recent entries
	start := len(bc.auditLog) - limit
	return bc.auditLog[start:], nil
}

// =============================================================================
// ENHANCED TRANSACTION VALIDATION
// =============================================================================

// AddTransaction with enhanced security checks
func (bc *Blockchain) AddTransaction(tx *Transaction) error {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()
	
	// Enhanced validation
	if err := bc.validateTransactionSecurity(tx); err != nil {
		bc.logAuditEvent("TX_REJECTED", tx.From, tx.To, tx.Amount, false, err.Error(), tx.ID)
		return fmt.Errorf("security validation failed: %v", err)
	}
	
	// Standard validation
	if err := tx.IsValidTransaction(); err != nil {
		return fmt.Errorf("invalid transaction: %v", err)
	}
	
	// Ensure cache is valid
	if !bc.cacheValid {
		bc.rebuildCache()
	}
	
	// Check sufficient balance
	senderBalance := bc.balanceCache[tx.From]
	requiredAmount := tx.Amount + tx.Fee
	
	if senderBalance < requiredAmount {
		return fmt.Errorf("insufficient balance: have %.8f, need %.8f", 
			senderBalance, requiredAmount)
	}
	
	// Check nonce
	expectedNonce := bc.nonceCache[tx.From]
	if tx.Nonce != expectedNonce {
		return fmt.Errorf("invalid nonce: expected %d, got %d", expectedNonce, tx.Nonce)
	}
	
	// Add to pool
	err := bc.TransactionPool.AddTransaction(tx)
	if err != nil {
		return err
	}
	
	// Save to mempool database
	if bc.persistenceManager != nil {
		if err := bc.persistenceManager.SaveMempoolTransaction(tx); err != nil {
			fmt.Printf("Warning: Failed to save transaction to mempool database: %v\n", err)
		}
	}
	
	bc.logAuditEvent("TX_ACCEPTED", tx.From, tx.To, tx.Amount, true, "", tx.ID)
	return nil
}

// validateTransactionSecurity performs security-specific validation
func (bc *Blockchain) validateTransactionSecurity(tx *Transaction) error {
	// Prevent overflow attacks
	if tx.Amount > 1e15 || tx.Amount < 0 {
		return fmt.Errorf("amount out of valid range: %.8f", tx.Amount)
	}
	
	if tx.Fee > 1e10 || tx.Fee < 0 {
		return fmt.Errorf("fee out of valid range: %.8f", tx.Fee)
	}
	
	// Prevent replay attacks (check timestamp)
	now := time.Now().Unix()
	if tx.Timestamp < now - 300 { // 5 minutes old
		return fmt.Errorf("transaction too old (possible replay attack)")
	}
	
	if tx.Timestamp > now + 300 { // 5 minutes in future
		return fmt.Errorf("transaction timestamp too far in future")
	}
	
	// Validate addresses
	if !ValidateAddress(tx.From) || !ValidateAddress(tx.To) {
		return fmt.Errorf("invalid address format")
	}
	
	// Prevent self-transfers (potential attack vector)
	if tx.From == tx.To {
		return fmt.Errorf("self-transfers not allowed")
	}
	
	return nil
}

// =============================================================================
// TREASURY STATISTICS
// =============================================================================

// GetTreasuryStats returns treasury information
func (bc *Blockchain) GetTreasuryStats() map[string]interface{} {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()
	
	treasuryBalance := bc.balanceCache[bc.Treasury.TreasuryAddress]
	totalSupply := bc.getTotalSupply()
	circulatingSupply := totalSupply - treasuryBalance
	
	return map[string]interface{}{
		"treasury_address":    bc.Treasury.TreasuryAddress,
		"treasury_balance":    treasuryBalance,
		"initial_supply":      bc.Treasury.InitialSupply,
		"max_supply":          bc.Treasury.MaxSupply,
		"total_supply":        totalSupply,
		"circulating_supply":  circulatingSupply,
		"total_minted":        bc.totalMinted,
		"total_burned":        bc.totalBurned,
		"supply_utilization":  (totalSupply / bc.Treasury.MaxSupply) * 100,
	}
}

// getTotalSupply calculates total supply in circulation
func (bc *Blockchain) getTotalSupply() float64 {
	total := 0.0
	for _, balance := range bc.balanceCache {
		total += balance
	}
	return total
}

// =============================================================================
// EXISTING FUNCTIONS (unchanged)
// =============================================================================

func (bc *Blockchain) AddBlock(block *Block) error {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()
	
	var previousBlock *Block
	if len(bc.Blocks) > 0 {
		previousBlock = bc.Blocks[len(bc.Blocks)-1]
	}
	
	if err := block.IsValidBlock(previousBlock); err != nil {
		return fmt.Errorf("invalid block: %v", err)
	}
	
	if block.Size > bc.MaxBlockSize {
		return fmt.Errorf("block size %d exceeds maximum %d", block.Size, bc.MaxBlockSize)
	}
	
	if err := bc.validateBlockTransactions(block); err != nil {
		return fmt.Errorf("block transactions invalid: %v", err)
	}
	
	bc.Blocks = append(bc.Blocks, block)
	
	if bc.persistenceManager != nil {
		if err := bc.persistenceManager.SaveBlock(block); err != nil {
			bc.Blocks = bc.Blocks[:len(bc.Blocks)-1]
			return fmt.Errorf("failed to save block to database: %v", err)
		}
	}
	
	txIDs := make([]string, len(block.Transactions))
	for i, tx := range block.Transactions {
		txIDs[i] = tx.ID
		if bc.persistenceManager != nil {
			bc.persistenceManager.RemoveMempoolTransaction(tx.ID)
		}
	}
	bc.TransactionPool.RemoveTransactions(txIDs)
	
	bc.rebuildCache()
	if bc.persistenceManager != nil {
		bc.saveBalancesToDatabase()
	}
	
	bc.adjustDifficulty()
	
	fmt.Printf("✅ Block %d saved to database (Hash: %s)\n", block.Number, block.Hash)
	return nil
}

func (bc *Blockchain) rebuildCacheFromDatabase() {
	bc.balanceCache = make(map[string]float64)
	bc.nonceCache = make(map[string]int64)
	
	addressMap := make(map[string]bool)
	
	for _, block := range bc.Blocks {
		if block.Miner != "" {
			addressMap[block.Miner] = true
		}
		for _, tx := range block.Transactions {
			addressMap[tx.From] = true
			addressMap[tx.To] = true
		}
	}
	
	for address := range addressMap {
		if bc.persistenceManager != nil {
			if balance, err := bc.persistenceManager.LoadBalance(address); err == nil {
				bc.balanceCache[address] = balance
			}
			
			if nonce, err := bc.persistenceManager.LoadNonce(address); err == nil {
				bc.nonceCache[address] = nonce
			}
		}
	}
	
	if len(bc.balanceCache) == 0 {
		bc.rebuildCache()
		bc.saveBalancesToDatabase()
	}
	
	bc.cacheValid = true
}

func (bc *Blockchain) saveBalancesToDatabase() {
	if bc.persistenceManager == nil {
		return
	}
	
	err := bc.persistenceManager.SaveBalancesAndNonces(bc.balanceCache, bc.nonceCache)
	if err != nil {
		fmt.Printf("Error saving balances to database: %v\n", err)
	}
}

func (bc *Blockchain) rebuildCache() {
	bc.balanceCache = make(map[string]float64)
	bc.nonceCache = make(map[string]int64)
	
	for _, block := range bc.Blocks {
		if block.Miner != "" {
			bc.balanceCache[block.Miner] += block.Reward + block.TotalFees
		}
		
		for _, tx := range block.Transactions {
			bc.balanceCache[tx.From] -= (tx.Amount + tx.Fee)
			bc.balanceCache[tx.To] += tx.Amount
			bc.nonceCache[tx.From]++
		}
	}
	
	bc.cacheValid = true
}

func (bc *Blockchain) validateBlockTransactions(block *Block) error {
	for _, tx := range block.Transactions {
		if err := tx.IsValidTransaction(); err != nil {
			return err
		}
	}
	return nil
}

func (bc *Blockchain) adjustDifficulty() {
	// Placeholder for difficulty adjustment
}

func (bc *Blockchain) GetLatestBlock() *Block {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()
	
	if len(bc.Blocks) == 0 {
		return nil
	}
	return bc.Blocks[len(bc.Blocks)-1]
}

func (bc *Blockchain) GetHeight() int {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()
	return len(bc.Blocks)
}

func (bc *Blockchain) GetChainLength() int {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()
	return len(bc.Blocks)
}

func (bc *Blockchain) GetBalance(address string) float64 {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()
	
	if !bc.cacheValid {
		bc.rebuildCache()
	}
	
	return bc.balanceCache[address]
}

func (bc *Blockchain) GetNonce(address string) int64 {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()
	
	if !bc.cacheValid {
		bc.rebuildCache()
	}
	
	return bc.nonceCache[address]
}

func (bc *Blockchain) GetBlockByNumber(blockNumber int) *Block {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()

	if blockNumber < 0 || blockNumber >= len(bc.Blocks) {
		return nil
	}
	return bc.Blocks[blockNumber]
}

func (bc *Blockchain) GetBlockByHash(hash string) *Block {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()

	for _, block := range bc.Blocks {
		if block.Hash == hash {
			return block
		}
	}
	return nil
}

func (bc *Blockchain) GetAllBalances() map[string]float64 {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()

	if !bc.cacheValid {
		bc.rebuildCache()
	}

	balancesCopy := make(map[string]float64)
	for address, balance := range bc.balanceCache {
		balancesCopy[address] = balance
	}
	return balancesCopy
}

func (bc *Blockchain) MineBlock(transactions []*Transaction, minerAddress string) (*Block, error) {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	var prevBlock *Block
	if len(bc.Blocks) > 0 {
		prevBlock = bc.Blocks[len(bc.Blocks)-1]
	} else {
		prevBlock = &Block{
			Hash: "0000000000000000000000000000000000000000000000000000000000000000",
		}
	}

	totalFees := 0.0
	for _, tx := range transactions {
		totalFees += tx.Fee
	}

	newBlock := &Block{
		Number:       int64(len(bc.Blocks)),
		Timestamp:    time.Now().Unix(),
		Transactions: transactions,
		PreviousHash: prevBlock.Hash,
		Nonce:        0,
		Miner:        minerAddress,
		Reward:       bc.BlockReward,
		TotalFees:    totalFees,
	}

	newBlock.Size = newBlock.calculateSize()

	target := strings.Repeat("0", bc.Difficulty)
	
	for {
		newBlock.Hash = newBlock.CalculateHash()
		if strings.HasPrefix(newBlock.Hash, target) {
			break
		}
		newBlock.Nonce++
	}

	if err := bc.AddBlock(newBlock); err != nil {
		return nil, fmt.Errorf("failed to add mined block: %v", err)
	}

	fmt.Printf("⛏️  Block %d mined successfully by %s (nonce: %d)\n", 
		newBlock.Number, minerAddress, newBlock.Nonce)
	
	return newBlock, nil
}

func (bc *Blockchain) GetBlockRange(start, end int64) []*Block {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()

	if start < 0 {
		start = 0
	}
	if end >= int64(len(bc.Blocks)) {
		end = int64(len(bc.Blocks)) - 1
	}
	if start > end {
		return []*Block{}
	}

	result := make([]*Block, end-start+1)
	copy(result, bc.Blocks[start:end+1])
	return result
}

func (bc *Blockchain) ReplaceChain(newChain []*Block) error {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	if len(newChain) <= len(bc.Blocks) {
		return fmt.Errorf("new chain is not longer than current chain")
	}

	if len(newChain) == 0 {
		return fmt.Errorf("new chain is empty")
	}

	bc.Blocks = newChain
	bc.rebuildCache()

	return nil
}

func (bc *Blockchain) GetStats() *ChainStats {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()

	totalTransactions := 0
	totalValue := 0.0
	totalFees := 0.0

	for _, block := range bc.Blocks {
		totalTransactions += len(block.Transactions)
		for _, tx := range block.Transactions {
			totalValue += tx.Amount
			totalFees += tx.Fee
		}
	}

	return &ChainStats{
		BlockCount:        len(bc.Blocks),
		TotalTransactions: totalTransactions,
		TotalValue:        totalValue,
		TotalFees:         totalFees,
		Difficulty:        bc.Difficulty,
		BlockReward:       bc.BlockReward,
	}
}

func (bc *Blockchain) GetTransactionByID(txID string) *Transaction {
	bc.mutex.RLock()
	defer bc.mutex.RUnlock()

	for _, block := range bc.Blocks {
		for _, tx := range block.Transactions {
			if tx.ID == txID {
				return tx
			}
		}
	}

	return bc.TransactionPool.GetTransaction(txID)
}

type ChainStats struct {
	BlockCount        int     `json:"block_count"`
	TotalTransactions int     `json:"total_transactions"`
	TotalValue        float64 `json:"total_value"`
	TotalFees         float64 `json:"total_fees"`
	Difficulty        int     `json:"difficulty"`
	BlockReward       float64 `json:"block_reward"`
}