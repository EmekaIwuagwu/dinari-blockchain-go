package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"      // Updated import path
	"github.com/btcsuite/btcd/btcec/v2/ecdsa" // For signature parsing
)

// =============================================================================
// TRANSACTION STRUCTURE & OPERATIONS (UPDATED FOR MODERN LIBRARIES)
// =============================================================================

// Transaction represents a blockchain transaction for cross-border payments
type Transaction struct {
	ID        string  `json:"id"`        // DTx-prefixed unique identifier
	From      string  `json:"from"`      // Sender's DT address
	To        string  `json:"to"`        // Recipient's DT address  
	Amount    float64 `json:"amount"`    // Amount to transfer
	Signature string  `json:"signature"` // Digital signature (hex encoded)
	Timestamp int64   `json:"timestamp"` // Unix timestamp when created
	Fee       float64 `json:"fee"`       // Transaction fee (for miners)
	Nonce     int64   `json:"nonce"`     // Sender's transaction nonce
}

// TransactionPool manages pending transactions with fee prioritization
type TransactionPool struct {
	transactions map[string]*Transaction
	byFee        []*Transaction
	mutex        sync.RWMutex
}

// NewTransactionPool creates a new transaction pool
func NewTransactionPool() *TransactionPool {
	return &TransactionPool{
		transactions: make(map[string]*Transaction),
		byFee:        make([]*Transaction, 0),
	}
}

// CreateTransaction creates a new unsigned transaction
func CreateTransaction(from, to string, amount, fee float64, nonce int64) (*Transaction, error) {
	if !ValidateAddress(from) {
		return nil, fmt.Errorf("invalid sender address: %s", from)
	}
	if !ValidateAddress(to) {
		return nil, fmt.Errorf("invalid recipient address: %s", to)
	}
	
	if amount <= 0 {
		return nil, fmt.Errorf("amount must be positive, got: %.8f", amount)
	}
	if fee < 0 {
		return nil, fmt.Errorf("fee cannot be negative, got: %.8f", fee)
	}
	if nonce < 0 {
		return nil, fmt.Errorf("nonce cannot be negative, got: %d", nonce)
	}

	tx := &Transaction{
		From:      from,
		To:        to,
		Amount:    amount,
		Fee:       fee,
		Nonce:     nonce,
		Timestamp: time.Now().Unix(),
	}
	
	tx.ID = tx.generateID()
	return tx, nil
}

// generateID creates a unique transaction ID with DTx prefix
func (tx *Transaction) generateID() string {
	data := fmt.Sprintf("%s%s%.8f%.8f%d%d", 
		tx.From, tx.To, tx.Amount, tx.Fee, tx.Nonce, tx.Timestamp)
	hash := sha256.Sum256([]byte(data))
	return "DTx" + hex.EncodeToString(hash[:16])
}

// SignTransaction signs a transaction with the sender's private key
func (tx *Transaction) SignTransaction(privateKey *btcec.PrivateKey) error {
	message := tx.getSigningMessage()
	messageHash := sha256.Sum256([]byte(message))
	
	// Updated for modern btcec library
	signature := ecdsa.Sign(privateKey, messageHash[:])
	
	tx.Signature = hex.EncodeToString(signature.Serialize())
	return nil
}

// getSigningMessage creates the message that gets signed
func (tx *Transaction) getSigningMessage() string {
	return fmt.Sprintf("%s%s%s%.8f%.8f%d%d", 
		tx.ID, tx.From, tx.To, tx.Amount, tx.Fee, tx.Nonce, tx.Timestamp)
}

// VerifyTransaction verifies a transaction signature against a public key
func (tx *Transaction) VerifyTransaction(publicKey *btcec.PublicKey) bool {
	if tx.Signature == "" {
		return false
	}
	
	message := tx.getSigningMessage()
	messageHash := sha256.Sum256([]byte(message))
	
	sigBytes, err := hex.DecodeString(tx.Signature)
	if err != nil {
		return false
	}
	
	// Updated for modern btcec library
	signature, err := ecdsa.ParseSignature(sigBytes)
	if err != nil {
		return false
	}
	
	return signature.Verify(messageHash[:], publicKey)
}

// IsValidTransaction performs comprehensive transaction validation
func (tx *Transaction) IsValidTransaction() error {
	if tx.ID == "" {
		return fmt.Errorf("transaction ID is required")
	}
	if !strings.HasPrefix(tx.ID, "DTx") {
		return fmt.Errorf("transaction ID must start with 'DTx'")
	}
	
	if !ValidateAddress(tx.From) {
		return fmt.Errorf("invalid sender address")
	}
	if !ValidateAddress(tx.To) {
		return fmt.Errorf("invalid recipient address")
	}
	
	if tx.Amount <= 0 {
		return fmt.Errorf("amount must be positive")
	}
	if tx.Fee < 0 {
		return fmt.Errorf("fee cannot be negative")
	}
	if tx.Nonce < 0 {
		return fmt.Errorf("nonce cannot be negative")
	}
	
	if tx.Signature == "" {
		return fmt.Errorf("transaction must be signed")
	}
	
	now := time.Now().Unix()
	if tx.Timestamp > now+300 {
		return fmt.Errorf("transaction timestamp too far in future")
	}
	
	return nil
}

// CalculateTransactionHash calculates the hash of a transaction
func (tx *Transaction) CalculateTransactionHash() string {
	data := fmt.Sprintf("%s%s%s%.8f%.8f%d%d%s", 
		tx.ID, tx.From, tx.To, tx.Amount, tx.Fee, tx.Nonce, tx.Timestamp, tx.Signature)
	return HashData([]byte(data))
}

// ToJSON converts transaction to JSON string
func (tx *Transaction) ToJSON() (string, error) {
	jsonBytes, err := json.Marshal(tx)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

// TransactionFromJSON creates transaction from JSON string
func TransactionFromJSON(jsonStr string) (*Transaction, error) {
	var tx Transaction
	err := json.Unmarshal([]byte(jsonStr), &tx)
	if err != nil {
		return nil, err
	}
	return &tx, nil
}

// =============================================================================
// TRANSACTION POOL OPERATIONS
// =============================================================================

// AddTransaction adds a transaction to the pool with fee prioritization
func (tp *TransactionPool) AddTransaction(tx *Transaction) error {
	tp.mutex.Lock()
	defer tp.mutex.Unlock()
	
	// Validate transaction
	if err := tx.IsValidTransaction(); err != nil {
		return fmt.Errorf("invalid transaction: %v", err)
	}
	
	// Check for duplicate transaction ID
	if _, exists := tp.transactions[tx.ID]; exists {
		return fmt.Errorf("transaction %s already exists in pool", tx.ID)
	}
	
	// Add to pool
	tp.transactions[tx.ID] = tx
	
	// Insert in fee-sorted order (highest fee first)
	tp.insertByFee(tx)
	
	return nil
}

// insertByFee inserts transaction in fee-sorted order (highest first)
func (tp *TransactionPool) insertByFee(tx *Transaction) {
	// Find insertion point using binary search
	left, right := 0, len(tp.byFee)
	for left < right {
		mid := (left + right) / 2
		if tp.byFee[mid].Fee >= tx.Fee {
			left = mid + 1
		} else {
			right = mid
		}
	}

	// Insert at the correct position
	tp.byFee = append(tp.byFee, nil)
	copy(tp.byFee[left+1:], tp.byFee[left:])
	tp.byFee[left] = tx
}

// RemoveTransaction removes a transaction from the pool
func (tp *TransactionPool) RemoveTransaction(txID string) bool {
	tp.mutex.Lock()
	defer tp.mutex.Unlock()

	_, exists := tp.transactions[txID]
	if !exists {
		return false
	}

	// Remove from map
	delete(tp.transactions, txID)

	// Remove from fee-sorted slice
	for i, sortedTx := range tp.byFee {
		if sortedTx.ID == txID {
			tp.byFee = append(tp.byFee[:i], tp.byFee[i+1:]...)
			break
		}
	}

	return true
}

// GetTopTransactionsByFee returns highest fee transactions
func (tp *TransactionPool) GetTopTransactionsByFee(count int) []*Transaction {
	tp.mutex.RLock()
	defer tp.mutex.RUnlock()

	if count > len(tp.byFee) {
		count = len(tp.byFee)
	}

	result := make([]*Transaction, count)
	copy(result, tp.byFee[:count])
	return result
}

// GetAllTransactions returns all transactions in the pool
func (tp *TransactionPool) GetAllTransactions() []*Transaction {
	tp.mutex.RLock()
	defer tp.mutex.RUnlock()

	result := make([]*Transaction, 0, len(tp.transactions))
	for _, tx := range tp.transactions {
		result = append(result, tx)
	}
	return result
}

// GetTransactionCount returns the number of transactions
func (tp *TransactionPool) GetTransactionCount() int {
	tp.mutex.RLock()
	defer tp.mutex.RUnlock()

	return len(tp.transactions)
}

// RemoveTransactions removes multiple transactions (batch operation)
func (tp *TransactionPool) RemoveTransactions(txIDs []string) int {
	tp.mutex.Lock()
	defer tp.mutex.Unlock()

	removed := 0
	txIDMap := make(map[string]bool)
	
	// Create map for O(1) lookup and remove from main map
	for _, txID := range txIDs {
		if _, exists := tp.transactions[txID]; exists {
			delete(tp.transactions, txID)
			txIDMap[txID] = true
			removed++
		}
	}
	
	// Remove from fee-sorted slice - ✅ FIXED: Use index-based iteration
	if removed > 0 {
		newByFee := make([]*Transaction, 0, len(tp.byFee)-removed)
		for i := range tp.byFee {
			if !txIDMap[tp.byFee[i].ID] {
				newByFee = append(newByFee, tp.byFee[i])
			}
		}
		tp.byFee = newByFee
	}
	
	return removed
}

// GetTransaction retrieves a transaction by ID from the pool
func (tp *TransactionPool) GetTransaction(txID string) *Transaction {
	tp.mutex.RLock()
	defer tp.mutex.RUnlock()

	return tp.transactions[txID]
}