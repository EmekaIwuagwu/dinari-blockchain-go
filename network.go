package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

// =============================================================================
// NETWORK PROTOCOL DEFINITIONS
// =============================================================================

// MessageType defines the type of network message
type MessageType string

const (
	MsgTransaction   MessageType = "transaction"
	MsgBlock         MessageType = "block"
	MsgGetChain      MessageType = "get_chain"
	MsgChain         MessageType = "chain"
	MsgGetPeers      MessageType = "get_peers"
	MsgPeers         MessageType = "peers"
	MsgPing          MessageType = "ping"
	MsgPong          MessageType = "pong"
	MsgGetMempool    MessageType = "get_mempool"
	MsgMempool       MessageType = "mempool"
)

// NetworkMessage represents a P2P network message
type NetworkMessage struct {
	Type      MessageType `json:"type"`
	Data      interface{} `json:"data"`
	Timestamp int64       `json:"timestamp"`
	From      string      `json:"from"`
	Signature string      `json:"signature,omitempty"`
}

// Peer represents a network peer
type Peer struct {
	ID            string    `json:"id"`
	Address       string    `json:"address"`
	Port          int       `json:"port"`
	LastSeen      time.Time `json:"last_seen"`
	Connected     bool      `json:"connected"`
	Version       string    `json:"version"`
	ChainHeight   int64     `json:"chain_height"`
	Latency       int64     `json:"latency_ms"`
	ConnectionTime time.Time `json:"connection_time"`
}

// NetworkStats represents network statistics
type NetworkStats struct {
	PeerCount        int     `json:"peer_count"`
	ActivePeers      int     `json:"active_peers"`
	MessagesSent     int64   `json:"messages_sent"`
	MessagesReceived int64   `json:"messages_received"`
	BytesSent        int64   `json:"bytes_sent"`
	BytesReceived    int64   `json:"bytes_received"`
	Uptime          int64   `json:"uptime_seconds"`
	AverageLatency   float64 `json:"average_latency_ms"`
}

// =============================================================================
// NETWORK MANAGER
// =============================================================================

// NetworkManager handles all P2P networking operations
type NetworkManager struct {
	nodeID          string
	address         string
	port            int
	peers           map[string]*Peer
	blockchain      *Blockchain
	mempool         *Mempool
	server          *http.Server
	rpcServer       *http.Server
	stats           *NetworkStats
	startTime       time.Time
	mutex           sync.RWMutex
	messageHandlers map[MessageType]func(*NetworkMessage, *Peer)
	maxPeers        int
	syncInProgress  bool
}

// NewNetworkManager creates a new network manager
func NewNetworkManager(nodeID, address string, port int, blockchain *Blockchain, mempool *Mempool) *NetworkManager {
	nm := &NetworkManager{
		nodeID:          nodeID,
		address:         address,
		port:            port,
		peers:           make(map[string]*Peer),
		blockchain:      blockchain,
		mempool:         mempool,
		stats:           &NetworkStats{},
		startTime:       time.Now(),
		messageHandlers: make(map[MessageType]func(*NetworkMessage, *Peer)),
		maxPeers:        50,
		syncInProgress:  false,
	}

	// Register message handlers
	nm.registerMessageHandlers()

	return nm
}

// =============================================================================
// SERVER OPERATIONS
// =============================================================================

// StartServer starts the P2P and JSON-RPC servers
func (nm *NetworkManager) StartServer() error {
	nm.mutex.Lock()
	defer nm.mutex.Unlock()

	// Start P2P server
	p2pMux := http.NewServeMux()
	p2pMux.HandleFunc("/p2p", nm.handleP2PConnection)
	p2pMux.HandleFunc("/health", nm.handleHealth)

	nm.server = &http.Server{
		Addr:    fmt.Sprintf("%s:%d", nm.address, nm.port),
		Handler: p2pMux,
	}

	// Start JSON-RPC server on different port
	rpcMux := http.NewServeMux()
	rpcMux.HandleFunc("/rpc", nm.handleJSONRPC)
	rpcMux.HandleFunc("/api/blockchain/info", nm.handleRESTGetBlockchainInfo)
	rpcMux.HandleFunc("/api/blockchain/stats", nm.handleRESTGetStats)
	rpcMux.HandleFunc("/api/blockchain/height", nm.handleRESTGetHeight)
	rpcMux.HandleFunc("/api/block/latest", nm.handleRESTGetLatestBlock)
	rpcMux.HandleFunc("/api/block/", nm.handleRESTGetBlock) // /api/block/{number} or /api/block/{hash}
	rpcMux.HandleFunc("/api/transaction/", nm.handleRESTGetTransaction) // /api/transaction/{id}
	rpcMux.HandleFunc("/api/transaction/send", nm.handleRESTSendTransaction)
	rpcMux.HandleFunc("/api/address/", nm.handleRESTGetAddress) // /api/address/{address}
	rpcMux.HandleFunc("/api/mempool", nm.handleRESTGetMempool)
	rpcMux.HandleFunc("/api/mempool/count", nm.handleRESTGetMempoolCount)
	rpcMux.HandleFunc("/api/treasury/mint", nm.handleTreasuryMint)
	rpcMux.HandleFunc("/api/treasury/burn", nm.handleTreasuryBurn)
	rpcMux.HandleFunc("/api/treasury/stats", nm.handleTreasuryStats)
	rpcMux.HandleFunc("/api/admin/grant-mint", nm.handleGrantMintAuthority)
	rpcMux.HandleFunc("/api/admin/revoke-mint", nm.handleRevokeMintAuthority)
	rpcMux.HandleFunc("/api/admin/grant-admin", nm.handleGrantAdmin)
	rpcMux.HandleFunc("/api/audit/logs", nm.handleGetAuditLogs)
	rpcMux.HandleFunc("/api/stats", nm.handleAPIStats)
	rpcMux.HandleFunc("/api/peers", nm.handleAPIPeers)

	nm.rpcServer = &http.Server{
		Addr:    fmt.Sprintf("%s:%d", nm.address, nm.port+1000), // RPC on port+1000
		Handler: rpcMux,
	}

	// Start servers in goroutines
	go func() {
		if err := nm.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("P2P server error: %v\n", err)
		}
	}()

	go func() {
		if err := nm.rpcServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("RPC server error: %v\n", err)
		}
	}()

	// Start background maintenance
	go nm.startMaintenance()

	fmt.Printf("✅ Network servers started on %s:%d (P2P) and %s:%d (RPC)\n", 
		nm.address, nm.port, nm.address, nm.port+1000)

	return nil
}

// StopServer stops the network servers
func (nm *NetworkManager) StopServer() error {
	nm.mutex.Lock()
	defer nm.mutex.Unlock()

	if nm.server != nil {
		nm.server.Close()
	}
	if nm.rpcServer != nil {
		nm.rpcServer.Close()
	}

	return nil
}


// =============================================================================
// HANDLER IMPLEMENTATIONS - Add to end of network.go
// =============================================================================

// MintRequest represents a mint request
type MintRequest struct {
	ToAddress    string  `json:"to_address"`
	Amount       float64 `json:"amount"`
	AuthorizedBy string  `json:"authorized_by"`
	Reason       string  `json:"reason"`
}

// BurnRequest represents a burn request
type BurnRequest struct {
	FromAddress  string  `json:"from_address"`
	Amount       float64 `json:"amount"`
	AuthorizedBy string  `json:"authorized_by"`
	Reason       string  `json:"reason"`
}

// PermissionRequest represents permission grant/revoke request
type PermissionRequest struct {
	TargetAddress string `json:"target_address"`
	AdminAddress  string `json:"admin_address"`
}

// handleTreasuryMint handles token minting requests
func (nm *NetworkManager) handleTreasuryMint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}

	var req MintRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "Invalid request body",
		})
		return
	}

	// Validate inputs
	if req.ToAddress == "" || req.AuthorizedBy == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "to_address and authorized_by are required",
		})
		return
	}

	if req.Amount <= 0 {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "amount must be positive",
		})
		return
	}

	if req.Reason == "" {
		req.Reason = "API_MINT_REQUEST"
	}

	// Execute mint
	tx, err := nm.blockchain.MintTokens(req.ToAddress, req.Amount, req.AuthorizedBy, req.Reason)
	if err != nil {
		respondJSON(w, http.StatusForbidden, map[string]string{
			"error": err.Error(),
		})
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"success":        true,
		"transaction_id": tx.ID,
		"to_address":     tx.To,
		"amount":         tx.Amount,
		"authorized_by":  req.AuthorizedBy,
		"reason":         req.Reason,
		"timestamp":      tx.Timestamp,
	})
}

// handleTreasuryBurn handles token burning requests
func (nm *NetworkManager) handleTreasuryBurn(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}

	var req BurnRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "Invalid request body",
		})
		return
	}

	// Validate inputs
	if req.FromAddress == "" || req.AuthorizedBy == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "from_address and authorized_by are required",
		})
		return
	}

	if req.Amount <= 0 {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "amount must be positive",
		})
		return
	}

	if req.Reason == "" {
		req.Reason = "API_BURN_REQUEST"
	}

	// Execute burn
	err := nm.blockchain.BurnTokens(req.FromAddress, req.Amount, req.AuthorizedBy, req.Reason)
	if err != nil {
		respondJSON(w, http.StatusForbidden, map[string]string{
			"error": err.Error(),
		})
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"success":       true,
		"from_address":  req.FromAddress,
		"amount":        req.Amount,
		"authorized_by": req.AuthorizedBy,
		"reason":        req.Reason,
		"timestamp":     time.Now().Unix(),
	})
}

// handleTreasuryStats returns treasury statistics
func (nm *NetworkManager) handleTreasuryStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := nm.blockchain.GetTreasuryStats()
	respondJSON(w, http.StatusOK, stats)
}

// handleGrantMintAuthority grants mint authority to an address
func (nm *NetworkManager) handleGrantMintAuthority(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PermissionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "Invalid request body",
		})
		return
	}

	// Validate inputs
	if req.TargetAddress == "" || req.AdminAddress == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "target_address and admin_address are required",
		})
		return
	}

	// Execute permission grant
	err := nm.blockchain.AddMintAuthority(req.TargetAddress, req.AdminAddress)
	if err != nil {
		respondJSON(w, http.StatusForbidden, map[string]string{
			"error": err.Error(),
		})
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"success":        true,
		"target_address": req.TargetAddress,
		"granted_by":     req.AdminAddress,
		"permission":     "MINT",
		"timestamp":      time.Now().Unix(),
	})
}

// handleRevokeMintAuthority revokes mint authority from an address
func (nm *NetworkManager) handleRevokeMintAuthority(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PermissionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "Invalid request body",
		})
		return
	}

	// Validate inputs
	if req.TargetAddress == "" || req.AdminAddress == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "target_address and admin_address are required",
		})
		return
	}

	// Execute permission revoke
	err := nm.blockchain.RemoveMintAuthority(req.TargetAddress, req.AdminAddress)
	if err != nil {
		respondJSON(w, http.StatusForbidden, map[string]string{
			"error": err.Error(),
		})
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"success":        true,
		"target_address": req.TargetAddress,
		"revoked_by":     req.AdminAddress,
		"permission":     "MINT",
		"timestamp":      time.Now().Unix(),
	})
}

// handleGrantAdmin grants admin privileges to an address
func (nm *NetworkManager) handleGrantAdmin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PermissionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "Invalid request body",
		})
		return
	}

	// Validate inputs
	if req.TargetAddress == "" || req.AdminAddress == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "target_address and admin_address are required",
		})
		return
	}

	// Execute admin grant
	err := nm.blockchain.AddAdmin(req.TargetAddress, req.AdminAddress)
	if err != nil {
		respondJSON(w, http.StatusForbidden, map[string]string{
			"error": err.Error(),
		})
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"success":        true,
		"target_address": req.TargetAddress,
		"granted_by":     req.AdminAddress,
		"permission":     "ADMIN",
		"timestamp":      time.Now().Unix(),
	})
}

// handleGetAuditLogs returns audit logs (admin only)
func (nm *NetworkManager) handleGetAuditLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get admin address from query parameter
	adminAddress := r.URL.Query().Get("admin")
	if adminAddress == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "admin address required in query parameter",
		})
		return
	}

	// Get limit from query parameter (default 100)
	limitStr := r.URL.Query().Get("limit")
	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	// Get audit logs
	logs, err := nm.blockchain.GetAuditLog(adminAddress, limit)
	if err != nil {
		respondJSON(w, http.StatusForbidden, map[string]string{
			"error": err.Error(),
		})
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"count": len(logs),
		"logs":  logs,
	})
}



// =============================================================================
// REST API HANDLER IMPLEMENTATIONS
// =============================================================================

// handleRESTGetBlockchainInfo returns blockchain information
func (nm *NetworkManager) handleRESTGetBlockchainInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := nm.blockchain.GetStats()
	
	info := map[string]interface{}{
		"chain_height":       nm.blockchain.GetChainLength(),
		"difficulty":         stats.Difficulty,
		"block_reward":       stats.BlockReward,
		"total_blocks":       stats.BlockCount,
		"total_transactions": stats.TotalTransactions,
		"total_value":        stats.TotalValue,
		"total_fees":         stats.TotalFees,
	}

	respondJSON(w, http.StatusOK, info)
}

// handleRESTGetStats returns detailed blockchain stats
func (nm *NetworkManager) handleRESTGetStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := nm.blockchain.GetStats()
	respondJSON(w, http.StatusOK, stats)
}

// handleRESTGetHeight returns current blockchain height
func (nm *NetworkManager) handleRESTGetHeight(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method allowed", http.StatusMethodNotAllowed)
		return
	}

	height := nm.blockchain.GetHeight()
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"height": height,
	})
}

// handleRESTGetLatestBlock returns the latest block
func (nm *NetworkManager) handleRESTGetLatestBlock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method allowed", http.StatusMethodNotAllowed)
		return
	}

	block := nm.blockchain.GetLatestBlock()
	if block == nil {
		respondJSON(w, http.StatusNotFound, map[string]string{
			"error": "No blocks found",
		})
		return
	}

	respondJSON(w, http.StatusOK, block)
}

// handleRESTGetBlock returns a block by number or hash
func (nm *NetworkManager) handleRESTGetBlock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract block identifier from URL path
	path := strings.TrimPrefix(r.URL.Path, "/api/block/")
	if path == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "Block number or hash required",
		})
		return
	}

	// Try as number first
	if blockNum, err := strconv.Atoi(path); err == nil {
		block := nm.blockchain.GetBlockByNumber(blockNum)
		if block == nil {
			respondJSON(w, http.StatusNotFound, map[string]string{
				"error": "Block not found",
			})
			return
		}
		respondJSON(w, http.StatusOK, block)
		return
	}

	// Try as hash
	block := nm.blockchain.GetBlockByHash(path)
	if block == nil {
		respondJSON(w, http.StatusNotFound, map[string]string{
			"error": "Block not found",
		})
		return
	}
	respondJSON(w, http.StatusOK, block)
}

// handleRESTGetTransaction returns a transaction by ID
func (nm *NetworkManager) handleRESTGetTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract transaction ID from URL path
	txID := strings.TrimPrefix(r.URL.Path, "/api/transaction/")
	if txID == "" || txID == "send" {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "Transaction ID required",
		})
		return
	}

	tx := nm.blockchain.GetTransactionByID(txID)
	if tx == nil {
		respondJSON(w, http.StatusNotFound, map[string]string{
			"error": "Transaction not found",
		})
		return
	}

	respondJSON(w, http.StatusOK, tx)
}

// SendTransactionRequest represents REST API send transaction request
type SendTransactionRequest struct {
	From   string  `json:"from"`
	To     string  `json:"to"`
	Amount float64 `json:"amount"`
	Fee    float64 `json:"fee"`
}

// handleRESTSendTransaction creates and broadcasts a transaction
func (nm *NetworkManager) handleRESTSendTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SendTransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "Invalid request body",
		})
		return
	}

	// Validate inputs
	if req.From == "" || req.To == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "from and to addresses are required",
		})
		return
	}

	if req.Amount <= 0 {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "amount must be positive",
		})
		return
	}

	// Get nonce
	nonce := nm.blockchain.GetNonce(req.From)

	// Create transaction
	tx, err := CreateTransaction(req.From, req.To, req.Amount, req.Fee, nonce)
	if err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": err.Error(),
		})
		return
	}

	// Note: In production, you'd need to sign the transaction here
	// For now, returning unsigned transaction info
	
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"transaction_id": tx.ID,
		"from":           tx.From,
		"to":             tx.To,
		"amount":         tx.Amount,
		"fee":            tx.Fee,
		"nonce":          tx.Nonce,
		"note":           "Transaction created but needs to be signed before broadcasting",
	})
}

// handleRESTGetAddress returns address information
func (nm *NetworkManager) handleRESTGetAddress(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract address from URL path
	address := strings.TrimPrefix(r.URL.Path, "/api/address/")
	if address == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "Address required",
		})
		return
	}

	// Validate address
	if !ValidateAddress(address) {
		respondJSON(w, http.StatusBadRequest, map[string]string{
			"error": "Invalid address format",
		})
		return
	}

	balance := nm.blockchain.GetBalance(address)
	nonce := nm.blockchain.GetNonce(address)

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"address": address,
		"balance": balance,
		"nonce":   nonce,
	})
}

// handleRESTGetMempool returns all pending transactions
func (nm *NetworkManager) handleRESTGetMempool(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method allowed", http.StatusMethodNotAllowed)
		return
	}

	transactions := nm.mempool.GetAllTransactions()
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"count":        len(transactions),
		"transactions": transactions,
	})
}

// handleRESTGetMempoolCount returns mempool transaction count
func (nm *NetworkManager) handleRESTGetMempoolCount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method allowed", http.StatusMethodNotAllowed)
		return
	}

	count := nm.mempool.GetTransactionCount()
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"count": count,
	})
}

// respondJSON helper function to send JSON responses
func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// =============================================================================
// P2P MESSAGE HANDLING
// =============================================================================

// handleP2PConnection handles incoming P2P connections
func (nm *NetworkManager) handleP2PConnection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var msg NetworkMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		http.Error(w, "Invalid JSON message", http.StatusBadRequest)
		return
	}

	// Update peer info
	peerAddr := r.RemoteAddr
	peer := nm.getOrCreatePeer(peerAddr, msg.From)

	// Handle message
	nm.handleMessage(&msg, peer)

	// Send response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})

	nm.updateStats(false, int64(len(body)))
}

// registerMessageHandlers registers handlers for different message types
func (nm *NetworkManager) registerMessageHandlers() {
	nm.messageHandlers[MsgTransaction] = nm.handleTransactionMessage
	nm.messageHandlers[MsgBlock] = nm.handleBlockMessage
	nm.messageHandlers[MsgGetChain] = nm.handleGetChainMessage
	nm.messageHandlers[MsgChain] = nm.handleChainMessage
	nm.messageHandlers[MsgGetPeers] = nm.handleGetPeersMessage
	nm.messageHandlers[MsgPeers] = nm.handlePeersMessage
	nm.messageHandlers[MsgPing] = nm.handlePingMessage
	nm.messageHandlers[MsgPong] = nm.handlePongMessage
	nm.messageHandlers[MsgGetMempool] = nm.handleGetMempoolMessage
	nm.messageHandlers[MsgMempool] = nm.handleMempoolMessage
}

// handleMessage processes incoming network messages
func (nm *NetworkManager) handleMessage(msg *NetworkMessage, peer *Peer) {
	handler, exists := nm.messageHandlers[msg.Type]
	if !exists {
		fmt.Printf("Unknown message type: %s\n", msg.Type)
		return
	}

	handler(msg, peer)
}

// handleTransactionMessage handles incoming transaction broadcasts
func (nm *NetworkManager) handleTransactionMessage(msg *NetworkMessage, peer *Peer) {
	txData, ok := msg.Data.(map[string]interface{})
	if !ok {
		return
	}

	txJSON, _ := json.Marshal(txData)
	tx, err := TransactionFromJSON(string(txJSON))
	if err != nil {
		return
	}

	// Add to mempool
	err = nm.mempool.AddTransaction(tx)
	if err != nil {
		fmt.Printf("Failed to add transaction to mempool: %v\n", err)
		return
	}

	// Broadcast to other peers (except sender)
	nm.broadcastMessage(msg, peer.ID)
}

// handleBlockMessage handles incoming block broadcasts
func (nm *NetworkManager) handleBlockMessage(msg *NetworkMessage, peer *Peer) {
	blockData, ok := msg.Data.(map[string]interface{})
	if !ok {
		return
	}

	blockJSON, _ := json.Marshal(blockData)
	block, err := BlockFromJSON(string(blockJSON))
	if err != nil {
		return
	}

	// Add to blockchain
	err = nm.blockchain.AddBlock(block)
	if err != nil {
		fmt.Printf("Failed to add block to blockchain: %v\n", err)
		return
	}

	// Update peer chain height
	peer.ChainHeight = block.Number

	// Broadcast to other peers (except sender)
	nm.broadcastMessage(msg, peer.ID)
}

// handleGetChainMessage handles blockchain sync requests
func (nm *NetworkManager) handleGetChainMessage(msg *NetworkMessage, peer *Peer) {
	blocks := nm.blockchain.GetBlockRange(0, int64(nm.blockchain.GetChainLength()-1))
	
	response := &NetworkMessage{
		Type:      MsgChain,
		Data:      blocks,
		Timestamp: time.Now().Unix(),
		From:      nm.nodeID,
	}

	nm.sendMessageToPeer(response, peer)
}

// handleChainMessage handles blockchain sync responses
func (nm *NetworkManager) handleChainMessage(msg *NetworkMessage, peer *Peer) {
	blocksData, ok := msg.Data.([]interface{})
	if !ok {
		return
	}

	var blocks []*Block
	for _, blockData := range blocksData {
		blockJSON, _ := json.Marshal(blockData)
		block, err := BlockFromJSON(string(blockJSON))
		if err != nil {
			continue
		}
		blocks = append(blocks, block)
	}

	// Replace chain if longer and valid
	if len(blocks) > nm.blockchain.GetChainLength() {
		err := nm.blockchain.ReplaceChain(blocks)
		if err != nil {
			fmt.Printf("Failed to replace chain: %v\n", err)
		} else {
			fmt.Printf("Blockchain synchronized with %d blocks\n", len(blocks))
		}
	}
}

// handleGetPeersMessage handles peer discovery requests
func (nm *NetworkManager) handleGetPeersMessage(msg *NetworkMessage, peer *Peer) {
	peers := nm.GetActivePeers()
	
	response := &NetworkMessage{
		Type:      MsgPeers,
		Data:      peers,
		Timestamp: time.Now().Unix(),
		From:      nm.nodeID,
	}

	nm.sendMessageToPeer(response, peer)
}

// handlePeersMessage handles peer discovery responses
func (nm *NetworkManager) handlePeersMessage(msg *NetworkMessage, peer *Peer) {
	peersData, ok := msg.Data.([]*Peer)
	if !ok {
		return
	}

	// Add new peers
	for _, newPeer := range peersData {
		if newPeer.ID != nm.nodeID && nm.getPeer(newPeer.ID) == nil {
			nm.addPeer(newPeer)
		}
	}
}

// handlePingMessage handles ping requests
func (nm *NetworkManager) handlePingMessage(msg *NetworkMessage, peer *Peer) {
	response := &NetworkMessage{
		Type:      MsgPong,
		Data:      time.Now().Unix(),
		Timestamp: time.Now().Unix(),
		From:      nm.nodeID,
	}

	nm.sendMessageToPeer(response, peer)
}

// handlePongMessage handles ping responses
func (nm *NetworkManager) handlePongMessage(msg *NetworkMessage, peer *Peer) {
	pingTime, ok := msg.Data.(int64)
	if !ok {
		return
	}

	latency := time.Now().Unix() - pingTime
	peer.Latency = latency * 1000 // Convert to milliseconds
}

// handleGetMempoolMessage handles mempool sync requests
func (nm *NetworkManager) handleGetMempoolMessage(msg *NetworkMessage, peer *Peer) {
	transactions := nm.mempool.GetAllTransactions()
	
	response := &NetworkMessage{
		Type:      MsgMempool,
		Data:      transactions,
		Timestamp: time.Now().Unix(),
		From:      nm.nodeID,
	}

	nm.sendMessageToPeer(response, peer)
}

// handleMempoolMessage handles mempool sync responses
func (nm *NetworkManager) handleMempoolMessage(msg *NetworkMessage, peer *Peer) {
	txsData, ok := msg.Data.([]interface{})
	if !ok {
		return
	}

	for _, txData := range txsData {
		txJSON, _ := json.Marshal(txData)
		tx, err := TransactionFromJSON(string(txJSON))
		if err != nil {
			continue
		}

		nm.mempool.AddTransaction(tx)
	}
}

// =============================================================================
// JSON-RPC SERVER
// =============================================================================

// JSONRPCRequest represents a JSON-RPC request
type JSONRPCRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      interface{}   `json:"id"`
}

// JSONRPCResponse represents a JSON-RPC response
type JSONRPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *RPCError   `json:"error,omitempty"`
	ID      interface{} `json:"id"`
}

// RPCError represents a JSON-RPC error
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// handleJSONRPC handles JSON-RPC requests
func (nm *NetworkManager) handleJSONRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var req JSONRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		nm.sendRPCError(w, req.ID, -32700, "Parse error")
		return
	}

	result, err := nm.handleRPCMethod(req.Method, req.Params)
	if err != nil {
		nm.sendRPCError(w, req.ID, -32603, err.Error())
		return
	}

	response := JSONRPCResponse{
		JSONRPC: "2.0",
		Result:  result,
		ID:      req.ID,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleRPCMethod handles specific RPC methods
func (nm *NetworkManager) handleRPCMethod(method string, params []interface{}) (interface{}, error) {
	switch method {
	case "getBlockchainInfo":
		return nm.blockchain.GetStats(), nil
	
	case "getBalance":
		if len(params) < 1 {
			return nil, fmt.Errorf("address parameter required")
		}
		address, ok := params[0].(string)
		if !ok {
			return nil, fmt.Errorf("invalid address parameter")
		}
		return nm.blockchain.GetBalance(address), nil
	
	case "sendTransaction":
		if len(params) < 4 {
			return nil, fmt.Errorf("insufficient parameters")
		}
		from := params[0].(string)
		to := params[1].(string)
		amount := params[2].(float64)
		fee := params[3].(float64)
		
		nonce := nm.blockchain.GetNonce(from)
		tx, err := CreateTransaction(from, to, amount, fee, nonce)
		if err != nil {
			return nil, err
		}
		
		return tx.ID, nm.BroadcastTransaction(tx)
	
	case "getTransaction":
		if len(params) < 1 {
			return nil, fmt.Errorf("transaction ID required")
		}
		txID := params[0].(string)
		return nm.blockchain.GetTransactionByID(txID), nil
	
	case "getBlock":
		if len(params) < 1 {
			return nil, fmt.Errorf("block number or hash required")
		}
		param := params[0]
		
		// ✅ FIXED: Cast float64 to int for GetBlockByNumber
		if blockNum, ok := param.(float64); ok {
			return nm.blockchain.GetBlockByNumber(int(blockNum)), nil
		}
		if blockHash, ok := param.(string); ok {
			return nm.blockchain.GetBlockByHash(blockHash), nil
		}
		return nil, fmt.Errorf("invalid block parameter")
	
	case "getMempool":
		return nm.mempool.GetAllTransactions(), nil
	
	case "getPeers":
		return nm.GetActivePeers(), nil
	
	case "getNetworkStats":
		return nm.GetNetworkStats(), nil
	
	default:
		return nil, fmt.Errorf("method not found: %s", method)
	}
}

// sendRPCError sends a JSON-RPC error response
func (nm *NetworkManager) sendRPCError(w http.ResponseWriter, id interface{}, code int, message string) {
	response := JSONRPCResponse{
		JSONRPC: "2.0",
		Error: &RPCError{
			Code:    code,
			Message: message,
		},
		ID: id,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // JSON-RPC errors are still HTTP 200
	json.NewEncoder(w).Encode(response)
}

// =============================================================================
// PEER MANAGEMENT
// =============================================================================

// ConnectToPeer connects to a new peer
func (nm *NetworkManager) ConnectToPeer(address string, port int) error {
	peerAddr := fmt.Sprintf("%s:%d", address, port)
	fmt.Printf("Processing peer: %s\n", peerAddr)
	peerID := fmt.Sprintf("peer_%s_%d", address, port)

	// Check if already connected
	if peer := nm.getPeer(peerID); peer != nil && peer.Connected {
		return fmt.Errorf("already connected to peer %s", peerID)
	}

	// Create peer
	peer := &Peer{
		ID:             peerID,
		Address:        address,
		Port:           port,
		Connected:      true,
		ConnectionTime: time.Now(),
		LastSeen:       time.Now(),
		Version:        "1.0.0",
	}

	nm.addPeer(peer)

	// Send ping to establish connection
	return nm.sendPing(peer)
}

// addPeer adds a peer to the network
func (nm *NetworkManager) addPeer(peer *Peer) {
	nm.mutex.Lock()
	defer nm.mutex.Unlock()

	if len(nm.peers) >= nm.maxPeers {
		// Remove oldest inactive peer
		nm.removeOldestInactivePeer()
	}

	nm.peers[peer.ID] = peer
	nm.stats.PeerCount = len(nm.peers)
}

// removePeer removes a peer from the network
func (nm *NetworkManager) removePeer(peerID string) {
	nm.mutex.Lock()
	defer nm.mutex.Unlock()

	delete(nm.peers, peerID)
	nm.stats.PeerCount = len(nm.peers)
}

// getPeer retrieves a peer by ID
func (nm *NetworkManager) getPeer(peerID string) *Peer {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()

	return nm.peers[peerID]
}

// getOrCreatePeer gets existing peer or creates new one
func (nm *NetworkManager) getOrCreatePeer(address, nodeID string) *Peer {
	// Parse address to get IP and port
	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		host = address
		portStr = "8080" // Default port
	}
	
	port, _ := strconv.Atoi(portStr)
	peerID := nodeID
	if peerID == "" {
		peerID = fmt.Sprintf("peer_%s_%d", host, port)
	}

	peer := nm.getPeer(peerID)
	if peer == nil {
		peer = &Peer{
			ID:        peerID,
			Address:   host,
			Port:      port,
			Connected: true,
			LastSeen:  time.Now(),
			Version:   "1.0.0",
		}
		nm.addPeer(peer)
	} else {
		peer.LastSeen = time.Now()
		peer.Connected = true
	}

	return peer
}

// GetActivePeers returns all active peers
func (nm *NetworkManager) GetActivePeers() []*Peer {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()

	var activePeers []*Peer
	for _, peer := range nm.peers {
		if peer.Connected && time.Since(peer.LastSeen) < 5*time.Minute {
			activePeers = append(activePeers, peer)
		}
	}

	return activePeers
}

// removeOldestInactivePeer removes the oldest inactive peer
func (nm *NetworkManager) removeOldestInactivePeer() {
	var oldestPeer *Peer
	oldestTime := time.Now()

	for _, peer := range nm.peers {
		if !peer.Connected && peer.LastSeen.Before(oldestTime) {
			oldestTime = peer.LastSeen
			oldestPeer = peer
		}
	}

	if oldestPeer != nil {
		delete(nm.peers, oldestPeer.ID)
	}
}

// =============================================================================
// MESSAGE BROADCASTING
// =============================================================================

// BroadcastTransaction broadcasts a transaction to all peers
func (nm *NetworkManager) BroadcastTransaction(tx *Transaction) error {
	msg := &NetworkMessage{
		Type:      MsgTransaction,
		Data:      tx,
		Timestamp: time.Now().Unix(),
		From:      nm.nodeID,
	}

	return nm.broadcastMessage(msg, "")
}

// BroadcastBlock broadcasts a block to all peers
func (nm *NetworkManager) BroadcastBlock(block *Block) error {
	msg := &NetworkMessage{
		Type:      MsgBlock,
		Data:      block,
		Timestamp: time.Now().Unix(),
		From:      nm.nodeID,
	}

	return nm.broadcastMessage(msg, "")
}

// broadcastMessage sends a message to all peers except excluded one
func (nm *NetworkManager) broadcastMessage(msg *NetworkMessage, excludePeerID string) error {
	peers := nm.GetActivePeers()
	
	for _, peer := range peers {
		if peer.ID != excludePeerID {
			go nm.sendMessageToPeer(msg, peer)
		}
	}

	return nil
}

// sendMessageToPeer sends a message to a specific peer
func (nm *NetworkManager) sendMessageToPeer(msg *NetworkMessage, peer *Peer) error {
	jsonData, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("http://%s:%d/p2p", peer.Address, peer.Port)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		peer.Connected = false
		return err
	}
	defer resp.Body.Close()

	nm.updateStats(true, int64(len(jsonData)))
	return nil
}

// sendPing sends a ping message to a peer
func (nm *NetworkManager) sendPing(peer *Peer) error {
	msg := &NetworkMessage{
		Type:      MsgPing,
		Data:      time.Now().Unix(),
		Timestamp: time.Now().Unix(),
		From:      nm.nodeID,
	}

	return nm.sendMessageToPeer(msg, peer)
}

// =============================================================================
// STATISTICS AND MAINTENANCE
// =============================================================================

// updateStats updates network statistics
func (nm *NetworkManager) updateStats(sent bool, bytes int64) {
	nm.mutex.Lock()
	defer nm.mutex.Unlock()

	if sent {
		nm.stats.MessagesSent++
		nm.stats.BytesSent += bytes
	} else {
		nm.stats.MessagesReceived++
		nm.stats.BytesReceived += bytes
	}

	// Update active peers count
	activeCount := 0
	totalLatency := int64(0)
	latencyCount := 0

	for _, peer := range nm.peers {
		if peer.Connected && time.Since(peer.LastSeen) < 5*time.Minute {
			activeCount++
			if peer.Latency > 0 {
				totalLatency += peer.Latency
				latencyCount++
			}
		}
	}

	nm.stats.ActivePeers = activeCount
	nm.stats.Uptime = int64(time.Since(nm.startTime).Seconds())

	if latencyCount > 0 {
		nm.stats.AverageLatency = float64(totalLatency) / float64(latencyCount)
	}
}

// GetNetworkStats returns current network statistics
func (nm *NetworkManager) GetNetworkStats() *NetworkStats {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()

	// Return copy to avoid race conditions
	statsCopy := *nm.stats
	return &statsCopy
}

// startMaintenance starts background maintenance routines
func (nm *NetworkManager) startMaintenance() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		nm.performMaintenance()
	}
}

// performMaintenance performs periodic network maintenance
func (nm *NetworkManager) performMaintenance() {
	nm.mutex.Lock()
	defer nm.mutex.Unlock()

	now := time.Now()
	var peersToRemove []string

	// Check peer health and remove stale peers
	for peerID, peer := range nm.peers {
		if now.Sub(peer.LastSeen) > 10*time.Minute {
			peersToRemove = append(peersToRemove, peerID)
		} else if peer.Connected {
			// Send periodic ping
			go nm.sendPing(peer)
		}
	}

	// Remove stale peers
	for _, peerID := range peersToRemove {
		delete(nm.peers, peerID)
	}

	nm.stats.PeerCount = len(nm.peers)
}

// =============================================================================
// API ENDPOINTS
// =============================================================================

// handleHealth handles health check requests
func (nm *NetworkManager) handleHealth(w http.ResponseWriter, r *http.Request) {
	health := map[string]interface{}{
		"status":      "healthy",
		"uptime":      int64(time.Since(nm.startTime).Seconds()),
		"peers":       len(nm.peers),
		"chain_height": nm.blockchain.GetChainLength(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

// handleAPIStats handles statistics API requests
func (nm *NetworkManager) handleAPIStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nm.GetNetworkStats())
}

// handleAPIPeers handles peers API requests
func (nm *NetworkManager) handleAPIPeers(w http.ResponseWriter, r *http.Request) {
	peers := nm.GetActivePeers()
	
	// Sort peers by connection time
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].ConnectionTime.Before(peers[j].ConnectionTime)
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(peers)
}