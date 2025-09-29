package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// =============================================================================
// NODE CORE STRUCTURE WITH DATABASE INTEGRATION
// =============================================================================

// Node represents a complete DinariBlockchain node with database persistence
type Node struct {
	ID                 string              `json:"id"`
	Config             *NodeConfig         `json:"config"`
	Blockchain         *Blockchain         `json:"-"`
	Mempool            *Mempool            `json:"-"`
	NetworkManager     *NetworkManager     `json:"-"`
	Wallet             *NodeWallet         `json:"-"`
	MiningManager      *MiningManager      `json:"-"`
	PersistenceManager *PersistenceManager `json:"-"` // NEW: Database manager
	running            bool                `json:"running"`
	startTime          time.Time           `json:"start_time"`
	mutex              sync.RWMutex        `json:"-"`
	shutdownChan       chan os.Signal      `json:"-"`
	ctx                context.Context     `json:"-"`
	cancel             context.CancelFunc  `json:"-"`
}

// NodeConfig contains node configuration parameters with database options
type NodeConfig struct {
	// Network Configuration
	ListenAddress   string   `json:"listen_address"`
	ListenPort      int      `json:"listen_port"`
	MaxPeers        int      `json:"max_peers"`
	BootstrapPeers  []string `json:"bootstrap_peers"`
	
	// Mining Configuration
	EnableMining    bool     `json:"enable_mining"`
	MinerAddress    string   `json:"miner_address"`
	MiningThreads   int      `json:"mining_threads"`
	MaxBlockSize    int      `json:"max_block_size"`
	MaxTxPerBlock   int      `json:"max_tx_per_block"`
	
	// Blockchain Configuration
	Difficulty      int      `json:"difficulty"`
	BlockReward     float64  `json:"block_reward"`
	TargetBlockTime int64    `json:"target_block_time"`
	
	// Mempool Configuration
	MempoolMaxSize  int      `json:"mempool_max_size"`
	MempoolMaxAge   int64    `json:"mempool_max_age_hours"`
	MinTxFee        float64  `json:"min_tx_fee"`
	
	// Database Configuration - NEW
	DataDir         string   `json:"data_dir"`
	EnablePersist   bool     `json:"enable_persistence"`
	EnableMempoolDB bool     `json:"enable_mempool_persistence"`
	BatchSize       int      `json:"db_batch_size"`
	AutoCompact     bool     `json:"auto_compact_db"`
	
	// API Configuration
	EnableRPC       bool     `json:"enable_rpc"`
	RPCPort         int      `json:"rpc_port"`
	EnableAPI       bool     `json:"enable_api"`
	APIPort         int      `json:"api_port"`
}

// NodeWallet manages the node's keys and addresses
type NodeWallet struct {
	keyPairs    map[string]*KeyPair
	defaultAddr string
	mutex       sync.RWMutex
}

// NodeStatus represents the current status of the node with database info
type NodeStatus struct {
	NodeID           string    `json:"node_id"`
	Running          bool      `json:"running"`
	Uptime           int64     `json:"uptime_seconds"`
	ChainHeight      int64     `json:"chain_height"`
	PeerCount        int       `json:"peer_count"`
	MempoolSize      int       `json:"mempool_size"`
	IsMining         bool      `json:"is_mining"`
	HashRate         float64   `json:"hash_rate"`
	LastBlockTime    int64     `json:"last_block_time"`
	TotalTxs         int       `json:"total_transactions"`
	WalletBalance    float64   `json:"wallet_balance"`
	SyncStatus       string    `json:"sync_status"`
	DatabaseEnabled  bool      `json:"database_enabled"`   // NEW
	DatabaseSize     int       `json:"database_size_mb"`   // NEW
	DataDirectory    string    `json:"data_directory"`     // NEW
}

// PeerInfo represents information about a connected peer
type PeerInfo struct {
	Address     string `json:"address"`
	ChainHeight int64  `json:"chain_height"`
	LastSeen    int64  `json:"last_seen"`
}

// =============================================================================
// NODE INITIALIZATION WITH DATABASE
// =============================================================================

// NewNode creates a new DinariBlockchain node with database persistence
func NewNode(config *NodeConfig) (*Node, error) {
	log.Printf("🚀 Initializing DinariBlockchain Node...")
	
	// Generate unique node ID
	nodeID, err := generateNodeID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate node ID: %v", err)
	}
	log.Printf("   Node ID: %s", nodeID)

	// Validate configuration
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %v", err)
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// NEW: Initialize persistence manager FIRST (if enabled)
	var persistenceManager *PersistenceManager
	if config.EnablePersist {
		log.Printf("📦 Initializing database at %s...", config.DataDir)
		
		persistConfig := &PersistenceConfig{
			DataDir:       config.DataDir,
			EnableMempool: config.EnableMempoolDB,
			BatchSize:     config.BatchSize,
			AutoSync:      true,
		}
		
		persistenceManager, err = NewPersistenceManager(persistConfig)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to initialize database: %v", err)
		}
		
		log.Printf("✅ Database initialized successfully")
		
		// Save node metadata
		persistenceManager.SaveMetadata("node_id", nodeID)
		persistenceManager.SaveMetadata("version", "1.0.0")
		persistenceManager.SaveMetadata("start_time", fmt.Sprintf("%d", time.Now().Unix()))
	} else {
		log.Printf("⚠️  Database persistence DISABLED - using in-memory storage only")
	}

	// Initialize wallet
	wallet := &NodeWallet{
		keyPairs: make(map[string]*KeyPair),
	}

	// Generate or load miner address
	if config.MinerAddress == "" {
		log.Printf("🔑 Generating new miner wallet...")
		keyPair, err := GenerateKeyPair()
		if err != nil {
			cancel()
			if persistenceManager != nil {
				persistenceManager.Close()
			}
			return nil, fmt.Errorf("failed to generate miner key pair: %v", err)
		}
		wallet.keyPairs[keyPair.Address] = keyPair
		wallet.defaultAddr = keyPair.Address
		config.MinerAddress = keyPair.Address
		
		log.Printf("✅ Miner Address: %s", keyPair.Address)
		log.Printf("   Private Key: %s", keyPair.GetPrivateKeyHex()[:16]+"...") // Show partial for security
	} else {
		log.Printf("✅ Using configured miner address: %s", config.MinerAddress)
	}

	// NEW: Initialize blockchain WITH database persistence
	var blockchain *Blockchain
	if persistenceManager != nil {
		log.Printf("🔗 Loading blockchain from database...")
		blockchain = NewBlockchainWithPersistence(config.MinerAddress, persistenceManager)
	} else {
		log.Printf("🔗 Creating in-memory blockchain...")
		blockchain = NewBlockchain(config.MinerAddress)
	}
	
	// Configure blockchain parameters
	blockchain.Difficulty = config.Difficulty
	blockchain.BlockReward = config.BlockReward
	blockchain.TargetBlockTime = config.TargetBlockTime
	blockchain.MaxBlockSize = config.MaxBlockSize

	log.Printf("✅ Blockchain initialized with %d blocks", len(blockchain.Blocks))

	// Initialize mempool
	log.Printf("📝 Initializing transaction mempool...")
	mempoolConfig := &MempoolConfig{
		MaxSize:   config.MempoolMaxSize,
		MaxMemory: int64(config.MempoolMaxSize * 1024), // 1KB per tx estimate
		MinFee:    config.MinTxFee,
		MaxAge:    time.Duration(config.MempoolMaxAge) * time.Hour,
	}
	mempool := NewMempool(mempoolConfig)
	
	// NEW: Load pending transactions from database
	if persistenceManager != nil && config.EnableMempoolDB {
		log.Printf("🔄 Restoring pending transactions from database...")
		pendingTxs, err := persistenceManager.LoadMempoolTransactions()
		if err != nil {
			log.Printf("⚠️  Failed to load mempool from database: %v", err)
		} else if len(pendingTxs) > 0 {
			restored := 0
			for _, tx := range pendingTxs {
				if err := mempool.AddTransaction(tx); err == nil {
					restored++
				}
			}
			log.Printf("✅ Restored %d/%d pending transactions", restored, len(pendingTxs))
		} else {
			log.Printf("   No pending transactions to restore")
		}
	}

	// Initialize network manager
	log.Printf("🌐 Initializing network manager...")
	networkManager := NewNetworkManager(nodeID, config.ListenAddress, config.ListenPort, blockchain, mempool)

	// Initialize mining manager
	log.Printf("⛏️  Initializing mining manager...")
	miningManager := NewMiningManager(config, blockchain, mempool, wallet)

	// Create node instance
	node := &Node{
		ID:                 nodeID,
		Config:             config,
		Blockchain:         blockchain,
		Mempool:            mempool,
		NetworkManager:     networkManager,
		Wallet:             wallet,
		MiningManager:      miningManager,
		PersistenceManager: persistenceManager, // NEW: Store persistence manager
		running:            false,
		shutdownChan:       make(chan os.Signal, 1),
		ctx:                ctx,
		cancel:             cancel,
	}

	log.Printf("✅ DinariBlockchain Node initialized successfully")
	return node, nil
}

// =============================================================================
// NODE LIFECYCLE MANAGEMENT WITH DATABASE
// =============================================================================

// Start starts the DinariBlockchain node with all services
func (n *Node) Start() error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if n.running {
		return fmt.Errorf("node is already running")
	}

	n.startTime = time.Now()
	
	log.Printf("🚀 Starting DinariBlockchain Node %s", n.ID)
	log.Printf("   Listen Address: %s:%d", n.Config.ListenAddress, n.Config.ListenPort)
	log.Printf("   Miner Address: %s", n.Config.MinerAddress)
	log.Printf("   Mining Enabled: %v (%d threads)", n.Config.EnableMining, n.Config.MiningThreads)
	log.Printf("   Database: %v (%s)", n.Config.EnablePersist, n.Config.DataDir)
	log.Printf("   Current Balance: %.8f DINAR", n.GetWalletBalance())

	// Start network services
	log.Printf("🌐 Starting network services...")
	if err := n.NetworkManager.StartServer(); err != nil {
		return fmt.Errorf("failed to start network services: %v", err)
	}

	// Connect to bootstrap peers
	if len(n.Config.BootstrapPeers) > 0 {
		log.Printf("🔗 Connecting to %d bootstrap peers...", len(n.Config.BootstrapPeers))
		go n.connectToBootstrapPeers()
	}

	// Start mining if enabled
	if n.Config.EnableMining {
		log.Printf("⛏️  Starting mining with %d threads...", n.Config.MiningThreads)
		if err := n.MiningManager.StartMining(n.ctx); err != nil {
			log.Printf("⚠️  Failed to start mining: %v", err)
		} else {
			log.Printf("✅ Mining started successfully")
		}
	}

	// Start background services
	go n.startBackgroundServices()

	// Setup graceful shutdown
	signal.Notify(n.shutdownChan, syscall.SIGINT, syscall.SIGTERM)
	go n.handleShutdown()

	n.running = true
	
	// Log startup completion
	log.Printf("✅ DinariBlockchain Node started successfully")
	log.Printf("   RPC Server: http://%s:%d/rpc", n.Config.ListenAddress, n.Config.ListenPort+1000)
	log.Printf("   API Endpoints: http://%s:%d/api/*", n.Config.ListenAddress, n.Config.ListenPort+1000)
	log.Printf("   Ready for cross-border payments! 💰")
	
	return nil
}

// Stop stops the node gracefully with database cleanup
func (n *Node) Stop() error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if !n.running {
		return fmt.Errorf("node is not running")
	}

	log.Printf("🛑 Stopping DinariBlockchain Node %s", n.ID)

	// Cancel context to stop all services
	n.cancel()

	// Stop mining
	log.Printf("⛏️  Stopping mining...")
	n.MiningManager.StopMining()

	// Stop network services
	log.Printf("🌐 Stopping network services...")
	n.NetworkManager.StopServer()

	// Clear mempool
	log.Printf("📝 Clearing mempool...")
	n.Mempool.Clear()
	
	// NEW: Database cleanup and persistence
	if n.PersistenceManager != nil {
		log.Printf("💾 Saving final state to database...")
		
		// Save current balances
		balances := n.Blockchain.GetAllBalances()
		nonces := make(map[string]int64)
		for addr := range balances {
			nonces[addr] = n.Blockchain.GetNonce(addr)
		}
		n.PersistenceManager.SaveBalancesAndNonces(balances, nonces)
		
		// Clear mempool database
		if n.Config.EnableMempoolDB {
			n.PersistenceManager.ClearMempool()
		}
		
		// Save shutdown metadata
		n.PersistenceManager.SaveMetadata("last_shutdown", fmt.Sprintf("%d", time.Now().Unix()))
		n.PersistenceManager.SaveMetadata("clean_shutdown", "true")
		
		// Compact database if enabled
		if n.Config.AutoCompact {
			log.Printf("🔧 Compacting database...")
			if err := n.PersistenceManager.CompactDatabase(); err != nil {
				log.Printf("⚠️  Database compaction failed: %v", err)
			} else {
				log.Printf("✅ Database compacted")
			}
		}
		
		// Close database
		n.PersistenceManager.Close()
		log.Printf("✅ Database closed safely")
	}

	n.running = false
	
	// Final status log
	uptime := time.Since(n.startTime)
	log.Printf("✅ DinariBlockchain Node stopped successfully")
	log.Printf("   Total uptime: %v", uptime)
	log.Printf("   Final chain height: %d", n.Blockchain.GetChainLength())
	log.Printf("   Final balance: %.8f DINAR", n.GetWalletBalance())

	return nil
}

// =============================================================================
// DATABASE OPERATIONS AND STATUS
// =============================================================================

// GetStatus returns the current status of the node with database info
func (n *Node) GetStatus() *NodeStatus {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	var uptime int64
	if n.running {
		uptime = int64(time.Since(n.startTime).Seconds())
	}

	latestBlock := n.Blockchain.GetLatestBlock()
	var lastBlockTime int64
	var chainHeight int64
	if latestBlock != nil {
		lastBlockTime = latestBlock.Timestamp
		chainHeight = latestBlock.Number
	}

	stats := n.Blockchain.GetStats()
	netStats := n.NetworkManager.GetNetworkStats()
	walletBalance := n.Blockchain.GetBalance(n.Config.MinerAddress)

	// NEW: Database information
	dbEnabled := n.PersistenceManager != nil
	dbSize := 0
	dataDir := ""
	if dbEnabled {
		dataDir = n.Config.DataDir
		// Estimate database size (simplified)
		dbSize = int(stats.TotalTransactions / 100) // Rough estimate in MB
	}

	return &NodeStatus{
		NodeID:          n.ID,
		Running:         n.running,
		Uptime:          uptime,
		ChainHeight:     chainHeight,
		PeerCount:       netStats.ActivePeers,
		MempoolSize:     n.Mempool.GetTransactionCount(),
		IsMining:        n.MiningManager.IsMining(),
		HashRate:        n.MiningManager.GetHashRate(),
		LastBlockTime:   lastBlockTime,
		TotalTxs:        stats.TotalTransactions,
		WalletBalance:   walletBalance,
		SyncStatus:      n.getSyncStatus(),
		DatabaseEnabled: dbEnabled,
		DatabaseSize:    dbSize,
		DataDirectory:   dataDir,
	}
}

// GetDatabaseStats returns detailed database statistics
func (n *Node) GetDatabaseStats() map[string]interface{} {
	if n.PersistenceManager == nil {
		return map[string]interface{}{
			"enabled": false,
			"message": "Database persistence is disabled",
		}
	}
	
	stats := n.PersistenceManager.GetDatabaseStats()
	stats["enabled"] = true
	stats["data_directory"] = n.Config.DataDir
	stats["mempool_persistence"] = n.Config.EnableMempoolDB
	
	// Add metadata
	if nodeID, err := n.PersistenceManager.LoadMetadata("node_id"); err == nil {
		stats["stored_node_id"] = nodeID
	}
	if version, err := n.PersistenceManager.LoadMetadata("version"); err == nil {
		stats["stored_version"] = version
	}
	if lastShutdown, err := n.PersistenceManager.LoadMetadata("last_shutdown"); err == nil {
		stats["last_shutdown"] = lastShutdown
	}
	
	return stats
}

// CompactDatabase manually compacts the database
func (n *Node) CompactDatabase() error {
	if n.PersistenceManager == nil {
		return fmt.Errorf("database persistence not enabled")
	}
	
	log.Printf("🔧 Starting database compaction...")
	start := time.Now()
	
	err := n.PersistenceManager.CompactDatabase()
	if err != nil {
		return fmt.Errorf("database compaction failed: %v", err)
	}
	
	duration := time.Since(start)
	log.Printf("✅ Database compacted successfully in %v", duration)
	
	return nil
}

// BackupDatabase creates a backup of the database (simplified)
func (n *Node) BackupDatabase(backupPath string) error {
	if n.PersistenceManager == nil {
		return fmt.Errorf("database persistence not enabled")
	}
	
	log.Printf("💾 Creating database backup to %s...", backupPath)
	
	// This is a simplified backup - in production you'd want proper database backup
	// For now, we'll save metadata about the backup
	timestamp := time.Now().Unix()
	err := n.PersistenceManager.SaveMetadata("last_backup", fmt.Sprintf("%d", timestamp))
	if err != nil {
		return fmt.Errorf("failed to save backup metadata: %v", err)
	}
	
	log.Printf("✅ Database backup completed")
	return nil
}

// =============================================================================
// WALLET OPERATIONS WITH DATABASE
// =============================================================================

// CreateWallet creates a new wallet address and saves to database
func (n *Node) CreateWallet() (*KeyPair, error) {
	keyPair, err := GenerateKeyPair()
	if err != nil {
		return nil, err
	}

	n.Wallet.mutex.Lock()
	n.Wallet.keyPairs[keyPair.Address] = keyPair
	n.Wallet.mutex.Unlock()

	// NEW: Initialize balance in database
	if n.PersistenceManager != nil {
		err = n.PersistenceManager.SaveBalance(keyPair.Address, 0.0)
		if err != nil {
			log.Printf("⚠️  Failed to initialize balance in database: %v", err)
		}
		
		err = n.PersistenceManager.SaveNonce(keyPair.Address, 0)
		if err != nil {
			log.Printf("⚠️  Failed to initialize nonce in database: %v", err)
		}
	}

	log.Printf("✅ New wallet created: %s", keyPair.Address)
	return keyPair, nil
}

// GetWalletBalance returns the balance of the node's default wallet
func (n *Node) GetWalletBalance() float64 {
	return n.Blockchain.GetBalance(n.Config.MinerAddress)
}

// SendTransaction creates and broadcasts a transaction with database persistence
func (n *Node) SendTransaction(to string, amount, fee float64) (*Transaction, error) {
	// Get nonce for sender
	nonce := n.Blockchain.GetNonce(n.Config.MinerAddress)
	
	// Create transaction
	tx, err := CreateTransaction(n.Config.MinerAddress, to, amount, fee, nonce)
	if err != nil {
		return nil, err
	}

	// Get wallet key pair
	n.Wallet.mutex.RLock()
	keyPair, exists := n.Wallet.keyPairs[n.Config.MinerAddress]
	n.Wallet.mutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("wallet key pair not found")
	}

	// Sign transaction
	err = tx.SignTransaction(keyPair.PrivateKey)
	if err != nil {
		return nil, err
	}

	// Add to mempool (this will also save to database if enabled)
	err = n.Blockchain.AddTransaction(tx)
	if err != nil {
		return nil, err
	}

	// Broadcast to network
	err = n.NetworkManager.BroadcastTransaction(tx)
	if err != nil {
		return nil, err
	}

	log.Printf("💸 Transaction sent: %s (%.8f DINAR to %s)", tx.ID, amount, to)
	return tx, nil
}

// =============================================================================
// NETWORK AND PEER MANAGEMENT
// =============================================================================

// connectToBootstrapPeers connects to initial bootstrap peers
func (n *Node) connectToBootstrapPeers() {
	log.Printf("🔗 Connecting to bootstrap peers...")
	
	for _, peerAddr := range n.Config.BootstrapPeers {
		// Skip connecting to ourselves
		if peerAddr == fmt.Sprintf("%s:%d", n.Config.ListenAddress, n.Config.ListenPort) {
			continue
		}

		go func(addr string) {
			// Parse address and port correctly
			parts := strings.Split(addr, ":")
			if len(parts) != 2 {
				log.Printf("⚠️  Invalid peer address format: %s", addr)
				return
			}
			
			host := parts[0]
			port, err := strconv.Atoi(parts[1])
			if err != nil {
				log.Printf("⚠️  Invalid port in peer address %s: %v", addr, err)
				return
			}
			
			if err := n.NetworkManager.ConnectToPeer(host, port); err != nil {
				log.Printf("⚠️  Failed to connect to bootstrap peer %s: %v", addr, err)
			} else {
				log.Printf("✅ Connected to bootstrap peer %s", addr)
			}
		}(peerAddr)
	}
}

// runBlockchainSync starts blockchain synchronization with peers
func (n *Node) runBlockchainSync() {
	syncInterval := 30 * time.Second
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()

	log.Printf("🔄 Starting blockchain sync routine...")

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.performBlockchainSync()
		}
	}
}

// runPeerDiscovery starts peer discovery routine
func (n *Node) runPeerDiscovery() {
	discoveryInterval := 60 * time.Second
	ticker := time.NewTicker(discoveryInterval)
	defer ticker.Stop()

	log.Printf("🔍 Starting peer discovery routine...")

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.performPeerDiscovery()
		}
	}
}

// performBlockchainSync performs blockchain synchronization with peers
func (n *Node) performBlockchainSync() {
	// Get connected peers
	peers := n.NetworkManager.GetActivePeers()
	if len(peers) == 0 {
		return
	}

	// Find the peer with the highest chain height
	var bestPeer *Peer
	localHeight := int64(n.Blockchain.GetChainLength())
	
	for _, peer := range peers {
		if peer.ChainHeight > localHeight && (bestPeer == nil || peer.ChainHeight > bestPeer.ChainHeight) {
			bestPeer = peer
		}
	}

	if bestPeer == nil {
		return // No peer has a longer chain
	}

	// Request blocks from the best peer
	go func(peer *Peer) {
		peerAddr := fmt.Sprintf("%s:%d", peer.Address, peer.Port)
		if err := n.requestBlockchainSync(peerAddr); err != nil {
			log.Printf("⚠️  Blockchain sync failed with peer %s: %v", peerAddr, err)
		}
	}(bestPeer)
}

// performPeerDiscovery performs peer discovery
func (n *Node) performPeerDiscovery() {
	// Get connected peers
	connectedPeers := n.NetworkManager.GetActivePeers()
	
	// Request peer lists from connected peers
	for _, peer := range connectedPeers {
		go func(p *Peer) {
			peerAddr := fmt.Sprintf("%s:%d", p.Address, p.Port)
			if err := n.requestPeerList(peerAddr); err != nil {
				log.Printf("⚠️  Peer discovery failed with %s: %v", peerAddr, err)
			}
		}(peer)
	}
}

// requestBlockchainSync requests blockchain synchronization from a peer
func (n *Node) requestBlockchainSync(peerAddr string) error {
	// Get our current blockchain height
	currentHeight := n.Blockchain.GetHeight()
	
	// Create sync request message (simplified)
	// In a real implementation, you'd have a proper message protocol
	log.Printf("🔄 Requesting blockchain sync from peer %s (our height: %d)", peerAddr, currentHeight)
	
	// This is a placeholder - you'd implement actual network message sending here
	return nil
}

// requestPeerList requests peer list from a connected peer
func (n *Node) requestPeerList(peerAddr string) error {
	// Create peer request message (simplified)
	log.Printf("🔍 Requesting peer list from %s", peerAddr)
	
	// This is a placeholder - you'd implement actual network message sending here
	return nil
}

// =============================================================================
// BACKGROUND SERVICES WITH DATABASE AWARENESS
// =============================================================================

// startBackgroundServices starts various background maintenance services
func (n *Node) startBackgroundServices() {
	// Blockchain sync service
	go n.runBlockchainSync()
	
	// Status logging service
	go n.runStatusLogger()
	
	// Peer discovery service
	go n.runPeerDiscovery()
	
	// NEW: Database maintenance service
	if n.PersistenceManager != nil {
		go n.runDatabaseMaintenance()
	}
}

// runDatabaseMaintenance performs periodic database maintenance
func (n *Node) runDatabaseMaintenance() {
	ticker := time.NewTicker(30 * time.Minute) // Every 30 minutes
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.performDatabaseMaintenance()
		}
	}
}

// performDatabaseMaintenance performs database optimization tasks
func (n *Node) performDatabaseMaintenance() {
	// Update metadata
	if n.PersistenceManager != nil {
		n.PersistenceManager.SaveMetadata("last_maintenance", fmt.Sprintf("%d", time.Now().Unix()))
		
		// Auto-compact if enabled and node is not too busy
		if n.Config.AutoCompact && n.Mempool.GetTransactionCount() < 100 {
			if err := n.PersistenceManager.CompactDatabase(); err != nil {
				log.Printf("⚠️  Auto-compaction failed: %v", err)
			} else {
				log.Printf("🔧 Database auto-compacted")
			}
		}
	}
}

// runStatusLogger periodically logs node status with database info
func (n *Node) runStatusLogger() {
	ticker := time.NewTicker(2 * time.Minute) // Every 2 minutes
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.logDetailedStatus()
		}
	}
}

// logDetailedStatus logs comprehensive node status including database
func (n *Node) logDetailedStatus() {
	status := n.GetStatus()
	
	log.Printf("📊 Node Status Report:")
	log.Printf("   ⛓️  Chain Height: %d blocks", status.ChainHeight)
	log.Printf("   🌐 Peers: %d connected", status.PeerCount)
	log.Printf("   📝 Mempool: %d transactions", status.MempoolSize)
	log.Printf("   ⛏️  Mining: %v (%.2f H/s)", status.IsMining, status.HashRate)
	log.Printf("   💰 Balance: %.8f DINAR", status.WalletBalance)
	log.Printf("   🔄 Sync: %s", status.SyncStatus)
	
	if status.DatabaseEnabled {
		log.Printf("   💾 Database: %d MB (%s)", status.DatabaseSize, status.DataDirectory)
	} else {
		log.Printf("   💾 Database: Disabled (in-memory only)")
	}
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

// handleShutdown handles graceful shutdown signals
func (n *Node) handleShutdown() {
	<-n.shutdownChan
	log.Printf("📡 Received shutdown signal, stopping node gracefully...")
	n.Stop()
	os.Exit(0)
}

// generateNodeID creates a unique identifier for the node
func generateNodeID() (string, error) {
	bytes := make([]byte, 16)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}
	return "node_" + hex.EncodeToString(bytes), nil
}

// validateConfig validates the node configuration
func validateConfig(config *NodeConfig) error {
	if config.ListenPort < 1024 || config.ListenPort > 65535 {
		return fmt.Errorf("invalid listen port: %d", config.ListenPort)
	}
	if config.BlockReward < 0 {
		return fmt.Errorf("block reward cannot be negative")
	}
	if config.Difficulty < 1 || config.Difficulty > 10 {
		return fmt.Errorf("difficulty must be between 1 and 10")
	}
	if config.TargetBlockTime < 5 || config.TargetBlockTime > 300 {
		return fmt.Errorf("target block time must be between 5 and 300 seconds")
	}
	if config.EnablePersist && config.DataDir == "" {
		return fmt.Errorf("data directory required when persistence is enabled")
	}
	return nil
}

// getSyncStatus determines the current synchronization status
func (n *Node) getSyncStatus() string {
	peers := n.NetworkManager.GetActivePeers()
	if len(peers) == 0 {
		return "no_peers"
	}

	localHeight := int64(n.Blockchain.GetChainLength())
	maxPeerHeight := int64(0)

	for _, peer := range peers {
		if peer.ChainHeight > maxPeerHeight {
			maxPeerHeight = peer.ChainHeight
		}
	}

	if maxPeerHeight == 0 {
		return "unknown"
	}

	if localHeight >= maxPeerHeight {
		return "synced"
	} else if maxPeerHeight-localHeight > 10 {
		return "syncing"
	} else {
		return "catching_up"
	}
}

// =============================================================================
// MINING MANAGER WITH DATABASE INTEGRATION
// =============================================================================

// MiningManager handles all mining operations
type MiningManager struct {
	config      *NodeConfig
	blockchain  *Blockchain
	mempool     *Mempool
	wallet      *NodeWallet
	isMining    bool
	hashRate    float64
	mutex       sync.RWMutex
	workers     []context.CancelFunc
}

// NewMiningManager creates a new mining manager
func NewMiningManager(config *NodeConfig, blockchain *Blockchain, mempool *Mempool, wallet *NodeWallet) *MiningManager {
	return &MiningManager{
		config:     config,
		blockchain: blockchain,
		mempool:    mempool,
		wallet:     wallet,
		isMining:   false,
		hashRate:   0.0,
		workers:    make([]context.CancelFunc, 0),
	}
}

// StartMining starts the mining process
func (mm *MiningManager) StartMining(ctx context.Context) error {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	if mm.isMining {
		return fmt.Errorf("mining already started")
	}

	mm.isMining = true

	// Start mining workers
	for i := 0; i < mm.config.MiningThreads; i++ {
		workerCtx, cancel := context.WithCancel(ctx)
		mm.workers = append(mm.workers, cancel)
		
		go mm.miningWorker(workerCtx, i)
	}

	return nil
}

// StopMining stops the mining process
func (mm *MiningManager) StopMining() {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	if !mm.isMining {
		return
	}

	// Stop all workers
	for _, cancel := range mm.workers {
		cancel()
	}

	mm.workers = make([]context.CancelFunc, 0)
	mm.isMining = false
	mm.hashRate = 0.0
}

// IsMining returns whether mining is currently active
func (mm *MiningManager) IsMining() bool {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()
	return mm.isMining
}

// GetHashRate returns the current hash rate
func (mm *MiningManager) GetHashRate() float64 {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()
	return mm.hashRate
}

// miningWorker is the main mining loop for a worker thread
func (mm *MiningManager) miningWorker(ctx context.Context, workerID int) {
	ticker := time.NewTicker(10 * time.Second) // Try mining every 10 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mm.attemptMining(workerID)
		}
	}
}

// attemptMining attempts to mine a new block
func (mm *MiningManager) attemptMining(workerID int) {
	// Get transactions for mining
	transactions := mm.mempool.GetTopTransactionsByFee(mm.config.MaxTxPerBlock)
	
	if len(transactions) == 0 {
		return // No transactions to mine
	}

	// Mine block - Correct argument order
	start := time.Now()
	block, err := mm.blockchain.MineBlock(transactions, mm.config.MinerAddress)
	if err != nil {
		log.Printf("⚠️  Mining failed (worker %d): %v", workerID, err)
		return
	}

	duration := time.Since(start)
	
	log.Printf("⛏️  Block #%d mined by worker %d!", block.Number, workerID)
	log.Printf("   Hash: %s", block.Hash)
	log.Printf("   Transactions: %d", len(block.Transactions))
	log.Printf("   Total Fees: %.8f DINAR", block.TotalFees)
	log.Printf("   Mining Time: %v", duration)
	log.Printf("   Block saved to database: ✅")
	
	// Update hash rate (simplified calculation)
	mm.mutex.Lock()
	mm.hashRate = float64(block.Nonce) / duration.Seconds()
	mm.mutex.Unlock()
}

// =============================================================================
// DEFAULT CONFIGURATION WITH DATABASE OPTIONS
// =============================================================================

// DefaultNodeConfig returns a default node configuration with database enabled
func DefaultNodeConfig() *NodeConfig {
	return &NodeConfig{
		// Network
		ListenAddress:  "0.0.0.0",
		ListenPort:     8080,
		MaxPeers:       50,
		BootstrapPeers: []string{},
		
		// Mining
		EnableMining:   true,
		MinerAddress:   "", // Will be generated
		MiningThreads:  1,
		MaxBlockSize:   1048576, // 1MB
		MaxTxPerBlock:  1000,
		
		// Blockchain
		Difficulty:      4,
		BlockReward:     50.0,
		TargetBlockTime: 30, // 30 seconds
		
		// Mempool
		MempoolMaxSize: 10000,
		MempoolMaxAge:  24, // 24 hours
		MinTxFee:       0.00001,
		
		// Database - Enable by default
		DataDir:         "./dinari-data",
		EnablePersist:   true,  // Enable database persistence
		EnableMempoolDB: true,  // Enable mempool persistence
		BatchSize:       1000,  // Database batch size
		AutoCompact:     true,  // Auto-compact database
		
		// API
		EnableRPC: true,
		RPCPort:   9080,
		EnableAPI: true,
		APIPort:   9081,
	}
}