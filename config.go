// =============================================================================
// CONFIG.GO - Genesis Configuration & Initial Setup
// =============================================================================

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"
)

// =============================================================================
// GENESIS CONFIGURATION
// =============================================================================

// GenesisConfig defines the initial blockchain state
type GenesisConfig struct {
	Timestamp      int64              `json:"timestamp"`
	Difficulty     int                `json:"difficulty"`
	ChainID        string             `json:"chain_id"`
	NetworkID      int                `json:"network_id"`
	BlockReward    float64            `json:"block_reward"`
	ExtraData      string             `json:"extra_data"`
	Allocations    map[string]float64 `json:"allocations"`     // Pre-allocated balances
	TreasuryConfig TreasuryGenesis    `json:"treasury_config"`
	AdminAddresses []string           `json:"admin_addresses"` // Initial admins
	MinterAddresses []string          `json:"minter_addresses"` // Initial minters
}

// TreasuryGenesis holds initial treasury configuration
type TreasuryGenesis struct {
	Address       string  `json:"address"`
	InitialSupply float64 `json:"initial_supply"`
	MaxSupply     float64 `json:"max_supply"`
}

// LoadGenesisConfig loads genesis configuration from JSON file
func LoadGenesisConfig(path string) (*GenesisConfig, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read genesis file: %v", err)
	}
	
	var config GenesisConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse genesis JSON: %v", err)
	}
	
	// Validate genesis config
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid genesis config: %v", err)
	}
	
	return &config, nil
}

// Validate checks if genesis configuration is valid
func (gc *GenesisConfig) Validate() error {
	if gc.Difficulty < 1 || gc.Difficulty > 20 {
		return fmt.Errorf("difficulty must be between 1 and 20")
	}
	
	if gc.BlockReward < 0 {
		return fmt.Errorf("block reward cannot be negative")
	}
	
	if gc.TreasuryConfig.InitialSupply > gc.TreasuryConfig.MaxSupply {
		return fmt.Errorf("initial supply cannot exceed max supply")
	}
	
	if gc.TreasuryConfig.MaxSupply <= 0 {
		return fmt.Errorf("max supply must be positive")
	}
	
	// Validate all allocation addresses
	for addr := range gc.Allocations {
		if !ValidateAddress(addr) {
			return fmt.Errorf("invalid allocation address: %s", addr)
		}
	}
	
	return nil
}

// SaveGenesisConfig saves genesis configuration to file
func (gc *GenesisConfig) SaveToFile(path string) error {
	data, err := json.MarshalIndent(gc, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal genesis config: %v", err)
	}
	
	if err := ioutil.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write genesis file: %v", err)
	}
	
	return nil
}

// =============================================================================
// GENESIS BLOCK CREATION FROM CONFIG
// =============================================================================

// CreateGenesisFromConfig creates a genesis block with config
func CreateGenesisFromConfig(config *GenesisConfig) *Block {
	genesisBlock := &Block{
		Number:       0,
		Timestamp:    config.Timestamp,
		Transactions: []*Transaction{},
		PreviousHash: "0",
		Nonce:        0,
		Miner:        "GENESIS",
		Reward:       0,
		TotalFees:    0,
	}
	
	return genesisBlock
}

// ApplyGenesisAllocations applies pre-allocated balances to blockchain
func ApplyGenesisAllocations(bc *Blockchain, config *GenesisConfig) error {
	log.Printf("📋 Applying genesis allocations...")
	
	bc.mutex.Lock()
	defer bc.mutex.Unlock()
	
	// Apply treasury configuration
	bc.Treasury.TreasuryAddress = config.TreasuryConfig.Address
	bc.Treasury.InitialSupply = config.TreasuryConfig.InitialSupply
	bc.Treasury.MaxSupply = config.TreasuryConfig.MaxSupply
	
	// Set treasury balance
	bc.balanceCache[config.TreasuryConfig.Address] = config.TreasuryConfig.InitialSupply
	
	// Apply all allocations
	for address, amount := range config.Allocations {
		bc.balanceCache[address] = amount
		log.Printf("   ✅ Allocated %.2f DINARI to %s", amount, address)
	}
	
	// Set up admin addresses
	for _, admin := range config.AdminAddresses {
		bc.Treasury.AdminAuthority[admin] = true
		log.Printf("   👑 Granted admin privileges to %s", admin)
	}
	
	// Set up minter addresses
	for _, minter := range config.MinterAddresses {
		bc.Treasury.MintAuthority[minter] = true
		log.Printf("   💰 Granted mint authority to %s", minter)
	}
	
	// Save to database if available
	if bc.persistenceManager != nil {
		bc.saveBalancesToDatabase()
	}
	
	bc.cacheValid = true
	log.Printf("✅ Genesis allocations applied successfully")
	
	return nil
}

// =============================================================================
// DEFAULT GENESIS CONFIGURATIONS
// =============================================================================

// DefaultGenesisConfig returns a default genesis configuration
func DefaultGenesisConfig() *GenesisConfig {
	return &GenesisConfig{
		Timestamp:   time.Now().Unix(),
		Difficulty:  4,
		ChainID:     "dinari-mainnet-1",
		NetworkID:   1,
		BlockReward: 50.0,
		ExtraData:   "DinariBlockchain Genesis Block - Cross-Border Payments",
		TreasuryConfig: TreasuryGenesis{
			Address:       "DTTreasury000000000000000000",
			InitialSupply: 1000000000.0, // 1 billion DINARI
			MaxSupply:     10000000000.0, // 10 billion max
		},
		Allocations: map[string]float64{
			"DTFoundation123456789012345": 100000000.0, // Foundation: 100M
			"DTDevelopment012345678901234": 50000000.0,  // Development: 50M
			"DTMarketing901234567890123456": 25000000.0,  // Marketing: 25M
		},
		AdminAddresses: []string{
			"DTAdmin1234567890123456789012", // Primary admin
		},
		MinterAddresses: []string{
			"DTMinter123456789012345678901", // Authorized minter
		},
	}
}

// TestnetGenesisConfig returns testnet genesis configuration
func TestnetGenesisConfig() *GenesisConfig {
	return &GenesisConfig{
		Timestamp:   time.Now().Unix(),
		Difficulty:  2, // Lower difficulty for testnet
		ChainID:     "dinari-testnet-1",
		NetworkID:   2,
		BlockReward: 100.0, // Higher rewards for testing
		ExtraData:   "DinariBlockchain Testnet Genesis",
		TreasuryConfig: TreasuryGenesis{
			Address:       "DTTreasury000000000000000000",
			InitialSupply: 100000000.0, // 100M for testnet
			MaxSupply:     1000000000.0, // 1B max
		},
		Allocations: map[string]float64{
			"DTTest1234567890123456789012": 10000000.0,
		},
		AdminAddresses: []string{
			"DTTestAdmin123456789012345678",
		},
		MinterAddresses: []string{
			"DTTestMinter12345678901234567",
		},
	}
}

// =============================================================================
// CONFIGURATION FILE MANAGEMENT
// =============================================================================

// InitializeGenesisFile creates a genesis.json file if it doesn't exist
func InitializeGenesisFile(path string, testnet bool) error {
	// Check if file already exists
	if _, err := os.Stat(path); err == nil {
		log.Printf("Genesis file already exists at %s", path)
		return nil
	}
	
	var config *GenesisConfig
	if testnet {
		config = TestnetGenesisConfig()
		log.Printf("Creating testnet genesis configuration...")
	} else {
		config = DefaultGenesisConfig()
		log.Printf("Creating mainnet genesis configuration...")
	}
	
	if err := config.SaveToFile(path); err != nil {
		return fmt.Errorf("failed to create genesis file: %v", err)
	}
	
	log.Printf("✅ Genesis file created at %s", path)
	return nil
}

// LoadOrCreateGenesis loads existing genesis or creates default
func LoadOrCreateGenesis(path string, testnet bool) (*GenesisConfig, error) {
	// Try to load existing genesis
	config, err := LoadGenesisConfig(path)
	if err == nil {
		log.Printf("✅ Loaded genesis configuration from %s", path)
		return config, nil
	}
	
	// If file doesn't exist, create it
	if os.IsNotExist(err) {
		log.Printf("Genesis file not found, creating default...")
		if err := InitializeGenesisFile(path, testnet); err != nil {
			return nil, err
		}
		return LoadGenesisConfig(path)
	}
	
	return nil, err
}

// =============================================================================
// GENESIS EXAMPLE FOR DOCUMENTATION
// =============================================================================

// ExampleGenesisJSON returns an example genesis.json for documentation
func ExampleGenesisJSON() string {
	config := DefaultGenesisConfig()
	data, _ := json.MarshalIndent(config, "", "  ")
	return string(data)
}

// PrintGenesisExample prints an example genesis configuration
func PrintGenesisExample() {
	fmt.Println("Example genesis.json configuration:")
	fmt.Println("=====================================")
	fmt.Println(ExampleGenesisJSON())
	fmt.Println("=====================================")
	fmt.Println()
	fmt.Println("Save this to genesis.json and customize as needed.")
	fmt.Println("Treasury address will hold the initial token supply.")
	fmt.Println("Allocations distribute tokens to specific addresses at genesis.")
	fmt.Println("Admin addresses can manage permissions.")
	fmt.Println("Minter addresses can mint new tokens from treasury.")
}

// =============================================================================
// CONFIGURATION VALIDATION
// =============================================================================

// ValidateNodeConfig validates complete node configuration
func ValidateNodeConfig(nodeConfig *NodeConfig, genesisConfig *GenesisConfig) error {
	// Validate node config
	if nodeConfig.ListenPort < 1024 || nodeConfig.ListenPort > 65535 {
		return fmt.Errorf("invalid listen port: %d", nodeConfig.ListenPort)
	}
	
	// Validate genesis config
	if err := genesisConfig.Validate(); err != nil {
		return fmt.Errorf("invalid genesis config: %v", err)
	}
	
	// Ensure treasury address is valid
	if !ValidateAddress(genesisConfig.TreasuryConfig.Address) {
		return fmt.Errorf("invalid treasury address in genesis")
	}
	
	// Validate that total allocations don't exceed initial supply
	totalAllocated := 0.0
	for _, amount := range genesisConfig.Allocations {
		totalAllocated += amount
	}
	
	if totalAllocated > genesisConfig.TreasuryConfig.InitialSupply {
		return fmt.Errorf("total allocations (%.2f) exceed initial supply (%.2f)", 
			totalAllocated, genesisConfig.TreasuryConfig.InitialSupply)
	}
	
	return nil
}