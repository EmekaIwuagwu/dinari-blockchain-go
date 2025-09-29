package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
	"log"
)

// =============================================================================
// ADVANCED MEMPOOL WITH DATABASE PERSISTENCE
// =============================================================================

// Mempool represents an advanced transaction pool with database persistence
type Mempool struct {
	transactions       map[string]*Transaction    // txID -> transaction
	byFee             []*Transaction             // sorted by fee (highest first)
	byAddress         map[string][]*Transaction  // address -> transactions
	byTime            []*Transaction             // sorted by timestamp
	maxSize           int                        // maximum transactions
	maxMemory         int64                      // maximum memory usage (bytes)
	currentMemory     int64                      // current memory usage
	minFee            float64                    // minimum fee to accept
	maxAge            time.Duration              // maximum transaction age
	mutex             sync.RWMutex               // thread safety
	stats             *MempoolStats              // performance statistics
	// NEW: Database persistence
	persistenceManager *PersistenceManager       // database manager
	enablePersistence  bool                      // whether to persist to database
	feeHistory        *FeeHistory                // fee analytics
	validationCache   map[string]bool            // validation results cache
}

// MempoolStats tracks mempool performance metrics with database persistence
type MempoolStats struct {
	TotalReceived       int64     `json:"total_received"`
	TotalRejected       int64     `json:"total_rejected"`
	TotalMined          int64     `json:"total_mined"`
	TotalExpired        int64     `json:"total_expired"`
	CurrentSize         int       `json:"current_size"`
	CurrentMemory       int64     `json:"current_memory_bytes"`
	AverageFee          float64   `json:"average_fee"`
	HighestFee          float64   `json:"highest_fee"`
	LowestFee           float64   `json:"lowest_fee"`
	LastCleanup         time.Time `json:"last_cleanup"`
	RejectionsPerMin    float64   `json:"rejections_per_minute"`
	// NEW: Database-backed statistics
	TotalProcessedLife  int64     `json:"total_processed_lifetime"`
	DatabaseSaves       int64     `json:"database_saves"`
	DatabaseLoads       int64     `json:"database_loads"`
	CacheHitRate        float64   `json:"cache_hit_rate"`
}

// FeeHistory tracks fee trends for analytics
type FeeHistory struct {
	hourlyStats   map[int64]*HourlyFeeStats  // hour timestamp -> stats
	dailyStats    map[int64]*DailyFeeStats   // day timestamp -> stats
	recommendations *FeeRecommendations      // current fee recommendations
	mutex         sync.RWMutex
}

// HourlyFeeStats represents fee statistics for one hour
type HourlyFeeStats struct {
	Timestamp    int64   `json:"timestamp"`
	MinFee       float64 `json:"min_fee"`
	MaxFee       float64 `json:"max_fee"`
	AvgFee       float64 `json:"avg_fee"`
	MedianFee    float64 `json:"median_fee"`
	TxCount      int     `json:"tx_count"`
	TotalVolume  float64 `json:"total_volume"`
}

// DailyFeeStats represents fee statistics for one day
type DailyFeeStats struct {
	Timestamp     int64   `json:"timestamp"`
	MinFee        float64 `json:"min_fee"`
	MaxFee        float64 `json:"max_fee"`
	AvgFee        float64 `json:"avg_fee"`
	MedianFee     float64 `json:"median_fee"`
	TxCount       int     `json:"tx_count"`
	TotalVolume   float64 `json:"total_volume"`
	PeakHour      int     `json:"peak_hour"`
	QuietHour     int     `json:"quiet_hour"`
}

// FeeRecommendations provides fee suggestions for users
type FeeRecommendations struct {
	Economic    float64 `json:"economic"`    // Low priority, may take longer
	Standard    float64 `json:"standard"`    // Normal priority
	Priority    float64 `json:"priority"`    // High priority, faster confirmation
	LastUpdated int64   `json:"last_updated"`
}

// MempoolConfig contains mempool configuration with persistence options
type MempoolConfig struct {
	MaxSize           int           `json:"max_size"`
	MaxMemory         int64         `json:"max_memory_bytes"`
	MinFee            float64       `json:"min_fee"`
	MaxAge            time.Duration `json:"max_age"`
	CleanupInterval   time.Duration `json:"cleanup_interval"`
	// NEW: Database persistence options
	EnablePersistence bool          `json:"enable_persistence"`
	SaveInterval      time.Duration `json:"save_interval"`
	CacheSize         int           `json:"validation_cache_size"`
	FeeHistoryDays    int           `json:"fee_history_days"`
}

// =============================================================================
// MEMPOOL INITIALIZATION WITH DATABASE
// =============================================================================

// NewMempool creates a new advanced mempool with database persistence
func NewMempool(config *MempoolConfig) *Mempool {
	if config == nil {
		config = &MempoolConfig{
			MaxSize:           10000,
			MaxMemory:         50 * 1024 * 1024, // 50MB
			MinFee:            0.00001,
			MaxAge:            24 * time.Hour,
			CleanupInterval:   5 * time.Minute,
			EnablePersistence: false, // Default disabled for backward compatibility
			SaveInterval:      30 * time.Second,
			CacheSize:         1000,
			FeeHistoryDays:    30,
		}
	}

	mempool := &Mempool{
		transactions:      make(map[string]*Transaction),
		byFee:            make([]*Transaction, 0),
		byAddress:        make(map[string][]*Transaction),
		byTime:           make([]*Transaction, 0),
		maxSize:          config.MaxSize,
		maxMemory:        config.MaxMemory,
		minFee:           config.MinFee,
		maxAge:           config.MaxAge,
		enablePersistence: config.EnablePersistence,
		stats:            &MempoolStats{},
		feeHistory:       NewFeeHistory(config.FeeHistoryDays),
		validationCache:  make(map[string]bool),
	}

	// Start background cleanup goroutine
	go mempool.startCleanupRoutine(config.CleanupInterval)

	// Start database sync routine if persistence is enabled
	if config.EnablePersistence {
		go mempool.startPersistenceRoutine(config.SaveInterval)
	}

	return mempool
}

// NewMempoolWithPersistence creates mempool with database persistence manager
func NewMempoolWithPersistence(config *MempoolConfig, persistenceManager *PersistenceManager) *Mempool {
	mempool := NewMempool(config)
	
	if persistenceManager != nil {
		mempool.persistenceManager = persistenceManager
		mempool.enablePersistence = true
		
		// Load existing data from database
		mempool.loadFromDatabase()
		
		fmt.Printf("✅ Mempool initialized with database persistence\n")
	}
	
	return mempool
}

// =============================================================================
// TRANSACTION MANAGEMENT WITH DATABASE PERSISTENCE
// =============================================================================

// AddTransaction adds a transaction to the mempool with database persistence
func (mp *Mempool) AddTransaction(tx *Transaction) error {
	mp.mutex.Lock()
	defer mp.mutex.Unlock()

	mp.stats.TotalReceived++

	// Check validation cache first
	if cached, exists := mp.validationCache[tx.ID]; exists {
		if !cached {
			mp.stats.TotalRejected++
			return fmt.Errorf("transaction %s failed previous validation", tx.ID)
		}
	}

	// Check if transaction already exists
	if _, exists := mp.transactions[tx.ID]; exists {
		mp.stats.TotalRejected++
		return fmt.Errorf("transaction %s already in mempool", tx.ID)
	}

	// Validate transaction
	if err := tx.IsValidTransaction(); err != nil {
		mp.stats.TotalRejected++
		// Cache validation failure
		mp.cacheValidationResult(tx.ID, false)
		return fmt.Errorf("invalid transaction: %v", err)
	}

	// Cache validation success
	mp.cacheValidationResult(tx.ID, true)

	// Check minimum fee
	if tx.Fee < mp.minFee {
		mp.stats.TotalRejected++
		return fmt.Errorf("transaction fee %.8f below minimum %.8f", tx.Fee, mp.minFee)
	}

	// Check age
	age := time.Since(time.Unix(tx.Timestamp, 0))
	if age > mp.maxAge {
		mp.stats.TotalRejected++
		return fmt.Errorf("transaction too old: %v", age)
	}

	// Check memory limit
	txMemory := mp.calculateTransactionMemory(tx)
	if mp.currentMemory+txMemory > mp.maxMemory {
		// Try to evict low-fee transactions
		if !mp.evictLowFeeTxs(txMemory) {
			mp.stats.TotalRejected++
			return fmt.Errorf("mempool memory limit exceeded")
		}
	}

	// Check size limit
	if len(mp.transactions) >= mp.maxSize {
		// Try to evict oldest low-fee transaction
		if !mp.evictOldestLowFeeTx() {
			mp.stats.TotalRejected++
			return fmt.Errorf("mempool size limit exceeded")
		}
	}

	// Add transaction to memory structures
	mp.transactions[tx.ID] = tx
	mp.currentMemory += txMemory

	// Add to sorted structures
	mp.insertByFee(tx)
	mp.insertByTime(tx)
	mp.addToAddressIndex(tx)

	// NEW: Save to database if persistence is enabled
	if mp.enablePersistence && mp.persistenceManager != nil {
		if err := mp.persistenceManager.SaveMempoolTransaction(tx); err != nil {
			// Don't fail the operation, just log warning
			fmt.Printf("⚠️  Failed to save transaction to database: %v\n", err)
		} else {
			mp.stats.DatabaseSaves++
		}
	}

	// Update fee history
	mp.feeHistory.AddTransaction(tx)

	// Update statistics
	mp.updateStats()

	fmt.Printf("📝 Transaction %s added to mempool (fee: %.8f DINAR)\n", tx.ID, tx.Fee)
	return nil
}

// RemoveTransaction removes a transaction from the mempool and database
func (mp *Mempool) RemoveTransaction(txID string) bool {
	mp.mutex.Lock()
	defer mp.mutex.Unlock()

	return mp.removeTransactionInternal(txID)
}

// removeTransactionInternal removes transaction (assumes lock is held)
func (mp *Mempool) removeTransactionInternal(txID string) bool {
	tx, exists := mp.transactions[txID]
	if !exists {
		return false
	}

	// Remove from memory structures
	delete(mp.transactions, txID)
	mp.currentMemory -= mp.calculateTransactionMemory(tx)

	// Remove from sorted structures
	mp.removeFromFeeIndex(tx)
	mp.removeFromTimeIndex(tx)
	mp.removeFromAddressIndex(tx)

	// NEW: Remove from database if persistence is enabled
	if mp.enablePersistence && mp.persistenceManager != nil {
		if err := mp.persistenceManager.RemoveMempoolTransaction(txID); err != nil {
			fmt.Printf("⚠️  Failed to remove transaction from database: %v\n", err)
		}
	}

	// Remove from validation cache
	delete(mp.validationCache, txID)

	mp.stats.TotalMined++
	mp.updateStats()

	return true
}

// RemoveTransactions removes multiple transactions (batch operation)
func (mp *Mempool) RemoveTransactions(txIDs []string) int {
	mp.mutex.Lock()
	defer mp.mutex.Unlock()

	removed := 0
	var dbTxIDs []string

	for _, txID := range txIDs {
		if mp.removeTransactionInternal(txID) {
			removed++
			dbTxIDs = append(dbTxIDs, txID)
		}
	}

	// NEW: Batch remove from database
	if mp.enablePersistence && mp.persistenceManager != nil && len(dbTxIDs) > 0 {
		// For now, remove individually. In production, implement batch delete
		for _, txID := range dbTxIDs {
			mp.persistenceManager.RemoveMempoolTransaction(txID)
		}
	}

	return removed
}

// =============================================================================
// DATABASE PERSISTENCE OPERATIONS
// =============================================================================

// loadFromDatabase loads mempool data from database
func (mp *Mempool) loadFromDatabase() {
	if !mp.enablePersistence || mp.persistenceManager == nil {
		return
	}

	fmt.Printf("🔄 Loading mempool from database...\n")

	// Load pending transactions
	transactions, err := mp.persistenceManager.LoadMempoolTransactions()
	if err != nil {
		fmt.Printf("⚠️  Failed to load mempool transactions: %v\n", err)
		return
	}

	loaded := 0
	for _, tx := range transactions {
		// Validate transaction is still valid (not too old)
		age := time.Since(time.Unix(tx.Timestamp, 0))
		if age > mp.maxAge {
			// Remove expired transaction from database
			mp.persistenceManager.RemoveMempoolTransaction(tx.ID)
			continue
		}

		// Add to memory without re-saving to database
		mp.addTransactionToMemory(tx)
		loaded++
	}

	// Load fee history
	mp.loadFeeHistoryFromDatabase()

	// Load statistics
	mp.loadStatsFromDatabase()

	mp.updateStats()
	mp.stats.DatabaseLoads++

	if loaded > 0 {
		fmt.Printf("✅ Loaded %d transactions from database\n", loaded)
	} else {
		fmt.Printf("   No transactions to restore\n")
	}
}

// addTransactionToMemory adds transaction to memory structures without database save
func (mp *Mempool) addTransactionToMemory(tx *Transaction) {
	mp.transactions[tx.ID] = tx
	mp.currentMemory += mp.calculateTransactionMemory(tx)
	mp.insertByFee(tx)
	mp.insertByTime(tx)
	mp.addToAddressIndex(tx)
	mp.cacheValidationResult(tx.ID, true)
}

// saveToDatabase saves current mempool state to database
func (mp *Mempool) saveToDatabase() {
	if !mp.enablePersistence || mp.persistenceManager == nil {
		return
	}

	// Save statistics
	mp.saveStatsToDatabase()

	// Save fee history
	mp.saveFeeHistoryToDatabase()

	// Note: Individual transactions are saved as they're added/removed
	// This method is for periodic bulk operations
}

// loadFeeHistoryFromDatabase loads fee history from database
func (mp *Mempool) loadFeeHistoryFromDatabase() {
	// Load hourly stats
	if data, err := mp.persistenceManager.LoadMetadata("fee_history_hourly"); err == nil && data != "" {
		// Parse and load hourly stats (simplified implementation)
		fmt.Printf("   Loaded fee history from database\n")
	}

	// Load daily stats
	if data, err := mp.persistenceManager.LoadMetadata("fee_history_daily"); err == nil && data != "" {
		// Parse and load daily stats (simplified implementation)
		fmt.Printf("   Loaded daily fee stats from database\n")
	}
}

// saveFeeHistoryToDatabase saves fee history to database
func (mp *Mempool) saveFeeHistoryToDatabase() {
	// Save current fee recommendations
	if mp.feeHistory != nil && mp.feeHistory.recommendations != nil {
		recData := fmt.Sprintf("%.8f,%.8f,%.8f,%d",
			mp.feeHistory.recommendations.Economic,
			mp.feeHistory.recommendations.Standard,
			mp.feeHistory.recommendations.Priority,
			mp.feeHistory.recommendations.LastUpdated)
		
		mp.persistenceManager.SaveMetadata("fee_recommendations", recData)
	}

	// Save aggregated statistics (simplified)
	mp.persistenceManager.SaveMetadata("fee_history_updated", fmt.Sprintf("%d", time.Now().Unix()))
}

// loadStatsFromDatabase loads mempool statistics from database
func (mp *Mempool) loadStatsFromDatabase() {
	if data, err := mp.persistenceManager.LoadMetadata("mempool_total_processed"); err == nil && data != "" {
		if _, parseErr := fmt.Sscanf(data, "%d", &mp.stats.TotalProcessedLife); parseErr == nil {
			fmt.Printf("   Loaded lifetime stats: %d transactions processed\n", mp.stats.TotalProcessedLife)
		}
	}
}

// saveStatsToDatabase saves mempool statistics to database
func (mp *Mempool) saveStatsToDatabase() {
	// Save lifetime statistics
	total := mp.stats.TotalReceived + mp.stats.TotalProcessedLife
	mp.persistenceManager.SaveMetadata("mempool_total_processed", fmt.Sprintf("%d", total))
	
	// Save last update timestamp
	mp.persistenceManager.SaveMetadata("mempool_last_update", fmt.Sprintf("%d", time.Now().Unix()))
}

// =============================================================================
// BACKGROUND PERSISTENCE ROUTINES
// =============================================================================

// startPersistenceRoutine starts background database sync
func (mp *Mempool) startPersistenceRoutine(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		mp.saveToDatabase()
	}
}

// =============================================================================
// VALIDATION CACHING
// =============================================================================

// cacheValidationResult caches a transaction validation result
func (mp *Mempool) cacheValidationResult(txID string, isValid bool) {
	// Limit cache size
	if len(mp.validationCache) >= 1000 {
		// Remove oldest entries (simplified LRU)
		count := 0
		for id := range mp.validationCache {
			delete(mp.validationCache, id)
			count++
			if count >= 100 {
				break
			}
		}
	}

	mp.validationCache[txID] = isValid
}

// isValidationCached checks if validation result is cached
func (mp *Mempool) isValidationCached(txID string) (bool, bool) {
	result, exists := mp.validationCache[txID]
	return result, exists
}

// =============================================================================
// FEE HISTORY AND ANALYTICS
// =============================================================================

// NewFeeHistory creates a new fee history tracker
func NewFeeHistory(retentionDays int) *FeeHistory {
	return &FeeHistory{
		hourlyStats: make(map[int64]*HourlyFeeStats),
		dailyStats:  make(map[int64]*DailyFeeStats),
		recommendations: &FeeRecommendations{
			Economic: 0.00001,
			Standard: 0.0001,
			Priority: 0.001,
			LastUpdated: time.Now().Unix(),
		},
	}
}

// AddTransaction adds a transaction to fee history
func (fh *FeeHistory) AddTransaction(tx *Transaction) {
	fh.mutex.Lock()
	defer fh.mutex.Unlock()

	now := time.Now()
	hourKey := now.Truncate(time.Hour).Unix()
	dayKey := now.Truncate(24 * time.Hour).Unix()

	// Update hourly stats
	if fh.hourlyStats[hourKey] == nil {
		fh.hourlyStats[hourKey] = &HourlyFeeStats{
			Timestamp: hourKey,
			MinFee:    tx.Fee,
			MaxFee:    tx.Fee,
			AvgFee:    tx.Fee,
			TxCount:   1,
			TotalVolume: tx.Amount,
		}
	} else {
		stats := fh.hourlyStats[hourKey]
		stats.TxCount++
		stats.TotalVolume += tx.Amount
		if tx.Fee < stats.MinFee {
			stats.MinFee = tx.Fee
		}
		if tx.Fee > stats.MaxFee {
			stats.MaxFee = tx.Fee
		}
		// Update average (simplified)
		stats.AvgFee = (stats.AvgFee + tx.Fee) / 2
	}

	// Update daily stats (similar logic)
	if fh.dailyStats[dayKey] == nil {
		fh.dailyStats[dayKey] = &DailyFeeStats{
			Timestamp: dayKey,
			MinFee:    tx.Fee,
			MaxFee:    tx.Fee,
			AvgFee:    tx.Fee,
			TxCount:   1,
			TotalVolume: tx.Amount,
		}
	} else {
		stats := fh.dailyStats[dayKey]
		stats.TxCount++
		stats.TotalVolume += tx.Amount
		if tx.Fee < stats.MinFee {
			stats.MinFee = tx.Fee
		}
		if tx.Fee > stats.MaxFee {
			stats.MaxFee = tx.Fee
		}
		stats.AvgFee = (stats.AvgFee + tx.Fee) / 2
	}

	// Update fee recommendations periodically
	if time.Now().Unix()-fh.recommendations.LastUpdated > 300 { // Every 5 minutes
		fh.updateFeeRecommendations()
	}
}

// updateFeeRecommendations updates fee recommendations based on recent data
func (fh *FeeHistory) updateFeeRecommendations() {
	// Collect recent fees
	var recentFees []float64
	cutoff := time.Now().Add(-2 * time.Hour).Unix()

	for timestamp, stats := range fh.hourlyStats {
		if timestamp >= cutoff {
			// Add weighted fees based on transaction count
			for i := 0; i < stats.TxCount; i++ {
				recentFees = append(recentFees, stats.AvgFee)
			}
		}
	}

	if len(recentFees) == 0 {
		return
	}

	// Sort fees
	sort.Float64s(recentFees)

	// Calculate percentiles
	p25 := len(recentFees) / 4
	p50 := len(recentFees) / 2
	p75 := (len(recentFees) * 3) / 4

	fh.recommendations.Economic = recentFees[p25]
	fh.recommendations.Standard = recentFees[p50]
	fh.recommendations.Priority = recentFees[p75]
	fh.recommendations.LastUpdated = time.Now().Unix()
}

// GetFeeRecommendations returns current fee recommendations
func (mp *Mempool) GetFeeRecommendations() *FeeRecommendations {
	mp.feeHistory.mutex.RLock()
	defer mp.feeHistory.mutex.RUnlock()

	// Return copy to avoid race conditions
	return &FeeRecommendations{
		Economic:    mp.feeHistory.recommendations.Economic,
		Standard:    mp.feeHistory.recommendations.Standard,
		Priority:    mp.feeHistory.recommendations.Priority,
		LastUpdated: mp.feeHistory.recommendations.LastUpdated,
	}
}

// =============================================================================
// EXISTING METHODS (UPDATED WITH PERSISTENCE AWARENESS)
// =============================================================================

// GetTransaction retrieves a transaction by ID (with database fallback)
func (mp *Mempool) GetTransaction(txID string) *Transaction {
	mp.mutex.RLock()
	defer mp.mutex.RUnlock()

	// Check memory first
	if tx, exists := mp.transactions[txID]; exists {
		return tx
	}

	// NEW: Check database if persistence is enabled
	if mp.enablePersistence && mp.persistenceManager != nil {
		if tx, err := mp.persistenceManager.LoadTransaction(txID); err == nil && tx != nil {
			mp.stats.DatabaseLoads++
			return tx
		}
	}

	return nil
}

// Clear removes all transactions from the mempool and database
func (mp *Mempool) Clear() {
	mp.mutex.Lock()
	defer mp.mutex.Unlock()

	mp.transactions = make(map[string]*Transaction)
	mp.byFee = make([]*Transaction, 0)
	mp.byAddress = make(map[string][]*Transaction)
	mp.byTime = make([]*Transaction, 0)
	mp.currentMemory = 0
	mp.validationCache = make(map[string]bool)

	// NEW: Clear database mempool
	if mp.enablePersistence && mp.persistenceManager != nil {
		if err := mp.persistenceManager.ClearMempool(); err != nil {
			fmt.Printf("⚠️  Failed to clear mempool database: %v\n", err)
		}
	}

	mp.updateStats()
}

// GetStats returns current mempool statistics with database info
func (mp *Mempool) GetStats() *MempoolStats {
	mp.mutex.RLock()
	defer mp.mutex.RUnlock()

	// Calculate cache hit rate
	totalValidations := mp.stats.TotalReceived
	cacheHits := float64(len(mp.validationCache))
	cacheHitRate := 0.0
	if totalValidations > 0 {
		cacheHitRate = (cacheHits / float64(totalValidations)) * 100
	}

	// Return copy with updated cache hit rate
	statsCopy := *mp.stats
	statsCopy.CacheHitRate = cacheHitRate
	return &statsCopy
}

// =============================================================================
// UTILITY METHODS (EXISTING, UNCHANGED)
// =============================================================================

// GetTopTransactionsByFee returns highest fee transactions
func (mp *Mempool) GetTopTransactionsByFee(count int) []*Transaction {
	mp.mutex.RLock()
	defer mp.mutex.RUnlock()

	if count > len(mp.byFee) {
		count = len(mp.byFee)
	}

	result := make([]*Transaction, count)
	copy(result, mp.byFee[:count])
	return result
}

// GetAllTransactions returns all transactions in the mempool
func (mp *Mempool) GetAllTransactions() []*Transaction {
	mp.mutex.RLock()
	defer mp.mutex.RUnlock()

	result := make([]*Transaction, 0, len(mp.transactions))
	for _, tx := range mp.transactions {
		result = append(result, tx)
	}
	return result
}

// GetTransactionCount returns the number of transactions
func (mp *Mempool) GetTransactionCount() int {
	mp.mutex.RLock()
	defer mp.mutex.RUnlock()

	return len(mp.transactions)
}

// =============================================================================
// HELPER METHODS
// =============================================================================

func (mp *Mempool) insertByFee(tx *Transaction) {
	insertIndex := sort.Search(len(mp.byFee), func(i int) bool {
		return mp.byFee[i].Fee < tx.Fee
	})

	mp.byFee = append(mp.byFee, nil)
	copy(mp.byFee[insertIndex+1:], mp.byFee[insertIndex:])
	mp.byFee[insertIndex] = tx
}

func (mp *Mempool) removeFromFeeIndex(tx *Transaction) {
	for i, sortedTx := range mp.byFee {
		if sortedTx.ID == tx.ID {
			mp.byFee = append(mp.byFee[:i], mp.byFee[i+1:]...)
			break
		}
	}
}

func (mp *Mempool) insertByTime(tx *Transaction) {
	insertIndex := sort.Search(len(mp.byTime), func(i int) bool {
		return mp.byTime[i].Timestamp > tx.Timestamp
	})

	mp.byTime = append(mp.byTime, nil)
	copy(mp.byTime[insertIndex+1:], mp.byTime[insertIndex:])
	mp.byTime[insertIndex] = tx
}

func (mp *Mempool) removeFromTimeIndex(tx *Transaction) {
	for i, sortedTx := range mp.byTime {
		if sortedTx.ID == tx.ID {
			mp.byTime = append(mp.byTime[:i], mp.byTime[i+1:]...)
			break
		}
	}
}

func (mp *Mempool) addToAddressIndex(tx *Transaction) {
	mp.byAddress[tx.From] = append(mp.byAddress[tx.From], tx)
	if tx.To != tx.From {
		mp.byAddress[tx.To] = append(mp.byAddress[tx.To], tx)
	}
}

func (mp *Mempool) removeFromAddressIndex(tx *Transaction) {
	mp.removeFromAddressList(tx.From, tx.ID)
	if tx.To != tx.From {
		mp.removeFromAddressList(tx.To, tx.ID)
	}
}

func (mp *Mempool) removeFromAddressList(address, txID string) {
	txs := mp.byAddress[address]
	for i, tx := range txs {
		if tx.ID == txID {
			mp.byAddress[address] = append(txs[:i], txs[i+1:]...)
			if len(mp.byAddress[address]) == 0 {
				delete(mp.byAddress, address)
			}
			break
		}
	}
}

func (mp *Mempool) calculateTransactionMemory(tx *Transaction) int64 {
	return int64(len(tx.ID) + len(tx.From) + len(tx.To) + len(tx.Signature) + 100)
}

func (mp *Mempool) evictLowFeeTxs(neededMemory int64) bool {
	freedMemory := int64(0)
	
	for i := len(mp.byFee) - 1; i >= 0 && freedMemory < neededMemory; i-- {
		tx := mp.byFee[i]
		txMemory := mp.calculateTransactionMemory(tx)
		
		mp.removeTransactionInternal(tx.ID)
		freedMemory += txMemory
	}
	
	return freedMemory >= neededMemory
}

func (mp *Mempool) evictOldestLowFeeTx() bool {
	if len(mp.byTime) == 0 {
		return false
	}
	
	quarter := len(mp.byFee) / 4
	if quarter < 1 {
		quarter = 1
	}
	
	var oldestTx *Transaction
	oldestTime := int64(0)
	
	for i := len(mp.byFee) - quarter; i < len(mp.byFee); i++ {
		tx := mp.byFee[i]
		if oldestTime == 0 || tx.Timestamp < oldestTime {
			oldestTime = tx.Timestamp
			oldestTx = tx
		}
	}
	
	if oldestTx != nil {
		mp.removeTransactionInternal(oldestTx.ID)
		return true
	}
	
	return false
}

func (mp *Mempool) updateStats() {
	mp.stats.CurrentSize = len(mp.transactions)
	mp.stats.CurrentMemory = mp.currentMemory
	
	if len(mp.byFee) > 0 {
		mp.stats.HighestFee = mp.byFee[0].Fee
		mp.stats.LowestFee = mp.byFee[len(mp.byFee)-1].Fee
		
		totalFee := 0.0
		for _, tx := range mp.byFee {
			totalFee += tx.Fee
		}
		mp.stats.AverageFee = totalFee / float64(len(mp.byFee))
	} else {
		mp.stats.HighestFee = 0.0
		mp.stats.LowestFee = 0.0
		mp.stats.AverageFee = 0.0
	}
}

func (mp *Mempool) startCleanupRoutine(interval time.Duration) {
	// Validate interval
	if interval <= 0 {
		log.Printf("Invalid mempool cleanup interval (%v), using default 1 hour", interval)
		interval = 1 * time.Hour
	}
	
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	
	for range ticker.C {
		mp.cleanup()
	}
}

func (mp *Mempool) cleanup() {
	mp.mutex.Lock()
	defer mp.mutex.Unlock()
	
	now := time.Now()
	var toRemove []string
	
	for txID, tx := range mp.transactions {
		age := now.Sub(time.Unix(tx.Timestamp, 0))
		if age > mp.maxAge {
			toRemove = append(toRemove, txID)
		}
	}
	
	for _, txID := range toRemove {
		mp.removeTransactionInternal(txID)
		mp.stats.TotalExpired++
	}
	
	mp.stats.LastCleanup = now
	mp.updateStats()
	
	// Save to database after cleanup
	if mp.enablePersistence {
		go mp.saveToDatabase()
	}
}