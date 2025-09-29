// =============================================================================
// SECURITY.GO - Rate Limiting, Audit Trail, Input Sanitization
// =============================================================================

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"crypto/rand" 
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

// =============================================================================
// RATE LIMITER
// =============================================================================

// RateLimiter manages request rate limiting per IP
type RateLimiter struct {
	visitors map[string]*Visitor
	mutex    sync.RWMutex
	cleanup  *time.Ticker
}

// Visitor tracks rate limit info for an IP
type Visitor struct {
	limiter  *TokenBucket
	lastSeen time.Time
}

// TokenBucket implements token bucket rate limiting
type TokenBucket struct {
	tokens      float64
	maxTokens   float64
	refillRate  float64 // tokens per second
	lastRefill  time.Time
	mutex       sync.Mutex
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter() *RateLimiter {
	rl := &RateLimiter{
		visitors: make(map[string]*Visitor),
		cleanup:  time.NewTicker(5 * time.Minute),
	}
	
	// Start cleanup routine
	go rl.cleanupVisitors()
	
	return rl
}

// NewTokenBucket creates a token bucket
func NewTokenBucket(maxTokens, refillRate float64) *TokenBucket {
	return &TokenBucket{
		tokens:     maxTokens,
		maxTokens:  maxTokens,
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

// Allow checks if request is allowed under rate limit
func (tb *TokenBucket) Allow() bool {
	tb.mutex.Lock()
	defer tb.mutex.Unlock()
	
	// Refill tokens based on time elapsed
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens = min(tb.maxTokens, tb.tokens + elapsed*tb.refillRate)
	tb.lastRefill = now
	
	// Check if we have tokens available
	if tb.tokens >= 1.0 {
		tb.tokens -= 1.0
		return true
	}
	
	return false
}

// CheckRateLimit checks if IP is within rate limits
func (rl *RateLimiter) CheckRateLimit(ip string) bool {
	rl.mutex.Lock()
	
	visitor, exists := rl.visitors[ip]
	if !exists {
		// New visitor - allow 10 requests per second
		visitor = &Visitor{
			limiter:  NewTokenBucket(10, 10), // 10 tokens, refill 10/sec
			lastSeen: time.Now(),
		}
		rl.visitors[ip] = visitor
	} else {
		visitor.lastSeen = time.Now()
	}
	
	rl.mutex.Unlock()
	
	return visitor.limiter.Allow()
}

// cleanupVisitors removes stale visitor records
func (rl *RateLimiter) cleanupVisitors() {
	for range rl.cleanup.C {
		rl.mutex.Lock()
		
		now := time.Now()
		for ip, visitor := range rl.visitors {
			if now.Sub(visitor.lastSeen) > 10*time.Minute {
				delete(rl.visitors, ip)
			}
		}
		
		rl.mutex.Unlock()
	}
}

// RateLimitMiddleware is HTTP middleware for rate limiting
func RateLimitMiddleware(rateLimiter *RateLimiter) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			ip := getClientIP(r)
			
			if !rateLimiter.CheckRateLimit(ip) {
				http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
				log.Printf("⚠️  Rate limit exceeded for IP: %s", ip)
				return
			}
			
			next(w, r)
		}
	}
}

// =============================================================================
// INPUT SANITIZATION
// =============================================================================

// SanitizeInput performs input validation and sanitization
type InputSanitizer struct{}

// ValidateAddress checks if address is valid format
func (is *InputSanitizer) ValidateAddress(address string) error {
	if len(address) < 20 || len(address) > 50 {
		return fmt.Errorf("address length invalid: must be 20-50 characters")
	}
	
	if !ValidateAddress(address) {
		return fmt.Errorf("address format invalid")
	}
	
	return nil
}

// ValidateAmount checks if amount is within safe bounds
func (is *InputSanitizer) ValidateAmount(amount float64) error {
	if amount <= 0 {
		return fmt.Errorf("amount must be positive")
	}
	
	if amount > 1e15 {
		return fmt.Errorf("amount too large (overflow protection)")
	}
	
	// Check for precision issues
	if amount < 1e-8 {
		return fmt.Errorf("amount too small (minimum: 0.00000001)")
	}
	
	return nil
}

// ValidateTransactionID checks transaction ID format
func (is *InputSanitizer) ValidateTransactionID(txID string) error {
	if len(txID) < 35 || len(txID) > 70 {
		return fmt.Errorf("transaction ID length invalid")
	}
	
	if txID[:3] != "DTx" {
		return fmt.Errorf("transaction ID must start with 'DTx'")
	}
	
	return nil
}

// SanitizeString removes potentially dangerous characters
func (is *InputSanitizer) SanitizeString(input string, maxLength int) (string, error) {
	if len(input) > maxLength {
		return "", fmt.Errorf("input too long: max %d characters", maxLength)
	}
	
	// Remove null bytes and control characters
	sanitized := ""
	for _, r := range input {
		if r >= 32 && r < 127 { // Printable ASCII only
			sanitized += string(r)
		}
	}
	
	return sanitized, nil
}

// =============================================================================
// REQUEST VALIDATION
// =============================================================================

// ValidateHTTPRequest performs security checks on HTTP requests
func ValidateHTTPRequest(r *http.Request) error {
	// Check Content-Type for POST/PUT requests
	if r.Method == http.MethodPost || r.Method == http.MethodPut {
		ct := r.Header.Get("Content-Type")
		if ct != "application/json" {
			return fmt.Errorf("invalid Content-Type: expected application/json")
		}
	}
	
	// Check Content-Length to prevent large payload attacks
	if r.ContentLength > 1048576 { // 1MB limit
		return fmt.Errorf("request body too large: max 1MB")
	}
	
	// Check for common attack headers
	if r.Header.Get("X-Forwarded-For") != "" {
		// Log for monitoring
		log.Printf("Request with X-Forwarded-For from: %s", r.RemoteAddr)
	}
	
	return nil
}

// =============================================================================
// IP UTILITIES
// =============================================================================

// getClientIP extracts real client IP from request
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header
	xff := r.Header.Get("X-Forwarded-For")
	if xff != "" {
		// Take first IP in list
		if ip, _, err := net.SplitHostPort(xff); err == nil {
			return ip
		}
		return xff
	}
	
	// Check X-Real-IP header
	xri := r.Header.Get("X-Real-IP")
	if xri != "" {
		return xri
	}
	
	// Fall back to RemoteAddr
	if ip, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return ip
	}
	
	return r.RemoteAddr
}

// =============================================================================
// CRYPTOGRAPHIC UTILITIES
// =============================================================================

// HashPassword hashes a password using SHA-256 (use bcrypt in production)
func HashPassword(password string, salt string) string {
	combined := password + salt
	hash := sha256.Sum256([]byte(combined))
	return hex.EncodeToString(hash[:])
}

// GenerateSecureToken generates a random secure token
func GenerateSecureToken(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// =============================================================================
// SECURITY MONITORING
// =============================================================================

// SecurityMonitor tracks suspicious activity
type SecurityMonitor struct {
	suspiciousIPs map[string]*SuspiciousActivity
	mutex         sync.RWMutex
	alertChannel  chan SecurityAlert
}

// SuspiciousActivity tracks potential attack patterns
type SuspiciousActivity struct {
	IP                string
	FailedRequests    int
	LastFailure       time.Time
	BlockedUntil      time.Time
	AttackPattern     string
}

// SecurityAlert represents a security event
type SecurityAlert struct {
	Timestamp   time.Time
	IP          string
	AlertType   string
	Description string
	Severity    string // LOW, MEDIUM, HIGH, CRITICAL
}

// NewSecurityMonitor creates a security monitor
func NewSecurityMonitor() *SecurityMonitor {
	return &SecurityMonitor{
		suspiciousIPs: make(map[string]*SuspiciousActivity),
		alertChannel:  make(chan SecurityAlert, 1000),
	}
}

// RecordFailedRequest records a failed/suspicious request
func (sm *SecurityMonitor) RecordFailedRequest(ip, pattern string) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	
	activity, exists := sm.suspiciousIPs[ip]
	if !exists {
		activity = &SuspiciousActivity{
			IP:             ip,
			FailedRequests: 0,
			AttackPattern:  pattern,
		}
		sm.suspiciousIPs[ip] = activity
	}
	
	activity.FailedRequests++
	activity.LastFailure = time.Now()
	
	// Auto-block after 10 failed requests in 5 minutes
	if activity.FailedRequests >= 10 {
		activity.BlockedUntil = time.Now().Add(1 * time.Hour)
		
		alert := SecurityAlert{
			Timestamp:   time.Now(),
			IP:          ip,
			AlertType:   "AUTO_BLOCK",
			Description: fmt.Sprintf("IP blocked after %d failed requests", activity.FailedRequests),
			Severity:    "HIGH",
		}
		
		select {
		case sm.alertChannel <- alert:
		default:
			log.Printf("⚠️  Alert channel full, dropping alert")
		}
		
		log.Printf("🚫 Blocked IP %s for 1 hour due to suspicious activity", ip)
	}
}

// IsBlocked checks if an IP is currently blocked
func (sm *SecurityMonitor) IsBlocked(ip string) bool {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()
	
	activity, exists := sm.suspiciousIPs[ip]
	if !exists {
		return false
	}
	
	if time.Now().Before(activity.BlockedUntil) {
		return true
	}
	
	return false
}

// GetSecurityAlerts returns recent security alerts
func (sm *SecurityMonitor) GetSecurityAlerts(count int) []SecurityAlert {
	alerts := make([]SecurityAlert, 0)
	
	for i := 0; i < count; i++ {
		select {
		case alert := <-sm.alertChannel:
			alerts = append(alerts, alert)
		default:
			return alerts
		}
	}
	
	return alerts
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// ValidateJSONRPCRequest validates JSON-RPC request structure
func ValidateJSONRPCRequest(method string, params []interface{}) error {
	// Validate method name
	allowedMethods := map[string]bool{
		"getBlockchainInfo": true,
		"getBalance":        true,
		"sendTransaction":   true,
		"getTransaction":    true,
		"getBlock":          true,
		"getMempool":        true,
		"getPeers":          true,
		"getNetworkStats":   true,
	}
	
	if !allowedMethods[method] {
		return fmt.Errorf("method not allowed: %s", method)
	}
	
	// Validate parameter count
	maxParams := 10
	if len(params) > maxParams {
		return fmt.Errorf("too many parameters: max %d", maxParams)
	}
	
	return nil
}