package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcutil/base58"
)

// =============================================================================
// CRYPTOGRAPHY & ADDRESS GENERATION (FIXED FOR MODERN LIBRARIES)
// =============================================================================

// KeyPair represents a public/private key pair for blockchain operations
type KeyPair struct {
	PrivateKey *btcec.PrivateKey
	PublicKey  *btcec.PublicKey
	Address    string
}

// GenerateKeyPair creates a new secp256k1 key pair and derives the DT address
func GenerateKeyPair() (*KeyPair, error) {
	// Updated method call for modern btcec library
	privateKey, err := btcec.NewPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("failed to generate private key: %v", err)
	}

	publicKey := privateKey.PubKey()
	address := GenerateAddress(publicKey)

	return &KeyPair{
		PrivateKey: privateKey,
		PublicKey:  publicKey,
		Address:    address,
	}, nil
}

// GenerateAddress creates a DT-prefixed address from a public key
// Format: "DT" + base58(sha256(compressed_pubkey)[:20])
func GenerateAddress(pubKey *btcec.PublicKey) string {
	pubKeyBytes := pubKey.SerializeCompressed()
	hash := sha256.Sum256(pubKeyBytes)
	payload := hash[:20]
	encodedAddress := base58.Encode(payload)
	return "DT" + encodedAddress
}

// GetPrivateKeyHex returns the private key as a hex string for wallet export
func (kp *KeyPair) GetPrivateKeyHex() string {
	privateKeyBytes := kp.PrivateKey.Serialize()
	return hex.EncodeToString(privateKeyBytes)
}

// GetPublicKeyHex returns the public key as a hex string
func (kp *KeyPair) GetPublicKeyHex() string {
	publicKeyBytes := kp.PublicKey.SerializeCompressed()
	return hex.EncodeToString(publicKeyBytes)
}

// ImportPrivateKey imports a private key from hex string and recreates the KeyPair
func ImportPrivateKey(privateKeyHex string) (*KeyPair, error) {
	privateKeyBytes, err := hex.DecodeString(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("invalid private key hex: %v", err)
	}

	// Fixed: Updated method for modern btcec library (handle both return values)
	privateKey, _ := btcec.PrivKeyFromBytes(privateKeyBytes)
	publicKey := privateKey.PubKey()
	address := GenerateAddress(publicKey)

	return &KeyPair{
		PrivateKey: privateKey,
		PublicKey:  publicKey,
		Address:    address,
	}, nil
}

// ValidateAddress checks if an address has valid DT prefix and format
func ValidateAddress(address string) bool {
	if len(address) < 3 || address[:2] != "DT" {
		return false
	}

	base58Part := address[2:]
	decoded := base58.Decode(base58Part)
	return len(decoded) == 20
}

// HashData creates a SHA256 hash of any data
func HashData(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// RecoverPublicKeyFromSignature recovers public key from signature and message
func RecoverPublicKeyFromSignature(message, signature string) (*btcec.PublicKey, error) {
	// Simplified implementation - full recovery needs additional data
	return nil, fmt.Errorf("public key recovery not fully implemented")
}