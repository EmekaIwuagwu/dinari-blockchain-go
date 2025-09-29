# Complete Windows PowerShell Fix for DinariBlockchain
# Run these commands one by one in PowerShell

# 1. Clean up and start fresh
Write-Host "🧹 Cleaning up..." -ForegroundColor Yellow
Remove-Item go.mod -ErrorAction SilentlyContinue
Remove-Item go.sum -ErrorAction SilentlyContinue

# 2. Initialize Go module properly
Write-Host "📦 Initializing Go module..." -ForegroundColor Green
go mod init dinari-blockchain-go

# 3. Create a working go.mod with correct dependencies
Write-Host "📝 Creating go.mod with working dependencies..." -ForegroundColor Green
@"
module dinari-blockchain-go

go 1.19

require (
    github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7
    github.com/btcsuite/btcd v0.23.0
    github.com/btcsuite/btcutil v1.0.3-0.20201208143702-a53e38424cce
)
"@ | Out-File -FilePath go.mod -Encoding UTF8

# 4. Install the working versions of dependencies
Write-Host "⬇️ Installing LevelDB..." -ForegroundColor Cyan
go get github.com/syndtr/goleveldb@v1.0.1-0.20210819022825-2ae1ddf74ef7

Write-Host "⬇️ Installing Bitcoin libraries..." -ForegroundColor Cyan
go get github.com/btcsuite/btcd@v0.23.0
go get github.com/btcsuite/btcutil@v1.0.3-0.20201208143702-a53e38424cce

# 5. Clean up dependencies
Write-Host "🔧 Organizing dependencies..." -ForegroundColor Magenta
go mod tidy

# 6. Create a simple test file to verify everything works
Write-Host "🧪 Creating test file..." -ForegroundColor Blue
@"
package main

import (
    "fmt"
    "github.com/btcsuite/btcd/btcec/v2"
    "github.com/btcsuite/btcutil/base58"
    "github.com/syndtr/goleveldb/leveldb"
    "os"
)

func main() {
    fmt.Println("🚀 Testing dinari-blockchain-go dependencies...")
    
    // Test crypto
    _, err := btcec.NewPrivateKey()
    if err != nil {
        fmt.Println("❌ Crypto test failed:", err)
        return
    }
    fmt.Println("✅ Crypto library working")
    
    // Test base58
    testData := []byte("Hello DinariBlockchain")
    encoded := base58.Encode(testData)
    fmt.Printf("✅ Base58 encoding working: %s\n", encoded)
    
    // Test database
    db, err := leveldb.OpenFile("./test-db", nil)
    if err != nil {
        fmt.Println("❌ Database test failed:", err)
        return
    }
    
    // Test database operations
    err = db.Put([]byte("test"), []byte("dinari"), nil)
    if err != nil {
        fmt.Println("❌ Database write failed:", err)
        db.Close()
        return
    }
    
    value, err := db.Get([]byte("test"), nil)
    if err != nil || string(value) != "dinari" {
        fmt.Println("❌ Database read failed")
        db.Close()
        return
    }
    
    db.Close()
    os.RemoveAll("./test-db") // Clean up
    fmt.Println("✅ Database library working")
    
    fmt.Println("🎉 All dependencies working for dinari-blockchain-go!")
    fmt.Println("💎 Ready to build your DinariBlockchain!")
}
"@ | Out-File -FilePath test-deps.go -Encoding UTF8

# 7. Test the installation
Write-Host "🧪 Testing dependencies..." -ForegroundColor Yellow
go run test-deps.go

Write-Host ""
Write-Host "=== Your go.mod file ===" -ForegroundColor Green
Get-Content go.mod

Write-Host ""
Write-Host "✅ Installation complete!" -ForegroundColor Green
Write-Host "Now you need to add your blockchain code to the .go files" -ForegroundColor Yellow