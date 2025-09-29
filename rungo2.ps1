# 1. Clean start
Remove-Item go.mod -ErrorAction SilentlyContinue
Remove-Item go.sum -ErrorAction SilentlyContinue

# 2. Initialize properly
go mod init dinari-blockchain-go

# 3. Install working versions
go get github.com/syndtr/goleveldb@v1.0.1-0.20210819022825-2ae1ddf74ef7
go get github.com/btcsuite/btcd@v0.23.0
go get github.com/btcsuite/btcutil@v1.0.3-0.20201208143702-a53e38424cce

# 4. Clean up
go mod tidy