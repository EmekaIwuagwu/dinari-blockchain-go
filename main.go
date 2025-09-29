package main

import (
    "fmt"
    "log"
)

func main() {
    fmt.Println("🚀 Starting DinariBlockchain...")
    
    // Create node configuration
    config := DefaultNodeConfig()
    config.ListenPort = 8080
    config.DataDir = "./dinari-data"
    config.EnablePersist = true
    
    // Create and start node
    node, err := NewNode(config)
    if err != nil {
        log.Fatal("Failed to create node:", err)
    }
    
    // Start the blockchain node
    err = node.Start()
    if err != nil {
        log.Fatal("Failed to start node:", err)
    }
    
    fmt.Println("✅ DinariBlockchain is running!")
    fmt.Println("Press Ctrl+C to stop...")
    
    // Keep running
    select {}
}