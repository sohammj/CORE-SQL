#include "concurrency.h"
#include "Utils.h"
#include <algorithm>
#include <iostream>

bool LockManager::acquireLock(int transactionId, const std::string& resourceName,
                             ResourceType resourceType, LockMode mode) {
    // Acquire exclusive lock on the lock table
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    // Create the lock request
    LockRequest request(transactionId, resourceName, resourceType, mode);
    
    // Check if resource already has locks
    auto it = lockTable.find(resourceName);
    if (it == lockTable.end()) {
        // No existing locks, grant immediately
        request.granted = true;
        lockTable[resourceName].push_back(request);
        return true;
    }
    
    // Check if transaction already holds a lock on this resource
    auto& locks = it->second;
    for (auto& existingLock : locks) {
        if (existingLock.transactionId == transactionId) {
            // Transaction already has a lock, check for upgrade
            if (existingLock.mode == mode) {
                // Already has the requested lock mode
                return true;
            } else if (existingLock.mode == LockMode::SHARED && mode == LockMode::EXCLUSIVE) {
                // Upgrade from shared to exclusive
                // Check if this is the only shared lock
                if (std::count_if(locks.begin(), locks.end(), 
                                 [](const LockRequest& lr) { 
                                     return lr.mode == LockMode::SHARED && lr.granted; 
                                 }) == 1) {
                    // Only this transaction has a shared lock, upgrade to exclusive
                    existingLock.mode = LockMode::EXCLUSIVE;
                    return true;
                } else {
                    // Other transactions have shared locks, can't upgrade
                    return false;
                }
            } else {
                // Downgrade from exclusive to shared (allowed)
                existingLock.mode = LockMode::SHARED;
                return true;
            }
        }
    }
    
    // Check if the request is compatible with existing locks
    if (isCompatible(request, locks)) {
        request.granted = true;
        locks.push_back(request);
        return true;
    }
    
    // Lock can't be granted now, add to the queue
    locks.push_back(request);
    return false;
}

void LockManager::releaseAllLocks(int transactionId) {
    // Acquire exclusive lock on the lock table
    std::unique_lock<std::shared_mutex> lock(mutex);
    
    // Remove all locks held by this transaction
    for (auto& pair : lockTable) {
        auto& locks = pair.second;
        locks.erase(
            std::remove_if(locks.begin(), locks.end(),
                          [transactionId](const LockRequest& lr) {
                              return lr.transactionId == transactionId;
                          }),
            locks.end());
        
        // Grant any waiting compatible locks
        for (auto& waitingLock : locks) {
            if (!waitingLock.granted && 
                isCompatible(waitingLock, locks)) {
                waitingLock.granted = true;
            }
        }
        
        // Remove empty entries
        if (locks.empty()) {
            lockTable.erase(pair.first);
        }
    }
}

bool LockManager::detectDeadlock() {
    // Acquire shared lock on the lock table
    std::shared_lock<std::shared_mutex> lock(mutex);
    
    // Build wait-for graph: transaction -> set of transactions it's waiting for
    std::unordered_map<int, std::set<int>> waitForGraph;
    
    for (const auto& pair : lockTable) {
        const auto& locks = pair.second;
        
        // Find transactions that have been granted this lock
        std::set<int> holdingTransactions;
        for (const auto& lr : locks) {
            if (lr.granted) {
                holdingTransactions.insert(lr.transactionId);
            }
        }
        
        // Find transactions waiting for this lock
        for (const auto& lr : locks) {
            if (!lr.granted) {
                // This transaction is waiting for all holding transactions
                if (waitForGraph.find(lr.transactionId) == waitForGraph.end()) {
                    waitForGraph[lr.transactionId] = std::set<int>();
                }
                
                for (int holdingTxn : holdingTransactions) {
                    waitForGraph[lr.transactionId].insert(holdingTxn);
                }
            }
        }
    }
    
    // Check for cycles in the wait-for graph
    for (const auto& pair : waitForGraph) {
        std::set<int> visited;
        std::set<int> recStack;
        if (hasCycle(waitForGraph, pair.first, visited, recStack)) {
            return true;
        }
    }
    
    return false;
}

std::vector<LockRequest> LockManager::getTransactionLocks(int transactionId) const {
    // Acquire shared lock on the lock table
    std::shared_lock<std::shared_mutex> lock(mutex);
    
    std::vector<LockRequest> result;
    
    for (const auto& pair : lockTable) {
        const auto& locks = pair.second;
        
        for (const auto& lr : locks) {
            if (lr.transactionId == transactionId) {
                result.push_back(lr);
            }
        }
    }
    
    return result;
}

bool LockManager::isCompatible(const LockRequest& request, 
                              const std::vector<LockRequest>& existingLocks) {
    // Exclusive locks require no existing granted locks
    if (request.mode == LockMode::EXCLUSIVE) {
        return std::none_of(existingLocks.begin(), existingLocks.end(),
                          [](const LockRequest& lr) { return lr.granted; });
    }
    
    // Shared locks require no existing exclusive granted locks
    if (request.mode == LockMode::SHARED) {
        return std::none_of(existingLocks.begin(), existingLocks.end(),
                          [](const LockRequest& lr) { 
                              return lr.granted && lr.mode == LockMode::EXCLUSIVE; 
                          });
    }
    
    return false;
}

bool LockManager::hasCycle(const std::unordered_map<int, std::set<int>>& waitForGraph,
                          int start, std::set<int>& visited, std::set<int>& recStack) {
    // Mark current node as visited and part of recursion stack
    visited.insert(start);
    recStack.insert(start);
    
    // Check all neighbors
    auto it = waitForGraph.find(start);
    if (it != waitForGraph.end()) {
        for (int neighbor : it->second) {
            // If neighbor not visited, recursively check for cycle
            if (visited.find(neighbor) == visited.end()) {
                if (hasCycle(waitForGraph, neighbor, visited, recStack)) {
                    return true;
                }
            } else if (recStack.find(neighbor) != recStack.end()) {
                // If the neighbor is already in recursion stack, there is a cycle
                return true;
            }
        }
    }
    
    // Remove from recursion stack
    recStack.erase(start);
    return false;
}