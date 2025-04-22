#ifndef CONCURRENCY_H
#define CONCURRENCY_H

#include <mutex>
#include <shared_mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

// Lock types
enum class LockMode {
    SHARED,      // Read lock (multiple allowed)
    EXCLUSIVE    // Write lock (exclusive)
};

// Resource types
enum class ResourceType {
    TABLE,      // Table resource
    ROW,        // Row resource
    DATABASE    // Entire database
};

// Lock request
struct LockRequest {
    int transactionId;
    std::string resourceName;
    ResourceType resourceType;
    LockMode mode;
    bool granted;
    
    LockRequest(int txnId, const std::string& resource, ResourceType type, LockMode lockMode)
        : transactionId(txnId), resourceName(resource), resourceType(type), 
          mode(lockMode), granted(false) {}
};

// Lock manager - handles concurrency control using two-phase locking
class LockManager {
public:
    // Request a lock
    bool acquireLock(int transactionId, const std::string& resourceName, 
                    ResourceType resourceType, LockMode mode);
    
    // Release all locks held by a transaction
    void releaseAllLocks(int transactionId);
    
    // Check for deadlocks
    bool detectDeadlock();
    
    // Get locks held by a transaction
    std::vector<LockRequest> getTransactionLocks(int transactionId) const;
    
private:
    // Lock table: resource -> set of lock requests
    std::unordered_map<std::string, std::vector<LockRequest>> lockTable;
    
    // Mutex for the lock table
    mutable std::shared_mutex mutex;
    
    // Helper methods
    bool isCompatible(const LockRequest& request, const std::vector<LockRequest>& existingLocks);
    bool hasCycle(const std::unordered_map<int, std::set<int>>& waitForGraph, 
                 int start, std::set<int>& visited, std::set<int>& recStack);
};

#endif // CONCURRENCY_H