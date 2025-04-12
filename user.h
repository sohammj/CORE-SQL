#ifndef USER_H
#define USER_H

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

// Represents a database privilege
struct Privilege {
    enum class Type {
        SELECT,
        INSERT,
        UPDATE,
        DELETE,
        ALL
    };
    
    Type type;
    std::string objectName;
    bool withGrantOption;
    
    Privilege(Type t, const std::string& obj, bool grant = false)
        : type(t), objectName(obj), withGrantOption(grant) {}
};

// Represents a database user
class User {
public:
    User() = default;
    User(const std::string& username, const std::string& passwordHash);
    
    // User authentication
    bool authenticate(const std::string& password) const;
    void changePassword(const std::string& newPassword);
    
    // Privilege management
    void grantPrivilege(const std::string& objectName, Privilege::Type type, bool withGrantOption = false);
    void revokePrivilege(const std::string& objectName, Privilege::Type type);
    bool hasPrivilege(const std::string& objectName, Privilege::Type type) const;
    
    // User roles and groups
    void addRole(const std::string& roleName);
    void removeRole(const std::string& roleName);
    bool hasRole(const std::string& roleName) const;
    
    // Getters
    const std::string& getUsername() const { return username; }
    std::vector<Privilege> getAllPrivileges() const;
    std::vector<std::string> getRoles() const;
    
private:
    std::string username;
    std::string passwordHash;
    std::vector<Privilege> privileges;
    std::unordered_set<std::string> roles;
    
    // Helper to hash passwords
    static std::string hashPassword(const std::string& password);
};

#endif // USER_H