#include "user.h"
#include <algorithm>
#include <functional>
#include "Utils.h"

// Simple password hashing (in a real system, use a proper cryptographic hash)
std::string User::hashPassword(const std::string& password) {
    // This is a placeholder. In a real system, use a secure hashing algorithm with salt
    std::hash<std::string> hasher;
    return std::to_string(hasher(password));
}

User::User(const std::string& username, const std::string& password)
    : username(username), passwordHash(hashPassword(password)) {}

bool User::authenticate(const std::string& password) const {
    return passwordHash == hashPassword(password);
}

void User::changePassword(const std::string& newPassword) {
    passwordHash = hashPassword(newPassword);
}

void User::grantPrivilege(const std::string& objectName, Privilege::Type type, bool withGrantOption) {
    // Check if this privilege already exists
    for (auto& privilege : privileges) {
        if (toLowerCase(privilege.objectName) == toLowerCase(objectName) && privilege.type == type) {
            privilege.withGrantOption = withGrantOption;
            return;
        }
    }
    
    // Add new privilege
    privileges.emplace_back(type, objectName, withGrantOption);
}

void User::revokePrivilege(const std::string& objectName, Privilege::Type type) {
    privileges.erase(
        std::remove_if(privileges.begin(), privileges.end(), 
            [&](const Privilege& p) {
                return toLowerCase(p.objectName) == toLowerCase(objectName) && p.type == type;
            }),
        privileges.end());
}

bool User::hasPrivilege(const std::string& objectName, Privilege::Type type) const {
    // Check for direct privilege on the object
    for (const auto& privilege : privileges) {
        if ((toLowerCase(privilege.objectName) == toLowerCase(objectName) && 
             (privilege.type == type || privilege.type == Privilege::Type::ALL)) ||
            (privilege.objectName == "*" && 
             (privilege.type == type || privilege.type == Privilege::Type::ALL))) {
            return true;
        }
    }
    
    // If no direct privilege is found, return false
    return false;
}

void User::addRole(const std::string& roleName) {
    roles.insert(toLowerCase(roleName));
}

void User::removeRole(const std::string& roleName) {
    roles.erase(toLowerCase(roleName));
}

bool User::hasRole(const std::string& roleName) const {
    return roles.find(toLowerCase(roleName)) != roles.end();
}

std::vector<Privilege> User::getAllPrivileges() const {
    return privileges;
}

std::vector<std::string> User::getRoles() const {
    return std::vector<std::string>(roles.begin(), roles.end());
}