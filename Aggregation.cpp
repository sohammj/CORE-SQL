#include "Aggregation.h"
#include <algorithm>
#include <numeric>
#include <sstream>
#include <unordered_map>
#include <stdexcept>
#include <cmath>

double Aggregation::computeMean(const std::vector<std::string>& values) {
    double sum = 0;
    int count = 0;
    
    for (const auto& val : values) {
        if (val.empty()) {
            continue; // Skip NULL values
        }
        
        try {
            double d = std::stod(val);
            sum += d;
            count++;
        } catch (...) {
            // Skip non-numeric values
        }
    }
    
    if (count == 0) {
        return 0; // Return 0 for empty set or all non-numeric values
    }
    
    return sum / count;
}

double Aggregation::computeMin(const std::vector<std::string>& values) {
    double min = std::numeric_limits<double>::infinity();
    bool found = false;
    
    for (const auto& val : values) {
        if (val.empty()) {
            continue; // Skip NULL values
        }
        
        try {
            double d = std::stod(val);
            if (!found || d < min) {
                min = d;
                found = true;
            }
        } catch (...) {
            // Skip non-numeric values
        }
    }
    
    if (!found) {
        return 0; // Return 0 for empty set or all non-numeric values
    }
    
    return min;
}

double Aggregation::computeMax(const std::vector<std::string>& values) {
    double max = -std::numeric_limits<double>::infinity();
    bool found = false;
    
    for (const auto& val : values) {
        if (val.empty()) {
            continue; // Skip NULL values
        }
        
        try {
            double d = std::stod(val);
            if (!found || d > max) {
                max = d;
                found = true;
            }
        } catch (...) {
            // Skip non-numeric values
        }
    }
    
    if (!found) {
        return 0; // Return 0 for empty set or all non-numeric values
    }
    
    return max;
}

double Aggregation::computeSum(const std::vector<std::string>& values) {
    double sum = 0;
    
    for (const auto& val : values) {
        if (val.empty()) {
            continue; // Skip NULL values
        }
        
        try {
            sum += std::stod(val);
        } catch (...) {
            // Skip non-numeric values
        }
    }
    
    return sum;
}

std::string Aggregation::computeMedian(std::vector<std::string> values) {
    std::vector<double> nums;
    
    for (const auto& val : values) {
        if (val.empty()) {
            continue; // Skip NULL values
        }
        
        try {
            double d = std::stod(val);
            nums.push_back(d);
        } catch (...) {
            // Skip non-numeric values
        }
    }
    
    if (nums.empty()) {
        return "0"; // Return 0 for empty set or all non-numeric values
    }
    
    std::sort(nums.begin(), nums.end());
    double median;
    size_t n = nums.size();
    
    if (n % 2 == 0) {
        // Even number of elements, take average of middle two
        median = (nums[n/2 - 1] + nums[n/2]) / 2;
    } else {
        // Odd number of elements, take middle one
        median = nums[n/2];
    }
    
    std::ostringstream oss;
    oss << median;
    return oss.str();
}

std::string Aggregation::computeMode(const std::vector<std::string>& values) {
    if (values.empty()) {
        return ""; // Return empty string for empty set
    }
    
    std::unordered_map<std::string, int> freq;
    int maxCount = 0;
    std::string mode;
    
    for (const auto& val : values) {
        if (val.empty()) {
            continue; // Skip NULL values
        }
        
        freq[val]++;
        
        if (freq[val] > maxCount) {
            maxCount = freq[val];
            mode = val;
        }
    }
    
    if (maxCount == 0) {
        return ""; // No non-NULL values
    }
    
    return mode;
}

int Aggregation::computeCount(const std::vector<std::string>& values, bool countAll) {
    if (countAll) {
        // COUNT(*) - count all rows including NULL values
        return values.size();
    } else {
        // COUNT(column) - count non-NULL values in the column
        int count = 0;
        for (const auto& val : values) {
            if (!val.empty()) {
                count++;
            }
        }
        return count;
    }
}

std::string Aggregation::computeStdDev(const std::vector<std::string>& values, bool population) {
    std::vector<double> nums;
    
    for (const auto& val : values) {
        if (val.empty()) {
            continue; // Skip NULL values
        }
        
        try {
            double d = std::stod(val);
            nums.push_back(d);
        } catch (...) {
            // Skip non-numeric values
        }
    }
    
    if (nums.empty() || nums.size() == 1 && !population) {
        return "0"; // Return 0 for empty set or single value for sample stddev
    }
    
    // Calculate mean
    double mean = std::accumulate(nums.begin(), nums.end(), 0.0) / nums.size();
    
    // Calculate sum of squared differences
    double sumSquaredDiff = 0.0;
    for (double num : nums) {
        double diff = num - mean;
        sumSquaredDiff += diff * diff;
    }
    
    // Calculate standard deviation
    double stdDev;
    if (population) {
        // Population standard deviation
        stdDev = std::sqrt(sumSquaredDiff / nums.size());
    } else {
        // Sample standard deviation
        stdDev = std::sqrt(sumSquaredDiff / (nums.size() - 1));
    }
    
    std::ostringstream oss;
    oss << stdDev;
    return oss.str();
}

std::string Aggregation::computeVariance(const std::vector<std::string>& values, bool population) {
    std::vector<double> nums;
    
    for (const auto& val : values) {
        if (val.empty()) {
            continue; // Skip NULL values
        }
        
        try {
            double d = std::stod(val);
            nums.push_back(d);
        } catch (...) {
            // Skip non-numeric values
        }
    }
    
    if (nums.empty() || nums.size() == 1 && !population) {
        return "0"; // Return 0 for empty set or single value for sample variance
    }
    
    // Calculate mean
    double mean = std::accumulate(nums.begin(), nums.end(), 0.0) / nums.size();
    
    // Calculate sum of squared differences
    double sumSquaredDiff = 0.0;
    for (double num : nums) {
        double diff = num - mean;
        sumSquaredDiff += diff * diff;
    }
    
    // Calculate variance
    double variance;
    if (population) {
        // Population variance
        variance = sumSquaredDiff / nums.size();
    } else {
        // Sample variance
        variance = sumSquaredDiff / (nums.size() - 1);
    }
    
    std::ostringstream oss;
    oss << variance;
    return oss.str();
}

std::string Aggregation::computeStringConcat(const std::vector<std::string>& values, const std::string& separator) {
    std::string result;
    bool first = true;
    
    for (const auto& val : values) {
        if (val.empty()) {
            continue; // Skip NULL values
        }
        
        if (!first) {
            result += separator;
        } else {
            first = false;
        }
        
        result += val;
    }
    
    return result;
}

double Aggregation::computePercentile(std::vector<std::string> values, double percentile) {
    if (percentile < 0 || percentile > 100) {
        throw std::invalid_argument("Percentile must be between 0 and 100");
    }
    
    std::vector<double> nums;
    
    for (const auto& val : values) {
        if (val.empty()) {
            continue; // Skip NULL values
        }
        
        try {
            double d = std::stod(val);
            nums.push_back(d);
        } catch (...) {
            // Skip non-numeric values
        }
    }
    
    if (nums.empty()) {
        return 0; // Return 0 for empty set
    }
    
    std::sort(nums.begin(), nums.end());
    
    // Calculate the index for the percentile
    double index = percentile / 100.0 * (nums.size() - 1);
    size_t lowerIndex = static_cast<size_t>(std::floor(index));
    size_t upperIndex = static_cast<size_t>(std::ceil(index));
    
    if (lowerIndex == upperIndex) {
        return nums[lowerIndex];
    } else {
        // Interpolate between the two values
        double weight = index - lowerIndex;
        return nums[lowerIndex] * (1 - weight) + nums[upperIndex] * weight;
    }
}