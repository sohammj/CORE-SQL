#include "Aggregation.h"
#include <algorithm>
#include <sstream>
#include <unordered_map>
#include <stdexcept>
#include <cmath>

double Aggregation::computeMean(const std::vector<std::string>& values) {
    double sum = 0;
    int count = 0;
    for (const auto& val : values) {
        try {
            double d = std::stod(val);
            sum += d;
            count++;
        } catch (...) {}
    }
    if (count == 0) return 0;
    return sum / count;
}

double Aggregation::computeMin(const std::vector<std::string>& values) {
    double min = INFINITY;
    for (const auto& val : values) {
        try {
            double d = std::stod(val);
            if (d < min)
                min = d;
        } catch (...) {}
    }
    return min;
}

double Aggregation::computeMax(const std::vector<std::string>& values) {
    double max = -INFINITY;
    for (const auto& val : values) {
        try {
            double d = std::stod(val);
            if (d > max)
                max = d;
        } catch (...) {}
    }
    return max;
}

double Aggregation::computeSum(const std::vector<std::string>& values) {
    double sum = 0;
    for (const auto& val : values) {
        try {
            sum += std::stod(val);
        } catch (...) {}
    }
    return sum;
}

std::string Aggregation::computeMedian(std::vector<std::string> values) {
    std::vector<double> nums;
    for (const auto& val : values) {
        try {
            double d = std::stod(val);
            nums.push_back(d);
        } catch (...) {}
    }
    if (nums.empty()) return "0";
    std::sort(nums.begin(), nums.end());
    double median;
    int n = nums.size();
    if (n % 2 == 0)
        median = (nums[n/2 - 1] + nums[n/2]) / 2;
    else
        median = nums[n/2];
    std::ostringstream oss;
    oss << median;
    return oss.str();
}

std::string Aggregation::computeMode(const std::vector<std::string>& values) {
    std::unordered_map<std::string, int> freq;
    for (const auto& val : values) {
        freq[val]++;
    }
    int maxCount = 0;
    std::string mode;
    for (const auto& kv : freq) {
        if (kv.second > maxCount) {
            maxCount = kv.second;
            mode = kv.first;
        }
    }
    return mode;
}