#ifndef AGGREGATION_H
#define AGGREGATION_H

#include <vector>
#include <string>

class Aggregation {
public:
    // Basic aggregate functions
    static double computeMean(const std::vector<std::string>& values);
    static double computeMin(const std::vector<std::string>& values);
    static double computeMax(const std::vector<std::string>& values);
    static double computeSum(const std::vector<std::string>& values);
    static std::string computeMedian(std::vector<std::string> values);
    static std::string computeMode(const std::vector<std::string>& values);
    static int computeCount(const std::vector<std::string>& values, bool countAll = false);
    
    // Advanced aggregate functions
    static std::string computeStdDev(const std::vector<std::string>& values, bool population = true);
    static std::string computeVariance(const std::vector<std::string>& values, bool population = true);
    static std::string computeStringConcat(const std::vector<std::string>& values, const std::string& separator = ",");
    static double computePercentile(std::vector<std::string> values, double percentile);
};

#endif // AGGREGATION_H