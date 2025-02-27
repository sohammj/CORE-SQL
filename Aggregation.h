#ifndef AGGREGATION_H
#define AGGREGATION_H

#include <vector>
#include <string>

class Aggregation {
public:
    static double computeMean(const std::vector<std::string>& values);
    static double computeMin(const std::vector<std::string>& values);
    static double computeMax(const std::vector<std::string>& values);
    static double computeSum(const std::vector<std::string>& values);
    static std::string computeMedian(std::vector<std::string> values);
    static std::string computeMode(const std::vector<std::string>& values);
};

#endif // AGGREGATION_H