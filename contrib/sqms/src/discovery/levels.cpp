#include "discovery/levels.hpp"
#include <unordered_set>
#include <algorithm>

/**
 * LevelOneStrategy: check the set of subplan's node type set,if the sub_plan's 
 * node type is in (join,project,filter,groupby),we will apply more checking,ohterwise,
 * we just use hash to compare,with time going,we will 
 */
std::vector<std::string> LevelOneStrategy::findChildren(){
    return std::vector<std::string>();
}

/**
 * LevelTwoStrategy: check the qual constraint condition,such as "t1.a = t2.b"
 */
std::vector<std::string> LevelTwoStrategy::findChildren(){
    return std::vector<std::string>();
}

/**
 * LevelThreeStrategy: check the range constraint condition
 */
std::vector<std::string> LevelThreeStrategy::findChildren(){
    return std::vector<std::string>();
}

