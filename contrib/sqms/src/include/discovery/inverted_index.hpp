#pragma once
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <atomic>
#include <vector>
#include <functional>
#include <shared_mutex>
#include <boost/bimap.hpp>
#include <mutex>

extern "C" {
    #include "common/config.h"
}

/**
 * TODO: a temp implemetaion of postingList,it will be replaced by another one 
 */
typedef std::vector<std::string> SET;
static std::string EncodingSets(const SET&sets){
    std::string encoding;
    for(size_t i=0;i<sets.size();i++){
        if(i!=0){
            encoding+=",";
        }
        encoding+= sets[i];
    }
    return encoding;
}

struct SetHasher {
    std::size_t operator()(const SET& sets) const {
        std::string encoding = EncodingSets(sets);
        return std::hash<std::string>{}(encoding);
    }

    std::size_t hash(const SET& sets) const {
        return operator()(sets);
    }

    bool equal(const SET& lhs, const SET& rhs) const {
        return lhs == rhs;
    }
};

class PostingList{
public:
    typedef std::vector<std::string> SET;
    void Insert(const SET &set,int id){
        list_.insert(set);
        bit_set_.set(id,1);
    }

    void Erase(const SET &set,int id){
        list_.erase(set);
        bit_set_.set(id,0);
    }

    std::vector<SET> SubSets(const SET &set){
        std::vector<SET> result;
        for (const auto& list_set : list_) {
            if(list_set.size() > set.size())continue;
            if (IsSubset(list_set, set)) {
                result.push_back(list_set);
            }
        }
        return result;
    }

    std::bitset<bit_map_size> GetBitSet(){
        return bit_set_;
    }
private:
    /*check if <list_set> is the subset of <set> */
    bool IsSubset(const SET &list_set, const SET &set) {
        for (const auto& item : list_set) {
            if (std::find(set.begin(), set.end(), item) == set.end()) {
                return false;
            }
        }
        return true;
    }

private:
    std::unordered_set<SET,SetHasher> list_;
    std::bitset<bit_map_size> bit_set_;
};

template<typename PostingList>
class InvertedIndex{
    typedef std::vector<std::string> SET;
public:
    /*insert a set into inverted index*/
    void Insert(const SET& set){
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        for(auto item : set){
            if(sets2id_.find(set) == sets2id_.end()){
                set_cnt_++;
                sets2id_[set] = set_cnt_;
                id2sets_[set_cnt_] = set;
            }
            inverted_map_[item].Insert(set,sets2id_[set]);
            items_cnt_ = inverted_map_.size();
        }
    }

    /*erase a set from inverted index*/
    void Erase(const SET&set){
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        if(sets2id_.find(set) == sets2id_.end())return;
        for(auto item : set){
            inverted_map_[item].Erase(set,sets2id_[set]);    
        }
        items_cnt_ = inverted_map_.size();
    }

    /**serach a set whether in inverted index or not */
    bool Serach(const SET& set){
        std::unique_lock<std::shared_mutex> lock(rw_mutex_);
        if(sets2id_.find(set) == sets2id_.end())
            return false;
        return true;
    }

    /*find superset of <set>*/
    std::unordered_set<SET,SetHasher> SuperSets(const SET&set){
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        std::unordered_set<SET,SetHasher> set_list;
        auto map = inverted_map_[set[0]].GetBitSet();
        for(int i=1;i<set.size();i++){
            map &= inverted_map_[set[i]].GetBitSet();
        }

        for(int i=0;i<bit_map_size;i++){
            if(map[i] == 1){
                set_list.insert(id2sets_[i]);
            }
        }
        return set_list;
    }

    /*find subsets of <set>*/
    std::unordered_set<SET,SetHasher> SubSets(const SET&set){
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        std::unordered_set<SET,SetHasher> set_list;
        for(auto item : set){
            auto temp_sets = inverted_map_[item].SubSets(set);
            for(auto set: temp_sets){
                set_list.insert(set);
            }                
        }
        return set_list;
    }
private:
    std::unordered_map<std::string, PostingList> inverted_map_;

    /* item in set,such as A,B,C,D....*/
    /* set such as {A,B},{A,B,C},{B,C,D,E} to their id*/
    std::unordered_map<std::vector<std::string>,int,SetHasher> sets2id_; 
    std::unordered_map<int,std::vector<std::string>> id2sets_; 
    /*id generate for the set id*/
    std::atomic<long long> set_cnt_{-1}; 
    std::atomic<int>items_cnt_{0};
    std::shared_mutex rw_mutex_;
};

