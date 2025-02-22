#pragma once
#include "common/util.hpp"
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
#include <iostream>
#include "sm_level_mgr.hpp"

/**
 * ScalingInfo
 * - join_type_list
 * TODO: 01-23 maybe we can put indexname , cols here to scale  
 */
class ScalingInfo{
    public:
        ScalingInfo(std::vector<std::string> join_type_list){
            for(const auto& type : join_type_list){
                join_type_list_.push_back(SMString(type.c_str()));
            }
            join_type_score_ = CalJoinTypeScore(join_type_list_,unique_id_);
        }
        static int CalJoinTypeScore(const SMVector<SMString>& join_type_list,SMString& unique_id);
        bool Match(ScalingInfo* scale_info);
        const SMString& UniqueId(){return unique_id_;}
        int JoinTypeScore(){return join_type_score_;}
    private:
        SMVector<SMString> join_type_list_;
        int join_type_score_ = -1;
        SMString unique_id_;
    };

/**
 * TODO: a temp implemetaion of postingList,it will be replaced by another one 
 */
typedef SMVector<SMString> SET;
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
    void Insert(const SET &set,int id){
        list_.insert(set);
        bit_set_.set(id,1);
    }

    void Erase(const SET &set,int id){
        list_.erase(set);
        bit_set_.set(id,0);
    }

    SMVector<SET> SubSets(const SET &set){
        SMVector<SET> result;
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
    SMUnorderedSet<SET,SetHasher>list_;
    std::bitset<bit_map_size> bit_set_;
};

template<typename PostingList>
class InvertedIndex{
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
    SMUnorderedSet<SET,SetHasher> SuperSets(const SET&set){
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        SMUnorderedSet<SET,SetHasher>set_list;
        auto map = inverted_map_[set[0]].GetBitSet();
        for(size_t i=1;i<set.size();i++){
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
    SMUnorderedSet<SET,SetHasher> SubSets(const SET&set){
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        SMUnorderedSet<SET,SetHasher> set_list;
        for(auto item : set){
            auto temp_sets = inverted_map_[item].SubSets(set);
            for(auto set: temp_sets){
                set_list.insert(set);
            }                
        }
        return set_list;
    }

    int GetSetIdBySet(const SET&set){
        std::shared_lock<std::shared_mutex> lock(rw_mutex_);
        if(sets2id_.find(set) == sets2id_.end())return -1;
        return sets2id_[set];
    }

private:
    SMUnorderedMap<SMString,PostingList,SMStringHash>inverted_map_;
    /* item in set,such as A,B,C,D....*/
    /* set such as {A,B},{A,B,C},{B,C,D,E} to their id*/
    SMUnorderedMap<SET,int,SetHasher>sets2id_; 
    SMUnorderedMap<int,SET> id2sets_; 
    /*id generate for the set id*/
    std::atomic<long long> set_cnt_{-1}; 
    std::atomic<int>items_cnt_{0};
    std::shared_mutex rw_mutex_;
};


/**
 * pe in posting list should be order by ranges, then we can use lower_bound to acc sarech
 */
struct PredRangeCompare {
    bool operator()(const SMPredEquivlence* lhs, const SMPredEquivlence* rhs) const{
        if(lhs->HasRange()){
            if(rhs->HasRange()){
                if(lhs->LowerLimit() == LOWER_LIMIT || (lhs->LowerLimit() != LOWER_LIMIT && rhs->LowerLimit() != LOWER_LIMIT && lhs->LowerLimit() < rhs->LowerLimit())){
                    return true;
                }else if(lhs->LowerLimit() == rhs->LowerLimit()){
                    if(lhs->LowerLimit() == UPPER_LIMIT || (lhs->UpperLimit() != UPPER_LIMIT && rhs->UpperLimit() != UPPER_LIMIT && lhs->UpperLimit() < rhs->UpperLimit())){
                        return true;
                    }
                }
                return false;
            }else{
                return false;
            }
        }else{
            if(rhs->HasRange()){
                return true;
            }else{
                return lhs->SubqueryNames().size() <= rhs->SubqueryNames().size();
            }
        }
    }
}; 
class RangePostingList{
public:
    void Insert(SMPredEquivlence* range,int id);
    void Erase(SMPredEquivlence* range,int id);
    /*get all ranges which is superset of range,used in query index*/
    SMVector<int> SuperSet(SMPredEquivlence* pe);
    /**
     * TODO: not implement yet
     */
    SMVector<int> SubSet(PredEquivlence* range);
    
private:
    /*check if the pe is the dst_pe's superset*/
    bool SuperSetInternal(SMPredEquivlence* dst_pe, SMPredEquivlence* pe);
    bool SearchSubquery(LevelManager* src_mgr,LevelManager* dst_mgr);
private:
    SMSet<SMPredEquivlence*,PredRangeCompare>sets_;
    SMUnorderedMap<SMPredEquivlence*,int> set2id_;
};

class RangeInvertedIndex{
public:
    /*insert a set into inverted index*/
    void Insert(LevelPredEquivlences* lpes);
    /*erase a set from inverted index*/
    void Erase(LevelPredEquivlences* lpes);
    /**serach a set whether in inverted index or not */
    bool Serach(LevelPredEquivlences* lpes);
    /*find superset of <set>*/
    SMSet<int> SuperSets(LevelPredEquivlences* lpes);
    /*find subsets of <set>*/
    SMSet<int> SubSets(LevelPredEquivlences* lpes);
    /*get lpes's all pe id in invertedindex*/
    SMSet<int> GetLpesIds(LevelPredEquivlences* lpes);

private:
    SMUnorderedMap<SMString,RangePostingList,SMStringHash>inverted_map_;
    /* item in set,such as A,B,C,D....*/
    /* set such as {A,B},{A,B,C},{B,C,D,E} to their id*/
    SMUnorderedMap<SMString,int,SMStringHash>set2id_; 
    SMUnorderedMap<int,SMString> id2set_; 
    /*id generate for the set id*/
    std::atomic<long long> set_cnt_{-1}; 
    std::atomic<int>items_cnt_{0};
    std::shared_mutex rw_mutex_;
};

