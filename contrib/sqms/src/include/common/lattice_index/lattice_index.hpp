#include<iostream>
#include<memory>
#include<unordered_set>

#include <unordered_set>
#include <memory>
#include <vector>
#include <iostream>


enum Role {
        TOPS = 0,
        ROOTS,
        MIDS,
};


template<typename T>
class LatticeTreeNode {
public:
    LatticeTreeNode(T key) : key_(key), role_(MIDS) {}

    T getKey() const {
        return key_;
    }

    void addSubSet(std::shared_ptr<LatticeTreeNode<T>> subNode) {
        sub_set_.insert(subNode);
    }

    void addSuperSet(std::shared_ptr<LatticeTreeNode<T>> superNode) {
        super_set_.insert(superNode);
    }

    const std::unordered_set<std::shared_ptr<LatticeTreeNode<T>>>& getSubSets() const {
        return sub_set_;
    }

    const std::unordered_set<std::shared_ptr<LatticeTreeNode<T>>>& getSuperSets() const {
        return super_set_;
    }

    void setRole(Role role) {
        role_ = role;
    }

    Role getRole() const {
        return role_;
    }

    //check if the key is the 
    bool isSubSet(T key){
        
    }

private:
    T key_;  // The key of the set (e.g., "AB")
    std::unordered_set<std::shared_ptr<LatticeTreeNode<T>>> sub_set_;  // Subsets (e.g., "A", "B")
    std::unordered_set<std::shared_ptr<LatticeTreeNode<T>>> super_set_;  // Supersets (e.g., "ABC")
    Role role_;  // The role of the node (TOPS, ROOTS, or MIDS)
};


template<typename T, typename SubsetChecker, typename SupersetChecker>
class LatticeTree {
public:
    // Insert a new node into the lattice tree
    bool Insert(T key) {
        if(key_sets_.find(key) != key_sets_.end()){
            return;
        }

        // Create a new node
        auto newNode = std::make_shared<LatticeTreeNode<T>>(key);
        nodes_.push_back(newNode);
        key_sets_.insert(key);

        bool inserted = false;
        for(auto top : tops_){
            inserted = InsertInternal(newNode,top,nullptr);
        }
        return inserted;
    }

    // Erase a node from the lattice tree
    void Erase(T key) {

    }

    // Find the subsets of the given key
    void SubSets(T key, std::vector<T>& subset) {
    }

    // Find the supersets of the given key
    void SuperSets(T key, std::vector<T>& superset) {
    }

private:
    bool InsertInternal(std::shared_ptr<LatticeTreeNode<T>> newNode, std::shared_ptr<LatticeTreeNode<T>> currentNode,std::shared_ptr<LatticeTreeNode<T>>prevNode,bool upper = true){
        bool inserted = false;
        
        if(currentNode == nullptr){
            if(upper){
                /**current pos is pre node's subset*/
                newNode->addSuperSet(prevNode);
                prevNode->addSubSet(newNode);
                newNode->setRole(Role::ROOTS);
            }else{
                newNode->addSubSet(prevNode);
                prevNode->addSuperSet(newNode);
                newNode->setRole(Role::TOPS);
                tops_->insert(newNode);
            }
            return true;
        }else{
            if()
        }

        if (supersetChecker_(newNode, currentNode)) {
            //newNode is a superset of cur node,then we go up 
            for (auto& superNode : currentNode->getSuperSets()){
                inserted = InsertInternal(newNode, superNode,currentNode,false);
                if (inserted) {
                    break;
                }
            }
        }else if(subsetChecker_(newNode, currentNode)){
            //newNode is a subset of cur node,then we go down 
            for(auto& subNode : currentNode->getSubSets()){
                inserted = InsertInternal(newNode,subNode,currentNode,true);
                if(inserted){
                    break;
                }
            }
        }
    }

private:
    // Sets of root and top nodes
    std::unordered_set<std::shared_ptr<LatticeTreeNode<T>>> tops_;
    // Set of keys to avoid inserting duplicate nodes
    std::unordered_set<T> key_sets_;

    SubsetChecker subsetChecker_;
    SupersetChecker supersetChecker_;

};
