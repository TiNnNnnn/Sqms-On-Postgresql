#include "inverted_index.hpp"
#include <iostream>

void TestInvertedIndex1(){
    std::cout<<"TestInvertedIndex1 Begin"<<std::endl;
    InvertedIndex<PostingList> idx;
    idx.Insert({"A"});
    idx.Insert({"B"});
    idx.Insert({"D"});
    idx.Insert({"A"});
    idx.Insert({"A","B"});
    idx.Insert({"B","E"});
    idx.Insert({"A","B","C"});
    idx.Insert({"A","B","F"});
    idx.Insert({"B","C","D","E"});
    
    std::cout<<"SubSet:"<<std::endl;
    auto res = idx.SubSets({"A","B"});
    for(auto set : res){
        for(auto item : set)std::cout<<item<<",";
        std::cout<<std::endl;
    }
    
    std::cout<<"SuperSet:"<<std::endl;
    res = idx.SuperSets({"A","B"});
    for(auto set : res){
        for(auto item : set)std::cout<<item<<",";
        std::cout<<std::endl;
    }
    std::cout<<"TestInvertedIndex1 End"<<std::endl;
}

void TestInvertedIndex2(){
    std::cout<<"TestInvertedIndex2 Begin"<<std::endl;
    InvertedIndex<PostingList> idx;
    idx.Insert({"A"});
    idx.Insert({"B"});
    idx.Insert({"D"});
    idx.Insert({"A","B"});
    idx.Insert({"B","E"});
    idx.Insert({"A","B","C"});
    idx.Insert({"A","B","F"});
    idx.Insert({"B","C","D","E"});
    
    std::cout<<"SubSet:"<<std::endl;
    auto res = idx.SubSets({"A","B"});
    for(auto set : res){
        for(auto item : set)std::cout<<item<<",";
        std::cout<<std::endl;
    }
    
    std::cout<<"SuperSet:"<<std::endl;
    res = idx.SuperSets({"A","B"});
    for(auto set : res){
        for(auto item : set)std::cout<<item<<",";
        std::cout<<std::endl;
    }

    std::cout<<"Del Sets"<<std::endl;
    idx.Erase({"A"});
    idx.Erase({"A","B"});
    idx.Erase({"A","B","C"});
    idx.Erase({"E"});

    std::cout<<"SubSet:"<<std::endl;
    res = idx.SubSets({"A","B"});
    for(auto set : res){
        for(auto item : set)std::cout<<item<<",";
        std::cout<<std::endl;
    }
    
    std::cout<<"SuperSet:"<<std::endl;
    res = idx.SuperSets({"A","B"});
    for(auto set : res){
        for(auto item : set)std::cout<<item<<",";
        std::cout<<std::endl;
    }
    std::cout<<"TestInvertedIndex2 End"<<std::endl;
}

int main(){
    TestInvertedIndex1();
    TestInvertedIndex2();
    return 0;
}