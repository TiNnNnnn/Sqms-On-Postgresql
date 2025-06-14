#include "discovery/sm_level_mgr.hpp"
#include <cfloat>
#include "utils/date.h"

void SMPredEquivlenceRange::Copy(PredEquivlenceRange* per){
    type_ = per->PredType();
    var_type_ = per->PredVarType();
    subquery_name_ = SMString(per->GetSubqueryName());
    lower_limit_ = SMString(per->LowerLimit());
    upper_limit_ = SMString(per->UpperLimit());
    boundary_constraint_ = per->GetBoundaryConstraint();
	list_op_type_ = per->ListOpType();
	for(const auto& item : per->List()){
		list_.push_back(SMString(item));
	}
	SMString list_use_or_ = SMString(per->ListUseOr());
    for(const auto& item : per->GetRightSets()){
		right_sets_.insert(SMString(item));
	}
	serialization_ = Serialization();
}

int SMPredEquivlenceRange::LimitCompare(const SMString& left_range,VarType left_type,const SMString& right_range,VarType right_type){
	assert(left_range.size() && right_range.size());

    /*pasrer string to int*/
	auto int_parser = [](const SMString& range)->int{
		int var = 0;
		if(range == UPPER_LIMIT){
			var = INT_MAX;
		}else if(range == LOWER_LIMIT){
			var = INT_MIN;
		}else{
			var = std::atoi(range.c_str());
		}
		return var;
	};
	
	/*parser string to double*/
	auto double_parser = [](const SMString& range)->double{
		double var = 0;
		if(range == UPPER_LIMIT){
			var = DBL_MAX;
		}else if(range == LOWER_LIMIT){
			var = DBL_MIN;
		}else{
			var = std::atof(range.c_str());
		}
		return var;
	};

	auto date_parser = [](const SMString& range) -> DateADT {
		DateADT d;
		if (range == UPPER_LIMIT) {
			d = DatumGetDateADT(DirectFunctionCall1(date_in, CStringGetDatum("5874897-12-31")));
		} else if (range == LOWER_LIMIT) {
			d = DatumGetDateADT(DirectFunctionCall1(date_in, CStringGetDatum("4713-01-01 BC")));
		} else {
			d = DatumGetDateADT(DirectFunctionCall1(date_in, CStringGetDatum(range.c_str())));
		}
	
		return d;
	};

	switch(left_type){
		case VarType::INT:{
			int left_var = int_parser(left_range);
			if(right_type == VarType::INT){
				int right_var = int_parser(right_range);
				return left_var - right_var;
			}else if(right_type == VarType::DOUBLE){
				double right_var = double_parser(right_range);
				if(double(left_var) == right_var){
					return 0;
				}else{
					return double(left_var) < right_var ? -1 : 1;
				}
			}else{
				std::cerr<<"right type not match left type <int>"<<std::endl;
			}
		}break;
		case VarType::STRING:{
			if(right_type == VarType::STRING){
				if(left_range == right_range)return 0;
				else{
					return left_range < right_range ? -1 : 1;
				}
			}else{
				std::cerr<<"right type not match left type <string>"<<std::endl;
			}
		}break;
		case VarType::DOUBLE:{
			double left_var = double_parser(right_range);
			if(right_type == VarType::DOUBLE){
				double right_var = double_parser(right_range);
				return left_var - right_var;
			}else if(right_type == VarType::INT){
				double right_var = int_parser(right_range);
				return left_var - right_var;
			}else{
				std::cerr<<"right type not match left type <double>"<<std::endl;
			}
		}break;
		case VarType::BOOL:{
			bool left_var = int_parser(left_range);
			if(right_type == VarType::BOOL){
				bool right_var = int_parser(right_range);
				return left_var == right_var;
			}else{
				std::cerr<<"right type not match left type <double>"<<std::endl;
			}
		}break;
		case VarType::DATE:{
			auto left_var = date_parser(left_range);
			if(right_type == VarType::DATE){
				auto right_var = date_parser(right_range);
				//std::cout<<"left_data:"<<left_var<<",right_date:"<<right_var<<std::endl;
				return left_var - right_var;
			}else{
				std::cerr<<"right type not match left type <date>"<<std::endl;
				exit(-1);
			}
		}break;
		case VarType::UNKNOWN:{
			/*if it's a unknown type,we just compare them base on str*/
			if(left_range == right_range){
				return 0;
			}else{
				return left_range < right_range;
			}
		}break;
		default:{
			std::cerr<<"error type in limit comparing!"<<std::endl;
			exit(-1);
		}
	}
	return true;
}

void SMPredEquivlence::Copy(PredEquivlence* pe){
    assert(pe);
    for(const auto& attr : pe->GetPredSet()){
        set_.insert(SMString(attr));
    } 
    for(const auto& range: pe->GetRanges()){
        SMPredEquivlenceRange* sm_range =  (SMPredEquivlenceRange*)ShmemAlloc(sizeof(SMPredEquivlenceRange));
        assert(sm_range);
        new (sm_range) SMPredEquivlenceRange();
        sm_range->Copy(range);
        ranges_.insert(sm_range);
    }
    for(const auto& item : pe->GetSubLinkLevelPeLists()){
        SMLevelManager* sm_level_mgr = (SMLevelManager*)ShmemAlloc(sizeof(SMLevelManager));
        assert(sm_level_mgr);
        assert(item.second);
        new (sm_level_mgr) SMLevelManager();
        sm_level_mgr->Copy(item.second.get());
        sublink_level_pe_lists_[SMString(item.first)] = sm_level_mgr;
    }
    early_stop_ = pe->EarlyStop();

    serialization_ = Serialization();
    has_subquery_ = pe->HasSubquery();
    has_range_ =pe->HasRange();
    range_cnt_ = pe->RangeCnt();
    for(const auto& name : pe->SubqueryNames()){
        subquery_names_.insert(SMString(name));
    }
    lower_limit_ = pe->LowerLimit();
    upper_limit_ = pe->UpperLimit();
     
}

SMString SMPredEquivlence::Serialization(){
	SMString str;
	for(const auto& name : set_){
		str += name;
	}
	for(const auto& range : ranges_){
		str += range->GetSerialization();
	}
	for(const auto& item: sublink_level_pe_lists_){
		str += item.second->GetJsonFullSubPlan();
	}
	return str;
}    
SMString SMPredEquivlence::GetSerialization(){
	if(serialization_.empty())
		return Serialization();
	return serialization_; 
}


const SMString& SMLevelManager::GetJsonSubPlan(){return json_sub_plan_;}
const SMString& SMLevelManager::GetJsonFullSubPlan(){return json_full_sub_plan_;}
const uint8_t* SMLevelManager::GetHspsPackage(){return hsps_package_;}
const SMString& SMLevelManager::GetQueryStr(){return q_str_;}
const size_t SMLevelManager::GetHspsPackSize(){return hsps_size_;}