#include "collect/level_mgr.hpp"
/**
 * ComputeEquivlenceClass: calulate the equivelence class and its containment for
 * each level for plan
 */
void LevelManager::ComputeTotalClass(){
    
    std::vector<HistorySlowPlanStat*>levels;
    std::vector<HistorySlowPlanStat*>tmp_levels;
    std::vector<std::vector<HistorySlowPlanStat*>> level_collector;

    levels.push_back(hsps_);
    while(levels.size()){
        size_t sz = levels.size();
        for(size_t i=0;i<sz;i++){
            for(size_t j = 0; j < levels[i]->n_childs ; j++){
                if(levels[i]->childs[j]){
                    tmp_levels.push_back(levels[i]);
                }
            }
        }
        level_collector.push_back(levels);
        levels.clear();
        std::swap(levels,tmp_levels);
    }
    std::reverse(level_collector.begin(),level_collector.end());
	
	height_ = level_collector.size();
    for(auto &lc : level_collector){
        ComputeLevelClass(lc);
    }
}

void LevelManager::ComputeLevelClass(const std::vector<HistorySlowPlanStat*>& list){
    for(const auto& s : list){
        /*parse exprstr first,then caluate equivlence class*/
        HandleNode(s);    
    }
}

void LevelManager::HandleNode(HistorySlowPlanStat* hsps){
    switch(hsps->node_tag){
        case T_Result:
			break;
		case T_ProjectSet:
			break;
		case T_ModifyTable:
			break;
		case T_Append:
			break;
		case T_MergeAppend:
			break;
		case T_RecursiveUnion:
			break;
		case T_BitmapAnd:
			break;
		case T_BitmapOr:
			break;
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
            /*jion*/

			break;
		case T_SeqScan:

			break;
		case T_SampleScan:
			break;
		case T_Gather:
			break;
		case T_GatherMerge:
			break;
		case T_IndexScan:
			break;
		case T_IndexOnlyScan:
			break;
		case T_BitmapIndexScan:
			break;
		case T_BitmapHeapScan:
			break;
		case T_TidScan:
			break;
		case T_SubqueryScan:
			break;
		case T_FunctionScan:
			break;
		case T_TableFuncScan:
			break;
		case T_ValuesScan:
			break;
		case T_CteScan:
			break;
		case T_NamedTuplestoreScan:
			break;
		case T_WorkTableScan:
			break;
		case T_ForeignScan:
			break;
		case T_CustomScan:
			break;
		case T_Material:
			break;
		case T_Sort:
			break;
		case T_IncrementalSort:
			break;
		case T_Group:
			break;
		case T_Agg:
			break;
		case T_WindowAgg:
			break;
		case T_Unique:
			break;
		case T_SetOp:
			break;
		case T_LockRows:
			break;
		case T_Limit:
			break;
		case T_Hash:
			break;
		default:
			break;  
    }
}

/**
 * EquivalenceClassesDecompase:calulate equivalance class shoule be
 * the first step of ComputeLevelClass,we caluate other level attr 
 * and class base on it's level equivalence calss 
 */
void LevelManager::EquivalenceClassesDecompase(PredExpression* root){
	
	std::vector<std::vector<AbstractPredNode*>> level_collector;
	ExprLevelCollect(root,level_collector);

	if(!level_collector.size()){
		/*no join_cond ,it means a full cartesian product*/
	}else if(level_collector.size() == 1){
		/*actually,maybe its the most common condition*/ 
        assert(level_collector[0].size()==1);
        assert(level_collector[0][0]->Type() == AbstractPredNodeType::QUALS);
        auto qual = static_cast<QualsWarp*>(level_collector[0][0])->GetQual();
		LevelPredEquivlences * lpes = new LevelPredEquivlences();
		if(strcmp(qual->op,"=")){
			PredEquivlence* pe = new PredEquivlence();
			lpes->Insert(pe);
		}else{
			/**
			 * except join_cond with op is "=", another operator can infer some infomation?
			 */
		}
	}else{
		/* more then one level,it means here is more than one join_cond in current node ,they connect by and/or/not*/
		LevelPredEquivlencesList* final_lpes_list;
		for(const auto& level : level_collector){
			/**deep copy pre level pred_expr_equliclence into this level */
			for(const auto& expr : level){
				if(expr->Type() == AbstractPredNodeType::OPERATOR){
					auto cur_op = static_cast<PredOperatorWrap*>(expr);
					switch(cur_op->GetOpType()){
						case PRED_OPERATOR__PRED_OPERATOR_TYPE__AND:{
							//LevelPredEquivlences * lpes = new LevelPredEquivlences();
							LevelPredEquivlencesList* and_lpes_list = new LevelPredEquivlencesList();
							for(int i=0;i<cur_op->ChildSize();i++){
								auto type = cur_op->Child(0)->Type();
								switch (type){
									case AbstractPredNodeType::QUALS:{
										LevelPredEquivlences * lpes = new LevelPredEquivlences();
										auto qual = static_cast<QualsWarp*>(cur_op->Child(i));
										lpes->Insert(qual->GetQual(),false);
										and_lpes_list->Insert(lpes,false);
									}break;
									case AbstractPredNodeType::OPERATOR:{
										switch(cur_op->GetOpType()){
											case PRED_OPERATOR__PRED_OPERATOR_TYPE__AND:{
												auto child_and_lpes_list = cur_op->GetAndLpesList();
												assert(and_lpes_list->Size());
												and_lpes_list->Insert(child_and_lpes_list,false);
											}break;
											case PRED_OPERATOR__PRED_OPERATOR_TYPE__OR:{
												auto or_lpes_list = cur_op->GetOrLpesList();
												assert(or_lpes_list->Size());
												and_lpes_list->Insert(or_lpes_list,false);
											}break;
											case PRED_OPERATOR__PRED_OPERATOR_TYPE__NOT:{
												/**
												 * TODO: not implement yet
												 */
											}break;
											default:{
												std::cerr<<"unknown child operator type :"<< cur_op->GetOpType()<<std::endl;
												exit(-1);
											}
										}
									}break;
									default:{
										std::cerr<<"unknown child type:"<<std::endl;
										exit(-1);
									}
								}
							}
							/* update tree structure,replace current opeator node into lpes node*/
							auto parent = cur_op->GetParent();
							if(!parent){
								final_lpes_list = and_lpes_list;
							}else{
								cur_op->SetAndLpesList(and_lpes_list);
							}
						}break;
						case PRED_OPERATOR__PRED_OPERATOR_TYPE__OR:{
							//std::vector<LevelPredEquivlences*>* or_lpes_list = new std::vector<LevelPredEquivlences*>();
							LevelPredEquivlencesList* or_lpes_list = new LevelPredEquivlencesList();
							std::vector<AbstractPredNode*>or_op_list;
							for(int i=0;i<cur_op->ChildSize();i++){
								auto type = cur_op->Child(0)->Type();
								switch (type){
									case AbstractPredNodeType::QUALS:{
										LevelPredEquivlences * lpes = new LevelPredEquivlences();
										auto qual = static_cast<QualsWarp*>(cur_op->Child(i));
										lpes->Insert(qual->GetQual(),false);
										or_lpes_list->Insert(lpes,false,true);
									}break;
									case AbstractPredNodeType::OPERATOR:{
										switch (cur_op->GetOpType()){
											case PRED_OPERATOR__PRED_OPERATOR_TYPE__AND:{
												auto and_lpes_list = cur_op->GetAndLpesList();
												assert(or_lpes_list->Size());
												or_lpes_list->Insert(and_lpes_list);
											}break;
											case PRED_OPERATOR__PRED_OPERATOR_TYPE__OR:{
												auto child_or_lpes_list = cur_op->GetOrLpesList();
												assert(or_lpes_list->Size());
												or_lpes_list->Insert(child_or_lpes_list);
											}break;
											case PRED_OPERATOR__PRED_OPERATOR_TYPE__NOT:{
												/**
												 * TODO: not implement yet
												 */
											}break;
											default:{
												std::cerr<<"unknown child operator type :"<< cur_op->GetOpType()<<std::endl;
												exit(-1);
											}
										}
									}break;
									default:{
										std::cerr<<"unknown child type:"<<std::endl;
										exit(-1);
									}
								}
							}
							/*For the convenience of representing the OR attribute, we do not directly replace 
							the OR node, but update its members*/
							auto parent = cur_op->GetParent();
							if(!parent){
								final_lpes_list = or_lpes_list;
							}else{
								cur_op->SetOrLpesList(or_lpes_list);
							}
						}break;
						case PRED_OPERATOR__PRED_OPERATOR_TYPE__NOT:{
							/**
							 * TODO: implement this 
							 */
							for(int i=0;i<cur_op->ChildSize();i++){

							}
						}break;
						default:
							std::cerr<<"level_collector:expr type error"<<std::endl;
							return;
					}
				}
			}
    	}
		/*we should merge pre level's lpes with this level*/
		if(total_equivlences_.size()){
			final_lpes_list->Insert(total_equivlences_.back(),false);
		}
		total_equivlences_.push_back(final_lpes_list);
	}
}

/**
 *  RangeConstrainedDecompose: merge expr form bottom to top
 *  we first get preds value on parent = 2, use a stack to merge, stop until
 *  the stack onlu has one/two element,then we get preds value on parent =1,repeated
 *  action above
 */
void LevelManager::RangeConstrainedDecompose(PredExpression * root){
    std::vector<std::vector<AbstractPredNode*>> level_collector;
	ExprLevelCollect(root,level_collector);
    /**
     * TODO: we must ensure left is the predicate such as t.a and so on,may be 
     * we need check here before insert it into pred_map;
    */
   	if(!level_collector.size()){

	}else if(level_collector.size()==1){
        /*if filter only has one predicate,then the expr root is qual*/
        assert(level_collector[0].size()==1);
        //assert(level_collector[0][0]->expr_case == PRED_EXPRESSION__EXPR_QUAL);
        //auto qual = level_collector[0][0]->qual;
    }else{
		for(const auto& level : level_collector){

		}
	}
}

/**
 * Insert: just for join_cond with "="
 */
bool PredEquivlence::Insert(const std::string& s){
	auto ret = set_.insert(s);
	if(ret.second)return true;
	return false;
}

bool PredEquivlence::Serach(Quals* quals,bool only_left){
	assert(quals);
	bool left = set_.find(quals->left) != set_.end();
	if(only_left){
		return left;
	}else{
		bool right = set_.find(quals->right) != set_.end();
		return left || right;
	}
}

/**
 * Serach: check if two pred equivlences can be merged
 */
bool PredEquivlence::Serach(PredEquivlence* pe){
	assert(pe);
	auto set = pe->GetPredSet();
	for(const auto& pred_name : set){
		if(set_.find(pred_name) != set_.end()){
			return true;
		}
	}
	return false;
}

/*
* Update: update equivlence ranges,we should check if the expr is in set
*/
bool PredEquivlence::UpdateRanges(Quals* qual){
	if(Serach(qual)){
		return true;
	}else{
		/**
		 * TODO: maybe we can use insert and then update ranges,such as [] operator in std::map
		 */
		return false;
	}
}

bool PredEquivlence::Compare(PredEquivlence* pe){

	return true;
}

bool PredEquivlence::Copy(PredEquivlence* pe){

	return true;
}

void PredEquivlence::ShowPredEquivlence(){
	
}

/**
 * LevelPredEquivlences::Insert
 */
bool LevelPredEquivlences::Insert(Quals* quals,bool only_left,bool is_or){
	assert(quals);
	
	return true;
}

/**
 * LevelPredEquivlences::Insert
 */
bool LevelPredEquivlences::Insert(LevelPredEquivlences* lpes){
	assert(lpes);
	for(const auto& pe : *lpes){
		PredEquivlence* new_pe =  new PredEquivlence();
		std::vector<PredEquivlence*>merge_pe_list;
		if(Serach(pe,merge_pe_list)){
			if(!MergePredEquivlences(new_pe,merge_pe_list)){
				return false;
			}
		}else{
			pe->Copy(new_pe);
			level_pe_sets_.insert(new_pe);
		}
	}
	return true;
}

/**
 * LevelPredEquivlences::MergePredEquivlences,here 
 */
bool LevelPredEquivlences::MergePredEquivlences(PredEquivlence* new_pe,const std::vector<PredEquivlence*>& merge_pe_list){
	if(new_pe){
		new_pe = new PredEquivlence();
	}

	std::set<std::string>pred_name_set;
	for(const auto& mpe : merge_pe_list){
		auto pred_set = mpe->GetPredSet();
		for(const auto& pred_name: pred_set){
			pred_name_set.insert(pred_name);
		}
		switch(mpe->GetType()){
			case PType::EQUAL:
			case PType::RANGE:{
				
			}break;
			case PType::LIST:{

			}break;
			case PType::SUBQUERY:{

			}break;
			default:{
				std::cerr<<"unknow type of pe"<<std::endl;
				exit(-1);
			}
		}

	}
	return true;
}

/**
 * LevelPredEquivlences::Serach: use before insert new pred_equivlence into level_pred_equivlences,
 * ret: 
 * 	- true: we can merge some pes into one new eq
 * 		- example: 
 * 			before: [A.a,B.b],[C.c]
 * 			insert: [B.b,C.c]
 * 			after:  [A.a,B.b,C.c]
 * 	- false: we can merge new pe into any pes, instead ,we need create and insert a new pe into lpes
 * 		- example: 
 * 			before: [A.a,B.b],[C.c]
 * 			insert: [D.d]
 * 			after:  [A.a,B.b],[C.c],[D.d]
 */
bool LevelPredEquivlences::Serach(PredEquivlence* pe, std::vector<PredEquivlence*>& pe_idx_list){
	assert(pe);
	bool ret = false;
	for(const auto& item : level_pe_sets_){
		if(item->Serach(pe)){
			pe_idx_list.push_back(item);
			ret = true;
		}
	}
	return ret;
}

bool LevelPredEquivlences::Serach(Quals* quals, std::vector<PredEquivlence*>& pe_idx_list){
	assert(quals);
	return true;
}

/**
 * LevelPredEquivlences::Copy
 */
bool LevelPredEquivlences::Copy(LevelPredEquivlences* lpes){
	assert(lpes);
	for(const auto& pe : *lpes){
		PredEquivlence* new_pred = new PredEquivlence();
		pe->Copy(new_pred);
		lpes->Insert(new_pred);
	}
	return true;
}

/**
 * LevelPredEquivlencesList::Insert 
 */
bool LevelPredEquivlencesList::Insert(LevelPredEquivlences* pe,bool only_left,bool is_or){
	assert(pe);
	if(!is_or){
		lpes_list_.push_back(pe);
	}else{
		for(auto& dst : lpes_list_){
			dst->Insert(pe);
		}
	}
	/**
	 * TODO: merge
	 **/
	return true;
}

/**
 * LevelPredEquivlencesList::Insert:
 * 1.or_model: directly add all level_pred_equivlences in lpes_list into current lpes_list
 * 2.and_model: here we should pair wise union
 * 				for example:
 *					dst: {[A.a,B.b],[C.c]},{[A.a,B.c],[D.d]}
 *              	src: {[B.b,C.c]}
 * 					--> dst': {[A.a,B.b,C.c]},{[A.a,B.c],[B.b,C.c],[D.d]}
 */
bool LevelPredEquivlencesList::Insert(LevelPredEquivlencesList* lpes_list,bool is_or){
	assert(lpes_list);
	if(!is_or){
		for(const auto& lpes: *lpes_list){
			Insert(lpes,false);
		}
	}else{
		size_t sz = lpes_list->Size();
		while(sz--){
			for(const auto& dst : lpes_list_){
				LevelPredEquivlences* new_lpes = new LevelPredEquivlences();
				dst->Copy(new_lpes);
				lpes_list_.push_back(new_lpes);
			}
		}
		for(auto& src : *lpes_list){
			for(auto& dst : lpes_list_){
				dst->Insert(src);
			}
		}

		/**
		 * TODO: merge, best metod isn't merge lpes here, instead, we wish merge before each Insert
		 * */
	}
	return true;
}

/**
 * LevelCollect: For exprs in node such as join_cond,filter,one_time_filter....
 */
void LevelManager::ExprLevelCollect(PredExpression * tree, std::vector<std::vector<AbstractPredNode*>>& level_collector){
    
	std::vector<PredExpression *>levels;
    std::vector<PredExpression *>tmp_levels;

	std::vector<AbstractPredNode *>wrap_levels;
	std::vector<AbstractPredNode *>wrap_tmp_levels;

	AbstractPredNode * root = nullptr;
	if(tree->expr_case == PRED_EXPRESSION__EXPR_QUAL){
		root = new QualsWarp(tree->qual);
	}else if(tree->expr_case == PRED_EXPRESSION__EXPR_OP){
		root = new PredOperatorWrap(tree->op->type);
	}else{
		std::cerr<<"unknow type of pred expression root type"<<std::endl;
        return;
	}

	levels.push_back(tree);
    wrap_levels.push_back(root);

    while(levels.size()){
        size_t sz = levels.size();
        for(size_t i=0;i<sz;i++){
			if(levels[i]->expr_case == PRED_EXPRESSION__EXPR_OP){
				auto node = static_cast<PredOperatorWrap*>(wrap_levels[i]);
				for(size_t j=0;j<levels[i]->op->n_childs;j++){
					AbstractPredNode * new_child = nullptr;
					auto child = levels[i]->op->childs[j];
					if(child->expr_case == PRED_EXPRESSION__EXPR_QUAL){
						new_child = new QualsWarp(child->qual);
					}else if(child->expr_case == PRED_EXPRESSION__EXPR_OP){
						new_child = new PredOperatorWrap(child->op->type);
					}else{
						std::cerr<<"unknow type of pred expression root type"<<std::endl;
						return;
					}
					node->AddChild(new_child);
					new_child->SetParent(node);
					wrap_tmp_levels.push_back(new_child);
					tmp_levels.push_back(levels[i]);
				}
			}else if(levels[i]->expr_case == PRED_EXPRESSION__EXPR_QUAL){
				/*no childs , nothing to do*/
			}else{
				std::cerr<<"unknow type of pred expression type"<<std::endl;
            	return;
			}
        }
        level_collector.push_back(wrap_levels);
        
		levels.clear();
		wrap_levels.clear();

        std::swap(levels,tmp_levels);
		std::swap(wrap_levels,wrap_tmp_levels);
    }
    std::reverse(level_collector.begin(),level_collector.end());
}
