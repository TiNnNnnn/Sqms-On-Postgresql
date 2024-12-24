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
		/*actully,maybe its the most common condition*/ 
        assert(level_collector[0].size()==1);
        assert(level_collector[0][0]->Type() == AbstractPredNodeType::QUALS);
        auto qual = static_cast<QualsWarp*>(level_collector[0][0]);			
		// if(strcmp(qual->,"=")){
		// 	//PredEquivlence* pe = new PredEquivlence();
		// 	//pe->Insert(qual->left);
		// 	//pe->Insert(qual->right);
		// 	//lpes->Insert(pe);
		// 	//equlivlences_.push_back(lpes);
		// }else{
		// 	/**
		// 	 * except join_cond with op is "=", another operator can infer some infomation?
		// 	 */
		// }
	}else{
		/* more then one level,it means here is more than one join_cond in current node ,they connect by and/or/not*/
		for(const auto& level : level_collector){
			/**deep copy pre level pred_expr_equliclence into this level */
			for(const auto& expr : level){
				if(expr->Type() == AbstractPredNodeType::OPERATOR){
					auto cur_op = static_cast<PredOperatorWrap*>(expr);
					switch(cur_op->GetOpType()){
						case PRED_OPERATOR__PRED_OPERATOR_TYPE__AND:{
							LevelPredEquivlences * lpes = new LevelPredEquivlences();
							int child_merge_cnt = 0;
							std::vector<AbstractPredNode*>or_op_list;
							for(int i=0;i<cur_op->ChildSize();i++){
								auto type = cur_op->Child(0)->Type();
								switch (type){
									case AbstractPredNodeType::QUALS:{
										auto qual = static_cast<QualsWarp*>(cur_op->Child(i));
										lpes->Insert(qual->GetQual(),false);
										child_merge_cnt++;
									}break;
									case AbstractPredNodeType::LEVELPREDEQUIVLENCES:{
										auto child_lpes = static_cast<LevelPredEquivlences*>(cur_op->Child(i));
										for(const auto& pe : child_lpes->LevelPeList()){
											lpes->Insert(pe);
										}
										child_merge_cnt++;
									}break;	
									case AbstractPredNodeType::OPERATOR:{	
										if(cur_op->GetOpType() != PRED_OPERATOR__PRED_OPERATOR_TYPE__OR){
											std::cerr<<"error child operator type :"<< cur_op->GetOpType()<<std::endl;
											exit(-1);
										}else{
											or_op_list.push_back(cur_op->Child(i));
										}
									}break;
									default:{
										std::cerr<<"unknown child type:"<<std::endl;
										exit(-1);
									}
								}
							}
							int new_child_size = cur_op->ChildSize() - child_merge_cnt + 1;
							auto parent = cur_op->GetParent();
							if(!parent){
								
							}else{
								assert(parent->Type() == AbstractPredNodeType::OPERATOR);
								assert(new_child_size-1 == or_op_list.size());
								auto p = static_cast<PredOperatorWrap*>(parent);
								for(int i = 0;i<new_child_size-1;i++){
									p->ReSetChild(or_op_list[i],i);
								}
								p->ReSetChild(lpes,new_child_size-1);
								p->SetChildSize(new_child_size);
							}
						}break;
						case PRED_OPERATOR__PRED_OPERATOR_TYPE__OR:{

						}break;
						case PRED_OPERATOR__PRED_OPERATOR_TYPE__NOT:{
							
						}break;
						default:
							std::cerr<<"level_collector:expr type error"<<std::endl;
							return;
					}
				}
			}
    	}
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

bool PredEquivlence::Serach(Quals* qual){
	if(set_.find(qual->left) != set_.end()){
		return true;
	}else{
		return false;
	}
}

/*
* Update: upodate equivlence ranges,we should check if the expr is in set
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
