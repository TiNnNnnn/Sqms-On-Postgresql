#include "collect/level_mgr.hpp"
#include <algorithm>

/**
 * LevelManager::Format
 */
bool LevelManager::Format(){
	ComputeTotalClass();
	return true;
}

/**
 * LevelManager::PrintPredEquivlences
 */
bool LevelManager::PrintPredEquivlences(){
	if(debug)
	 	ShowTotalPredClass();
}

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
                tmp_levels.push_back(levels[i]->childs[j]);
            }
        }
        level_collector.push_back(levels);
        levels.clear();
        std::swap(levels,tmp_levels);
    }
    std::reverse(level_collector.begin(),level_collector.end());
	
	total_height_ = level_collector.size();

    for(auto &lc : level_collector){
        ComputeLevelClass(lc);
    }

	// if(debug)
	// 	ShowTotalPredClass();
}

void LevelManager::ComputeLevelClass(const std::vector<HistorySlowPlanStat*>& list){
    /*calulate equivlences first*/
	ReSetAllPreProcessd();
	for(const auto& s : list){
		cur_hsps_ = s;
		NodeCollector* new_node_collector =  new NodeCollector();
		nodes_collector_map_[s] = new_node_collector;

		first_pred_check_ = true;
        HandleEquivleces(s);
	}
	/*calulate other attrs based on equivlences*/
	for(const auto& s : list){
		cur_hsps_ = s;
	 	HandleNode(s);
	}
	++cur_height_;
}

void LevelManager::HandleEquivleces(HistorySlowPlanStat* hsps){
	PredEquivalenceClassesDecompase(hsps->join_cond_expr_tree);
	PredEquivalenceClassesDecompase(hsps->join_filter_expr_tree);
	PredEquivalenceClassesDecompase(hsps->filter_tree);
	PredEquivalenceClassesDecompase(hsps->one_time_filter_tree);
}

void LevelManager::HandleNode(HistorySlowPlanStat* hsps){
	NodeTag node_tag = static_cast<NodeTag>(hsps->node_tag);
	TblDecompase(hsps);
	OutputDecompase(hsps);
	GroupKeyDecompase(hsps);
	SortKeyDecompase(hsps);
    switch(node_tag){
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
		case T_HashJoin:{
			if(hsps->join_type == ""){

			}
		}break;
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
			std::cerr<<"unknown node tag"<<std::endl;
			break;  
    }
}

void PrintIndent(int depth) {
    for (int i = 0; i < depth; ++i) {
        std::cout << "  ";
    }
}

void PrintLine(int len){
	for(int i=0;i<len;++i){
		std::cout << "-";
	}
	std::cout<<std::endl;
}

/**
 * TblDecompase: collect all tables for each levels
 */
void LevelManager::TblDecompase(HistorySlowPlanStat* hsps){
	LevelTblList* final_tb_list = nullptr;
	LevelTblList* node_final_tbl_list = nullptr;

	bool same_level_need_merged = true;
	if(cur_height_ == (int)total_outputs_.size()){
		total_tbls_.push_back(nullptr);	
		same_level_need_merged = false;
	}

	/*relation name is nullptr, it indecate is not a leaf node of plan*/
	if(!strlen(hsps->object_name)){
		
		node_final_tbl_list = new LevelTblList();
		if(cur_height_ >= 1){
			for(size_t i =0;i<cur_hsps_->n_childs;i++){
				auto child_tbls = nodes_collector_map_[cur_hsps_->childs[i]]->node_tbls_;
				node_final_tbl_list->Insert(child_tbls);	
			}
		}
		nodes_collector_map_[cur_hsps_]->node_tbls_ = node_final_tbl_list;

		if(same_level_need_merged){
			return;
		}
		final_tb_list = new LevelTblList();
		if(cur_height_ >= 1){
			final_tb_list->Insert(total_tbls_[cur_height_-1]);
		}
		total_tbls_[cur_height_] = final_tb_list;
		return;
	}

	final_tb_list = new LevelTblList();
	node_final_tbl_list = new LevelTblList();

	final_tb_list->Insert(hsps->object_name);
	node_final_tbl_list->Insert(hsps->object_name);
	
	if(same_level_need_merged){
		final_tb_list->Insert(total_tbls_[cur_height_]);
	}
	
	if(cur_height_ >=1){
		final_tb_list->Insert(total_tbls_[cur_height_-1]);

		for(size_t i =0;i<cur_hsps_->n_childs;i++){
			auto child_tbls = nodes_collector_map_[cur_hsps_->childs[i]]->node_tbls_;
			node_final_tbl_list->Insert(child_tbls);	
		}		
	}

	total_tbls_[cur_height_] = final_tb_list;
	nodes_collector_map_[cur_hsps_]->node_tbls_ = node_final_tbl_list;
}

/**
 * AttrDecompase: caluate equivalance class for output columns for each levels 
 */
void LevelManager::OutputDecompase(HistorySlowPlanStat* hsps){
	if(hsps->n_output == 0){
		std::cerr<<"output is empty"<<std::endl;
		exit(-1);
	}
	
	LevelOutputList* final_lo_list = new LevelOutputList();
	LevelOutputList* node_final_lo_list = new LevelOutputList();

	bool same_level_need_merged = true;
	if(cur_height_ == (int)total_outputs_.size()){
		total_outputs_.push_back(nullptr);	
		same_level_need_merged = false;
	}

	auto lpes_list =  total_equivlences_[cur_height_];
	final_lo_list->Insert(lpes_list,hsps);

	auto node_lpes_list = nodes_collector_map_[cur_hsps_]->node_equivlences_;
	node_final_lo_list->Insert(node_lpes_list,cur_hsps_);

	if(same_level_need_merged){
		final_lo_list->Insert(total_outputs_.back());
	}

	total_outputs_[cur_height_] = final_lo_list;
	nodes_collector_map_[cur_hsps_]->node_outputs_ = node_final_lo_list;
}

void LevelOutputList::Insert(LevelPredEquivlencesList* lpes_list,HistorySlowPlanStat* hsps){
	assert(lpes_list);
	
	if(lpes_list->Size()){
		output2pe_list_.resize(lpes_list->Size());
		output_extend_list_.resize(lpes_list->Size());
	}else{
		output2pe_list_.resize(1);
		output_extend_list_.resize(1);
	}

	/* lpes_list == nullptr , just for node_collector */
	if(!lpes_list->Size()){
		for(size_t i=0;i<hsps->n_output;i++){
			std::string attr(hsps->output[i]);
			output2pe_list_[0].insert(std::make_pair(attr,nullptr));
			output_extend_list_[0].insert(attr);
		}
		return;
	}
	
	int lpes_idx = 0;
	for(const auto& e: *lpes_list){
		for(size_t i=0;i<hsps->n_output;i++){
			std::string attr(hsps->output[i]);
			PredEquivlence* pe = nullptr;
			if(e->Serach(attr,pe)){
				assert(pe);
				output2pe_list_[lpes_idx][attr] = pe;
				const auto& set = pe->GetPredSet();
				for(const auto& s : set){
					output_extend_list_[lpes_idx].insert(s);	
				}
			}else{
				output2pe_list_[lpes_idx].insert(std::make_pair(attr,nullptr));
				output_extend_list_[lpes_idx].insert(attr);
			}
		}
		lpes_idx++;
	}
}

void LevelOutputList::Insert(LevelOutputList* lo_list){
	assert(lo_list);
	for(size_t i = 0;i < output2pe_list_.size();i++){
		auto& dst_map = output2pe_list_[i];
		auto src_map = lo_list->output2pe_list_[i];
		for(const auto& src_attr: src_map){
			if(dst_map.find(src_attr.first) != dst_map.end()){
				/*here we need check if it is a equivlence,if not,it's a error*/
			}else{
				dst_map.insert(src_attr);
			}
		}

		auto& dst_set = output_extend_list_[i];
		auto src_set = lo_list->output_extend_list_[i];
		for(const auto& src_attr : src_set){
			dst_set.insert(src_attr);
		}
	}
}

void LevelOutputList::ShowLevelOutputList(int depth){
	PrintIndent(depth);
	for(size_t i = 0; i < output2pe_list_.size(); i++){
		std::cout<<"output: (";
		if(i)std::cout<<",";
		int j = 0;	
		for(const auto& e : output2pe_list_[i]){
			if(j) std::cout<<",";
			if(e.second == nullptr){
				std::cout<<e.first;
			}else{
				std::cout<<e.first<<"->";
				e.second->ShowPredEquivlenceSets(depth);
			}
			j++;
		}
		std::cout<<"), extend_list: (";
		j = 0;
		for(const auto& e : output_extend_list_[i]){
			if(j) std::cout<<",";
			std::cout<<e;
			j++;
		}
		std::cout<<")";
		i++;
	}
	std::cout<<"\n";
}

void LevelManager::SortKeyDecompase(HistorySlowPlanStat* hsps){
	assert(total_equivlences_[cur_height_]);

	bool same_level_need_merged = true;
	if(cur_height_ == (int)total_sorts_.size()){
		total_sorts_.push_back(nullptr);	
		same_level_need_merged = false;
	}
	
	LevelAggAndSortList* final_ls_list = nullptr;
	LevelAggAndSortList* node_final_ls_list = nullptr;

	bool qlabel = strcmp(hsps->group_sort_qlabel,"Sort Key");
	if(qlabel){
		node_final_ls_list = new LevelAggAndSortList(nodes_collector_map_[cur_hsps_]->node_equivlences_);
		if(cur_height_ >= 1){
			for(size_t i=0;i<cur_hsps_->n_childs;i++){
				auto child_sorts = nodes_collector_map_[cur_hsps_->childs[i]]->node_sorts_;
				if(!child_sorts)
					continue;
				node_final_ls_list->Insert(child_sorts);
			}
		}
		if(node_final_ls_list->Size() && !nodes_collector_map_[cur_hsps_]->node_sorts_){
			nodes_collector_map_[cur_hsps_]->node_sorts_ = node_final_ls_list;
		}

		if(same_level_need_merged){
			return;
		}
		final_ls_list = new LevelAggAndSortList(total_equivlences_[cur_height_]);
		if(cur_height_ >= 1 && GetPreProcessed(PreProcessLabel::SORT)){
			final_ls_list->Insert(total_sorts_[cur_height_-1]);
			SetPreProcessed(PreProcessLabel::SORT,true);
		}
		total_sorts_[cur_height_] = final_ls_list;
	}else{
		final_ls_list = new LevelAggAndSortList(total_equivlences_[cur_height_]);
		node_final_ls_list = new LevelAggAndSortList(nodes_collector_map_[cur_hsps_]->node_equivlences_);

		final_ls_list->Insert(hsps,"Sort Key");
		node_final_ls_list->Insert(hsps,"Sort Key");

		if(same_level_need_merged){
			final_ls_list->Insert(total_sorts_.back());
		}
		if(cur_height_ >= 1 && GetPreProcessed(PreProcessLabel::SORT)){
			final_ls_list->Insert(total_sorts_[cur_height_-1]);
			SetPreProcessed(PreProcessLabel::SORT,true);

			for(size_t i=0;i<cur_hsps_->n_childs;i++){
				auto child_sorts = nodes_collector_map_[cur_hsps_->childs[i]]->node_sorts_;
				if(!child_sorts)
					continue;
				node_final_ls_list->Insert(child_sorts);
			}
		}

		nodes_collector_map_[cur_hsps_]->node_sorts_ = node_final_ls_list;
		total_sorts_[cur_height_] = final_ls_list;		
	}	

}

void LevelManager::GroupKeyDecompase(HistorySlowPlanStat* hsps){
	assert(total_equivlences_[cur_height_]);

	bool same_level_need_merged = true;
	if(cur_height_ == (int)total_aggs_.size()){
		total_aggs_.push_back(nullptr);	
		same_level_need_merged = false;
	}

	LevelAggAndSortList* final_la_list = nullptr;
	LevelAggAndSortList* node_final_la_list = nullptr;

	if(strcmp(hsps->group_sort_qlabel,"Group Key")){
		
		node_final_la_list = new LevelAggAndSortList(nodes_collector_map_[cur_hsps_]->node_equivlences_);
		if(cur_height_ >= 1){
			for(size_t i=0;i<cur_hsps_->n_childs;i++){
				auto child_aggs = nodes_collector_map_[cur_hsps_->childs[i]]->node_aggs_;
				if(!child_aggs)
					continue;
				node_final_la_list->Insert(child_aggs);
			}
		}
		if(node_final_la_list->Size() && !nodes_collector_map_[cur_hsps_]->node_aggs_){
			nodes_collector_map_[cur_hsps_]->node_aggs_ = node_final_la_list;
		}

		if(same_level_need_merged){
			return;
		}
		final_la_list = new LevelAggAndSortList(total_equivlences_[cur_height_]);
		if(cur_height_ >= 1 && !GetPreProcessed(PreProcessLabel::AGG)){
			final_la_list->Insert(total_aggs_[cur_height_-1]);
			SetPreProcessed(PreProcessLabel::AGG,true);
		}
		total_aggs_[cur_height_] = final_la_list;
	}else if(!strcmp(hsps->group_sort_qlabel,"Group Key")){

		final_la_list = new LevelAggAndSortList(total_equivlences_[cur_height_]);
		node_final_la_list = new LevelAggAndSortList(nodes_collector_map_[cur_hsps_]->node_equivlences_);

		final_la_list->Insert(hsps,"Group Key");
		node_final_la_list->Insert(hsps,"Group Key");

		if(same_level_need_merged){
			final_la_list->Insert(total_aggs_.back());
		}
		if(cur_height_ >= 1){
			if(!GetPreProcessed(PreProcessLabel::AGG)){
				final_la_list->Insert(total_aggs_[cur_height_-1]);
				SetPreProcessed(PreProcessLabel::AGG,true);
			}

			for(size_t i=0;i<cur_hsps_->n_childs;i++){
				auto child_aggs = nodes_collector_map_[cur_hsps_->childs[i]]->node_aggs_;
				if(!child_aggs)
					continue;
				node_final_la_list->Insert(child_aggs);
			}
		}

		nodes_collector_map_[cur_hsps_]->node_aggs_ = node_final_la_list;
		total_aggs_[cur_height_] = final_la_list;		
	}
}

void AggAndSortEquivlence::Init(HistorySlowPlanStat* hsps,LevelPredEquivlences* lpes){
	if(!lpes){
		for(size_t i=0;i<hsps->n_group_sort_keys;i++){
			std::string key(hsps->group_sort_keys[i]->key);
			key2pe_.insert(std::make_pair(key,nullptr));
			extends_.insert(key);
		}		
		return;
	}

	for(size_t i = 0;i<hsps->n_group_sort_keys;i++){
		std::string key(hsps->group_sort_keys[i]->key);
		PredEquivlence* pe = nullptr;
		if(lpes->Serach(key,pe)){
			assert(pe);
			key2pe_[key] = pe;
			const auto& set = pe->GetPredSet();
			for(const auto& s : set){
				extends_.insert(s);	
			}
		}else{
			key2pe_.insert(std::make_pair(key,nullptr));
			extends_.insert(key);
		}		
	}
}

void AggAndSortEquivlence::ShowAggEquivlence(int depth){
	std::cout<<tag_<<":(";
	int j = 0;
	for(const auto& e : key2pe_){
		if(j) std::cout<<",";
		if(e.second == nullptr){
			std::cout<<e.first;
		}else{
			std::cout<<e.first<<"->";
			e.second->ShowPredEquivlenceSets(depth);
		}
		j++;
	}
	std::cout<<"), extends: (";
	j = 0;
	for(const auto& e : extends_){
		if(j) std::cout<<",";
		std::cout<<e;
		j++;
	}
	std::cout<<")";
}

void LevelAggAndSortEquivlences::Insert(AggAndSortEquivlence* ae){
	level_agg_sets_.push_back(ae);
}

void LevelAggAndSortEquivlences::Insert(LevelAggAndSortEquivlences* level_ae){
	for(const auto& ae : level_ae->level_agg_sets_){
		level_agg_sets_.push_back(ae);
	}
}

void LevelAggAndSortEquivlences::ShowLevelAggEquivlence(int depth){
	PrintIndent(depth);
	std::cout<<"{";
	int idx = 0;
	for(const auto& ae : level_agg_sets_){
		if(idx)std::cout<<",";
		ae->ShowAggEquivlence(depth+1);
		idx++;
	}
	std::cout<<"}\n";
}

void LevelAggAndSortList::Insert(HistorySlowPlanStat* hsps,std::string label){
	assert(lpes_list_);
	if(!lpes_list_->Size()){
		AggAndSortEquivlence* new_agg = new AggAndSortEquivlence(label);
		new_agg->Init(hsps,nullptr);
		LevelAggAndSortEquivlences* new_ae_list = new LevelAggAndSortEquivlences();
		new_ae_list->Insert(new_agg);
		level_agg_list_.push_back(new_ae_list);
	}else{
		for(const auto& lpes: *lpes_list_){			
			AggAndSortEquivlence* new_agg = new AggAndSortEquivlence(label);
			new_agg->Init(hsps,lpes);
			LevelAggAndSortEquivlences* new_ae_list = new LevelAggAndSortEquivlences();
			new_ae_list->Insert(new_agg);
			level_agg_list_.push_back(new_ae_list);
		}
	}
}

void LevelAggAndSortList::Insert(LevelAggAndSortList* la_list){
	assert(la_list);

	if(level_agg_list_.empty()){
		LevelAggAndSortEquivlences *new_dst_lae = new LevelAggAndSortEquivlences();
		for(const auto& src_lae : la_list->GetLevelAggList()){
			new_dst_lae->Insert(src_lae);
		}
		level_agg_list_.push_back(new_dst_lae);
		return;
	}

	for(const auto& dst_lae: level_agg_list_){
		for(const auto& src_lae : la_list->GetLevelAggList()){
			dst_lae->Insert(src_lae);
		}
	}
}

void LevelAggAndSortList::Copy(LevelAggAndSortList* la_list){}


void LevelAggAndSortList::ShowLevelAggAndSortList(int depth){
	for(const auto& lae: level_agg_list_){
		lae->ShowLevelAggEquivlence(depth+1);
	}	
}


void LevelTblList::Insert(LevelTblList* lt_list){
	assert(lt_list);
	for(const auto& name : lt_list->GetTblSet()){
		tbl_set_.insert(name);
	}
}

void LevelTblList::ShowLevelTblList(int depth){
	std::cout<<"{";
	int idx = 0;
	for(const auto& name: tbl_set_){
		if(idx++)std::cout<<",";
		std::cout<<name;
	}
	std::cout<<"}\n";
}

/**
 * EquivalenceClassesDecompase:calulate equivalance class for predicates for
 * each levels
 */
void LevelManager::PredEquivalenceClassesDecompase(PredExpression* root){
	bool same_level_need_merged = true;
	if(cur_height_ ==  (int)total_equivlences_.size()){
		/*
		  if true,it means current level's predicates are still not processed,we 
		  create a new final_lpes_list in total_equivlences_,even if the current
		  pred_expression tree is nullptr;
		*/
		total_equivlences_.push_back(nullptr);
		same_level_need_merged = false;
	}

	LevelPredEquivlencesList* node_final_lpes_list = nullptr;
	LevelPredEquivlencesList* final_lpes_list = nullptr;

	if(!root){
		if(first_pred_check_){
			node_final_lpes_list = new LevelPredEquivlencesList();
			if(cur_height_ >= 1){
				for(size_t i = 0;i<cur_hsps_->n_childs; ++i){
					auto child_eq = nodes_collector_map_[cur_hsps_->childs[i]]->node_equivlences_;
					node_final_lpes_list->Insert(child_eq);
				}
			}
			nodes_collector_map_[cur_hsps_]->node_equivlences_ = node_final_lpes_list;
			first_pred_check_ = false;
		}
		/*we should merge current level's pre lpes with this level*/
		if(same_level_need_merged){
			return;
		}
		final_lpes_list = new LevelPredEquivlencesList();
		/*we should merge pre level's lpes with this level*/
		if(cur_height_ >= 1 && !GetPreProcessed(PreProcessLabel::PREDICATE)){
			final_lpes_list->Insert(total_equivlences_[cur_height_-1],false);
			SetPreProcessed(PreProcessLabel::PREDICATE,true);
		}
		total_equivlences_[cur_height_] = final_lpes_list;
		return;
	}
	
	std::vector<std::vector<AbstractPredNode*>> level_collector;
	ExprLevelCollect(root,level_collector);

	assert(level_collector.size());	
	if(level_collector.size() == 1){
		/*actually,maybe its the most common condition*/ 
		final_lpes_list = new LevelPredEquivlencesList();
        assert(level_collector[0].size()==1);
        assert(level_collector[0][0]->Type() == AbstractPredNodeType::QUALS);
        
		auto qual = static_cast<QualsWarp*>(level_collector[0][0])->GetQual();
		qual->hsps = cur_hsps_;
		PredEquivlence* pe = new PredEquivlence(qual);
		LevelPredEquivlences * lpes = new LevelPredEquivlences();
		lpes->Insert(pe);
		final_lpes_list->Insert(lpes);
	}else{
		/* more then one level,it means here is more than one join_cond in current node ,they connect by and/or/not*/
		for(const auto& level : level_collector){
			/**deep copy pre level pred_expr_equliclence into this level */
			for(const auto& expr : level){
				if(expr->Type() == AbstractPredNodeType::OPERATOR){
					auto cur_op = static_cast<PredOperatorWrap*>(expr);
					switch(cur_op->GetOpType()){
						case PRED_OPERATOR__PRED_OPERATOR_TYPE__AND:{
							//LevelPredEquivlences * lpes = new LevelPredEquivlences();
							LevelPredEquivlencesList* and_lpes_list = new LevelPredEquivlencesList();
							for(size_t i=0;i<cur_op->ChildSize();i++){
								auto type = cur_op->Child(0)->Type();
								switch (type){
									case AbstractPredNodeType::QUALS:{
										//LevelPredEquivlences * lpes = new LevelPredEquivlences();
										auto qual = static_cast<QualsWarp*>(cur_op->Child(i))->GetQual();
										qual->hsps = cur_hsps_;
										LevelPredEquivlences * lpes = new LevelPredEquivlences();
										lpes->Insert(qual,false);
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
							for(size_t i=0;i<cur_op->ChildSize();i++){
								auto type = cur_op->Child(0)->Type();
								switch (type){
									case AbstractPredNodeType::QUALS:{
										//LevelPredEquivlences * lpes = new LevelPredEquivlences();
										auto qual = static_cast<QualsWarp*>(cur_op->Child(i))->GetQual();
										qual->hsps = cur_hsps_;
										LevelPredEquivlences * lpes = new LevelPredEquivlences();
										lpes->Insert(qual,false);
										or_lpes_list->Insert(lpes,true);
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
							for(size_t i=0;i<cur_op->ChildSize();i++){

							}
						}break;
						default:
							std::cerr<<"level_collector:expr type error"<<std::endl;
							return;
					}
				}
			}
    	}
	}

	node_final_lpes_list = new LevelPredEquivlencesList();
	final_lpes_list->Copy(node_final_lpes_list);

	/*we should merge current level's pre lpes with this level*/
	if(same_level_need_merged){
		final_lpes_list->Insert(total_equivlences_[cur_height_],false);
	}
	if(nodes_collector_map_[cur_hsps_]->node_equivlences_ && nodes_collector_map_[cur_hsps_]->node_equivlences_->Size())
		node_final_lpes_list->Insert(nodes_collector_map_[cur_hsps_]->node_equivlences_);
	
	/*we should merge pre level's lpes with this level*/
	if(cur_height_ >= 1){	
		if(!GetPreProcessed(PreProcessLabel::PREDICATE)){
			final_lpes_list->Insert(total_equivlences_[cur_height_-1],false);
			SetPreProcessed(PreProcessLabel::PREDICATE,true);
		}
		if(first_pred_check_){
			for(size_t i = 0;i<cur_hsps_->n_childs; ++i){
				auto child_eq = nodes_collector_map_[cur_hsps_->childs[i]]->node_equivlences_;
				node_final_lpes_list->Insert(child_eq);
			}
			first_pred_check_ = false;
		}
	}
	/**
	 * here we try to extract the equivalence class from subquery 
	*/
	auto sub_final_lpes_list = new LevelPredEquivlencesList();
	for(const auto& lpes_list : *final_lpes_list){
		for(const auto& pe : *lpes_list){
			for(const auto& sublink : pe->GetSubLinkLevelPeLists()){
				auto sub_top_output =  sublink.second->GetTotalOutput()[0];
				/*each item in output2pelist is or*/
				for(size_t j = 0; j < sub_top_output->GetOutput2PeList().size(); ++j){
					auto& u_map = sub_top_output->GetOutput2PeList()[j];
					assert(u_map.size());
					if(u_map.size() == 1){
						/*subquery oputput*/
						auto sub_set = sub_top_output->GetOutputExtendList()[j];
						/* main query (lefthand) */
						auto pe_set = pe->GetPredSet(); 
						for(const auto& attr: sub_set){
							if(pe_set.find(attr) != pe_set.end()){
								auto sub_top_pe_list = sublink.second->GetTotalEquivlences()[0];
								sub_final_lpes_list->Insert(sub_top_pe_list,false);
								break;
							}
						}
					}	
				}
			}
		}
	}
	if(sub_final_lpes_list->Size())
		final_lpes_list->Insert(sub_final_lpes_list,false);
	/*update toal_equivlences*/
	total_equivlences_[cur_height_] = final_lpes_list;
	nodes_collector_map_[cur_hsps_]->node_equivlences_ = node_final_lpes_list;
}

/**
 * PredEquivlenceRange::Serach: Check if there is any intersection between the two range
 * TODO: 24-12-27 it a rough version
 */
bool PredEquivlenceRange::Serach(PredEquivlenceRange* range){
	assert(range);
	switch(type_){
		case PType::NOT_EQUAL:{
			if(range->PredType() == PType::NOT_EQUAL){
				return range->UpperLimit() == UpperLimit();	
			}else{
				return false;
			}
		}break;
		case PType::EQUAL:{
			if(range->PredType() == PType::NOT_EQUAL){
				return range->UpperLimit() == UpperLimit();	
			}else{
				return false;
			}
		}break;
		case PType::RANGE:{
			if(range->PredType() == PType::RANGE){
				bool intersected = 
					(range->UpperLimit() < LowerLimit())||
					(range->LowerLimit() > UpperLimit())||
					(range->UpperLimit() == LowerLimit() && (!GetLowerBoundaryConstraint()||!range->GetUpperBoundaryConstraint()))||
					(range->LowerLimit() == UpperLimit() && (!GetUpperBoundaryConstraint()||!range->GetLowerBoundaryConstraint()));
				return !intersected;
			}else if (range->PredType() == PType::EQUAL){
				/*if this qual num is in the range of this->ranges,then we just convert ptype into PType::EQUAL*/
				bool intersected = 
					((range->UpperLimit() < LowerLimit())) ||
					(range->UpperLimit() == LowerLimit() && (!GetLowerBoundaryConstraint()||!range->GetUpperBoundaryConstraint()));
				return !intersected;
			}else{
				return false;
			}
		}break;
		case PType::LIST:{
			return false;
		}break;
		case PType::SUBQUERY:{
			return false;
		}break;
		default:{
			std::cerr<<"unknow type of pe"<<std::endl;
			exit(-1);
		}
	}	
	return false;
}

void PredEquivlenceRange::Copy(PredEquivlenceRange* new_range){
	new_range->SetPredType(type_);
	new_range->SetLowerLimit(lower_limit_);
	new_range->SetUpperLimit(upper_limit_);
	new_range->SetBoundaryConstraint(boundary_constraint_);
	new_range->SetList(list_);
}

void PredEquivlenceRange::PrintPredEquivlenceRange(int depth){
	std::string output;
	auto ll = lower_limit_ == LOWER_LIMIT?"-∞":lower_limit_;
	auto ul = upper_limit_ == UPPER_LIMIT?"+∞":upper_limit_;
	switch(type_){
		case PType::EQUAL:{
			output += "=";
			output += ll;
		}break;
		case PType::NOT_EQUAL:{
			output += "<>";
			output += ll;
		}break;
		case PType::RANGE:{
			if(boundary_constraint_.first){
				output+="[";
			}else{
				output+="(";
			}
			output += ll;
			output += ",";
			output += ul;
			if(boundary_constraint_.second){
				output+="]";
			}else{
				output+=")";
			}
		}break;
		case PType::LIST:{
			output+="[";
			for(size_t i=0;i<list_.size();i++){
				output += list_[i];
				if(i != list_.size()-1) output +=",";
			}
			output+="]";
		}break;
		case PType::SUBQUERY:{
			output+="SUBQUERY";
		}break;
		default:{
			std::cerr<<"unknow type of pe_range: "<<int(type_)<<std::endl;
			exit(-1);
		}
	}
	std::cout<<output;
}

PredEquivlence::PredEquivlence(Quals* qual){
	assert(qual);
	PType type = QualType(qual);
	PredEquivlenceRange* range = new PredEquivlenceRange(); 
	switch(type){
		case PType::JOIN_EQUAL:{
			set_.insert(qual->left);
			set_.insert(qual->right);
		}break;
		case PType::EQUAL:{
			set_.insert(qual->left);
			range->SetPredType(PType::EQUAL);
			range->SetLowerLimit(qual->right);
			range->SetUpperLimit(qual->right);
			ranges_.insert(range);
		}break;
		case PType::NOT_EQUAL:{
			set_.insert(qual->left);
			auto op = qual->op;
			if(!strcmp(op,"!=") or !strcmp(op,"<>")){
				range->SetPredType(PType::NOT_EQUAL);
				range->SetLowerLimit(qual->right);
				range->SetUpperLimit(qual->right);
			}else if(!strcmp(op,"!~")){
				range->SetPredType(PType::NOT_EQUAL);
				/**
				 * TODO: not implement: regular expression check
				*/
			}
			ranges_.insert(range);
		}break;
		case PType::RANGE:{
			set_.insert(qual->left);
			auto op = qual->op;
			if(!strcmp(op,">")){
				range->SetPredType(PType::RANGE);
				range->SetLowerLimit(qual->right);
				range->SetBoundaryConstraint(std::make_pair(false,true));
			}else if(!strcmp(op,">=")){
				range->SetPredType(PType::RANGE);
				range->SetLowerLimit(qual->right);
				range->SetBoundaryConstraint(std::make_pair(true,true));
			}else if(!strcmp(op,"<")){
				range->SetPredType(PType::RANGE);
				range->SetUpperLimit(qual->right);
				range->SetBoundaryConstraint(std::make_pair(true,false));
			}else if(!strcmp(op,"<=")){
				range->SetPredType(PType::RANGE);
				range->SetUpperLimit(qual->right);
				range->SetBoundaryConstraint(std::make_pair(false,false));
			}else{
				std::cerr<<"unkonw op type of range qual while init pred equivlence"<<std::endl;
				exit(-1);
			}
			ranges_.insert(range);
		}break;
		case PType::LIST:{
			set_.insert(qual->left);
			/**
			 * TODO: not implement yet: we should convert str_list into true list
			 */
			ranges_.insert(range);
		}break;
		case PType::SUBQUERY:{
			std::string l = extract_field(qual->left);
			if(l.empty()){
				set_.insert(qual->left);
			}else{
				set_.insert(l);
			}
			assert(qual->hsps);
			PlanFormatContext* pf_context = new PlanFormatContext();
			/*calualate pred equivlence here*/
			for(size_t i = 0; i < qual->hsps->n_subplans; i++){
				auto sub_level_mgr = std::make_shared<LevelManager>(qual->hsps->subplans[i],nullptr);
				pf_context->SetStrategy(sub_level_mgr);
				pf_context->executeStrategy();
				sublink_level_pe_lists_[qual->hsps->subplans[i]->sub_plan_name] = sub_level_mgr; 
			}
		}break;
		case PType::SUBLINK:{
			std::string l = extract_field(qual->left);
			if(l.empty()){
				set_.insert(qual->left);
			}else{
				set_.insert(l);
			}
			assert(qual->hsps);
			PlanFormatContext* pf_context = new PlanFormatContext();
			/*calualate pred equivlence here*/
			for(size_t i = 0; i < qual->hsps->n_subplans; i++){
				auto sub_level_mgr = std::make_shared<LevelManager>(qual->hsps->subplans[i],nullptr);
				pf_context->SetStrategy(sub_level_mgr);
				pf_context->executeStrategy();
				sublink_level_pe_lists_[qual->hsps->subplans[i]->sub_plan_name] = sub_level_mgr; 
			}
		}break;
		default:{
			std::cerr<<"unkonw type of qual while init pred equivlence"<<std::endl;
			exit(-1);
		}
	}
}
 
bool PredEquivlence::IsOnlyLeft(Quals* qual){
	if(PType::JOIN_EQUAL == QualType(qual)){
		return false;
	}
	return true;
}

/**
 * PredEquivlence::QualType: 
 * - Get qual PType
 * - Normalized qual
 */
PType PredEquivlence::QualType(Quals* qual){
	assert(qual);
	if(strlen(qual->op)){
		if(strlen(qual->use_or)){
			return PType::LIST;
		}else{
			auto& op = qual->op;
			auto left_type = NodeTag(qual->l_type);
			auto right_type = NodeTag(qual->r_type);
			if(PredVariable(left_type) && PredVariable(right_type)){
				if(!strcmp(op,"=")){
					return PType::JOIN_EQUAL;
				}else if(!strcmp(op,">")){
					/*not support*/
				}else if(!strcmp(op,">=")){
					/*not support*/
				}else if(!strcmp(op,"<")){
					/*not support*/
				}else if(!strcmp(op,"<=")){
					/*not support*/
				}else if(!strcmp(op,"!=") or !strcmp(op,"<>")){
					/*not support*/
				}else if(!strcmp(op,"!~")){
					/*not support*/
				}
			}else if(PredVariable(left_type) && !PredVariable(right_type)){
				switch(right_type){
					case T_Const:{
						if(!strcmp(op,"=")){
							return PType::EQUAL;
						}else if(!strcmp(op,">")){
							return PType::RANGE;
						}else if(!strcmp(op,">=")){
							return PType::RANGE;
						}else if(!strcmp(op,"<")){
							return PType::RANGE;
						}else if(!strcmp(op,"<=")){
							return PType::RANGE;
						}else if(!strcmp(op,"!=") or !strcmp(op,"<>")){
							return PType::NOT_EQUAL;
						}else if(!strcmp(op,"!~")){
							return PType::NOT_EQUAL;
						}
					}break;
					case T_SubLink:{
						return PType::SUBLINK;
					}break;
					case T_SubPlan:{
						return PType::SUBQUERY;
					}break;
					default:{
						std::cerr<<"unsupport right value type :"<<right_type<<" of quals"<<std::endl;
					}
				}
			}else if(!PredVariable(left_type) && PredVariable(right_type)){
				/*we wish not appear this condition,but we try to process*/
				std::swap(qual->left,qual->right);
				std::swap(qual->l_type,qual->r_type);
				auto& op = qual->op;
				switch(right_type){
					case T_Const:{
						if(!strcmp(op,"=")){
							return PType::EQUAL;
						}else if(!strcmp(op,">")){
							strcpy(op,"<");
							return PType::RANGE;
						}else if(!strcmp(op,">=")){
							strcpy(op,"<=");
							return PType::RANGE;
						}else if(!strcmp(op,"<")){
							strcpy(op,">");
							return PType::RANGE;
						}else if(!strcmp(op,"<=")){
							strcpy(op,">=");
							return PType::RANGE;
						}else if(!strcmp(op,"!=") or !strcmp(op,"<>")){
							return PType::NOT_EQUAL;
						}else if(!strcmp(op,"!~")){
							std::cerr<<"impossible left & right for !~ operator ! "<<std::endl;
							exit(-1);
						}
					}break;
					case T_SubLink:{
						if(!strcmp(op,"=")){
						}else if(!strcmp(op,">")){
							strcpy(op,"<");
						}else if(!strcmp(op,">=")){
							strcpy(op,"<=");
						}else if(!strcmp(op,"<")){
							strcpy(op,">");
						}else if(!strcmp(op,"<=")){
							strcpy(op,">=");
						}else if(!strcmp(op,"!=") or !strcmp(op,"<>")){
						}else if(!strcmp(op,"!~")){
							std::cerr<<"impossible left & right for !~ operator ! "<<std::endl;
							exit(-1);
						}
						return PType::SUBLINK;
					}break;
					case T_SubPlan:{
						if(!strcmp(op,"=")){
						}else if(!strcmp(op,">")){
							strcpy(op,"<");
						}else if(!strcmp(op,">=")){
							strcpy(op,"<=");
						}else if(!strcmp(op,"<")){
							strcpy(op,">");
						}else if(!strcmp(op,"<=")){
							strcpy(op,">=");
						}else if(!strcmp(op,"!=") or !strcmp(op,"<>")){
						}else if(!strcmp(op,"!~")){
							std::cerr<<"impossible left & right for !~ operator ! "<<std::endl;
							exit(-1);
						}
						return PType::SUBQUERY;
					}break;
					default:{
						std::cerr<<"unsupport right value type :"<<right_type<<" of quals"<<std::endl;
					}
				}
			}else{
				std::cerr<<"not support currnt type of quals: left: "<<left_type<<",right: "<<right_type<<std::endl;
				exit(-1);
			}
		}
	}else if(qual->hash_sub_plan){
		return PType::SUBQUERY;
	}
}

/**
 * PredVariable: checek if the nodetag indicates a variable value
 */
bool PredEquivlence::PredVariable(NodeTag node_tag){
	switch(node_tag){
		case T_Const:
		case T_SubLink:
		case T_SubPlan:{
			return false;
		}break;
		default:{
			return true;
		}
	}
}

/**
 * PredEquivlence::Insert: used while pred_equivlences merging
 */
bool PredEquivlence::Insert(PredEquivlence* pe, bool check_can_merged){
	assert(pe);
	if(!check_can_merged){
		/*we should check if the pe can insert into current pe*/
		if(!Serach(pe)){
			return false;
		}
	}
	/*update pred_name set*/
	auto pred_set = pe->GetPredSet();
		for(const auto& pred_name: pred_set){
			set_.insert(pred_name);
	}
	/*update ranges*/
	for(const auto& r : pe->GetRanges()){
		std::vector<PredEquivlenceRange*> merge_range_list;
		if(RangesSerach(r,merge_range_list)){
			/*merge ranges */
			merge_range_list.push_back(r);
			if(!MergePredEquivlenceRanges(merge_range_list)){
				return false;
			}
		}else{
			/*add a new range*/
			PredEquivlenceRange* range = new PredEquivlenceRange();
			r->Copy(range);
			ranges_.insert(r);
		}
	}
	return true;
}

/**
 * PredEquivlence::RangesSerach: serach if the range has intersection with this->ranges_:
 * ret:
 * 	- true:
 *  - false:
 */
bool PredEquivlence::RangesSerach(PredEquivlenceRange* range,std::vector<PredEquivlenceRange*>& merge_range_list){
	assert(range);
	bool found = false;
	for(const auto& r : ranges_){
		if(r->Serach(range)){
			merge_range_list.push_back(r);
			found = true;
		}
	}
	return found;	
}

/**
 * PredEquivlence::MergePredEquivlenceRanges: 
 * TODO: 24-12-27 current we only can merge ranges which types are all PType::Range!
 */
bool PredEquivlence::MergePredEquivlenceRanges(const std::vector<PredEquivlenceRange*>& merge_range_list) {
	PredEquivlenceRange* new_range = new PredEquivlenceRange();
	int idx = 0;
	
	std::string upper_bound;
	std::string lower_bound;
	bool left = false;
	bool right = false;

	for(const auto& r : merge_range_list){
		if(r->PredType() != PType::RANGE){
			std::cerr<<"only support range type merge currently"<<std::endl;
			exit(-1);
		}

		if(idx == 0){
			upper_bound = merge_range_list[0]->UpperLimit();
			lower_bound = merge_range_list[0]->LowerLimit();
			left = merge_range_list[0]->GetLowerBoundaryConstraint();
			right = merge_range_list[0]->GetUpperBoundaryConstraint();
		}else{

			if((r->UpperLimit() < upper_bound && r->UpperLimit() != UPPER_LIMIT && upper_bound != UPPER_LIMIT)||
			   (upper_bound == UPPER_LIMIT)||
			   (r->UpperLimit() == upper_bound && r->GetUpperBoundaryConstraint() && !right)
			){
				upper_bound = r->UpperLimit();
				right = r->GetUpperBoundaryConstraint();
			}
			
			if((r->LowerLimit() > lower_bound && r->LowerLimit() != LOWER_LIMIT &&  lower_bound != LOWER_LIMIT)||
			   (lower_bound  == LOWER_LIMIT)||
			   (r->LowerLimit() == lower_bound && r->GetLowerBoundaryConstraint() && !left)
			){
				lower_bound = r->LowerLimit();
				left = r->GetLowerBoundaryConstraint();
			}
		}
		ranges_.erase(r);
		++idx;
	}
	new_range->SetPredType(PType::RANGE);
	new_range->SetLowerLimit(lower_bound);
	new_range->SetUpperLimit(upper_bound);
	ranges_.insert(new_range);
	return true;
}

bool PredEquivlence::Serach(Quals* quals){
	assert(quals);
	bool left = set_.find(quals->left) != set_.end();
	if(IsOnlyLeft(quals)){
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

bool PredEquivlence::Serach(const std::string& attr){
 	return  set_.find(attr) != set_.end();
}

/**
 * PredEquivlence::Compare: use while check if is the slow subqueries
 * TODO: not implement
 */
bool PredEquivlence::Compare(PredEquivlence* pe){
	return true;
}

bool PredEquivlence::Copy(PredEquivlence* pe){
	pe->SetPredSet(set_);
	pe->SetRanges(ranges_);
	pe->SetSubLinkLevelPeLists(sublink_level_pe_lists_);
	return true;
}


void PredEquivlence::ShowPredEquivlence(int depth){
	std::cout<<"(name_sets: [";
	int idx = 0;
	for(auto iter = set_.begin();iter != set_.end();iter++){
		if (idx) 
			std::cout<<",";
		std::cout<<*iter;
		idx++;
	}
	
	std::cout<<"] , ";
	idx = 0;

	std::cout<<"range_sets:[";
	for(const auto& range : ranges_){
		if(idx)
			std::cout<<",";
		range->PrintPredEquivlenceRange(depth);
		idx++;
	}

	if(sublink_level_pe_lists_.empty()){
		std::cout<<"])";
	}else{
		std::cout<<"] , sublinks: ["<<std::endl;
		PrintIndent(depth+1);
		for(const auto& subklink : sublink_level_pe_lists_){
			std::cout<<""<<subklink.first<<std::endl;
			PrintIndent(depth+1);
			subklink.second->ShowTotalPredClass(depth+3);
		}
		PrintIndent(depth);
		std::cout<<"])";
	}
}

void PredEquivlence::ShowPredEquivlenceSets(int depth){
	std::cout<<"[";
	int idx = 0;
	for(auto iter = set_.begin();iter != set_.end();iter++){
		if (idx) 
			std::cout<<",";
		std::cout<<*iter;
		idx++;
	}
	std::cout<<"]";
}

/**
 * LevelPredEquivlences::Insert
 */
bool LevelPredEquivlences::Insert(Quals* quals,bool is_or){
	assert(quals);
	std::vector<PredEquivlence*>merge_pe_list;
	if(Serach(quals,merge_pe_list)){
		if(!MergePredEquivlences(merge_pe_list)){
			return false;
		}
	}else{
		PredEquivlence* pe = new PredEquivlence(quals);
		level_pe_sets_.insert(pe);
	}
	return true;
}

bool LevelPredEquivlences::Insert(PredEquivlence* pe){
	assert(pe);
	std::vector<PredEquivlence*>merge_pe_list;
	if(Serach(pe,merge_pe_list)){
		if(!MergePredEquivlences(merge_pe_list)){
				return false;
		}
	}else{
		PredEquivlence* new_pe = new PredEquivlence();
		pe->Copy(new_pe);
		level_pe_sets_.insert(new_pe);
	}
	return true;
}

/**
 * LevelPredEquivlences::Insert
 */
bool LevelPredEquivlences::Insert(LevelPredEquivlences* lpes){
	assert(lpes);
	for(const auto& pe : *lpes){
		std::vector<PredEquivlence*>merge_pe_list;
		if(Serach(pe,merge_pe_list)){
			merge_pe_list.push_back(pe);
			if(!MergePredEquivlences(merge_pe_list)){
				return false;
			}
		}else{
			PredEquivlence* new_pe =  new PredEquivlence();
			pe->Copy(new_pe);
			level_pe_sets_.insert(new_pe);
		}
	}
	return true;
}

/**
 * LevelPredEquivlences::MergePredEquivlences,here 
 */
bool LevelPredEquivlences::MergePredEquivlences(const std::vector<PredEquivlence*>& merge_pe_list){
	PredEquivlence* new_pe = new PredEquivlence();
	std::set<std::string>pred_name_set;
	int idx = 0;
	for(const auto& mpe : merge_pe_list){
		if(idx == 0){
			mpe[idx].Copy(new_pe);
		}else{
			new_pe->Insert(mpe,true);
		}
		/*remove the pe has been merged*/
		level_pe_sets_.erase(mpe);
		++idx;
	}
	/*insert the new pe*/
	level_pe_sets_.insert(new_pe);
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

bool LevelPredEquivlences::Serach(Quals* quals,std::vector<PredEquivlence*>& pe_idx_list){
	assert(quals);
	bool ret = false;
	for(const auto& item : level_pe_sets_){
		if(item->Serach(quals)){
			pe_idx_list.push_back(item);
			ret = true;
		}
	}
	return ret;
}

bool LevelPredEquivlences::Serach(const std::string& attr,PredEquivlence*& pe){
	for(const auto& item : level_pe_sets_){
		if(item->Serach(attr)){
			pe = item;
			return true;
		}
	}
	return false;
}

/**
 * LevelPredEquivlences::Copy
 */
bool LevelPredEquivlences::Copy(LevelPredEquivlences* lpes){
	assert(lpes);
	for(const auto& pe : level_pe_sets_){
		PredEquivlence* new_pred = new PredEquivlence();
		pe->Copy(new_pred);
		lpes->Insert(new_pred);
	}
	return true;
}

void LevelPredEquivlences::ShowLevelPredEquivlences(int depth){
	PrintIndent(depth);
	std::cout<<"{";
	int idx  = 0; 
	for(const auto& pe : level_pe_sets_){
		if(idx)
			std::cout<<",";
		pe->ShowPredEquivlence(depth+1);
		idx++;
	}
	std::cout<<"}\n";
}

/**
 * LevelPredEquivlencesList::Insert 
 */
bool LevelPredEquivlencesList::Insert(LevelPredEquivlences* pe,bool is_or){
	assert(pe);
	if(is_or || lpes_list_.empty()){
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
	if(is_or){ /*or_model*/
		for(const auto& lpes: *lpes_list){
			Insert(lpes,false);
		}
	}else{ /*and_model*/
		if(lpes_list_.empty()){
			/*current level don't has any predicates,we just copy the pre level*/
			for(const auto& src : *lpes_list){
				LevelPredEquivlences* new_lpes = new LevelPredEquivlences();
				src->Copy(new_lpes);
				lpes_list_.push_back(new_lpes);
			}
		}else{
			int sz = lpes_list->Size()-1;
			while(sz > 0){
				for(const auto& dst : lpes_list_){
					LevelPredEquivlences* new_lpes = new LevelPredEquivlences();
					dst->Copy(new_lpes);
					lpes_list_.push_back(new_lpes);
				}
				--sz;
			}

			for(auto& src : *lpes_list){
				for(auto& dst : lpes_list_){
					dst->Insert(src);
				}
			}

			/**
			 * TODO: merge, best metod isn't merge lpes here, instead, we wish merge before each Insert 
			 */
		}
	
	}
	return true;
}


void LevelPredEquivlencesList::Copy(LevelPredEquivlencesList* new_lpes_list){
	for(const auto& lpes : lpes_list_){
		LevelPredEquivlences* new_lpes =  new LevelPredEquivlences();
		lpes->Copy(new_lpes);
		new_lpes_list->Insert(new_lpes,true);
	}
}


void LevelPredEquivlencesList::ShowLevelPredEquivlencesList(int depth){
	for(const auto& lpe: lpes_list_){
		lpe->ShowLevelPredEquivlences(depth+1);
	}	
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
					tmp_levels.push_back(child);
				}
			}else if(levels[i]->expr_case == PRED_EXPRESSION__EXPR_QUAL){
				/*no chils,nothing to do*/
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

void LevelManager::ReSetAllPreProcessd(){
	for (const auto& label : p_labels) {
		pre_processed_map_[label] = false;
	}
}

void LevelManager::ShowPredClass(int height,int depth){
	assert(height>=0);
	
	PrintIndent(depth);
	std::cout << "level<"+ std::to_string(height) <<">pred_equivelnces: "<<std::endl;
	total_equivlences_[height]->ShowLevelPredEquivlencesList(depth+1);
	
	PrintIndent(depth);
	std::cout<<"output cols: "<<std::endl;
	total_outputs_[height]->ShowLevelOutputList(depth+1);

	PrintIndent(depth);
	std::cout<<"tables: ";
	total_tbls_[height]->ShowLevelTblList(depth+1);

	PrintIndent(depth);
	std::cout<<"groupby keys: "<<std::endl;
	total_aggs_[height]->ShowLevelAggAndSortList(depth+1);

	PrintIndent(depth);
	std::cout<<"sort keys: "<<std::endl;
	total_sorts_[height]->ShowLevelAggAndSortList(depth+1);
	
	PrintIndent(depth);
	PrintLine(50-depth);
}

void LevelManager::ShowTotalPredClass(int depth){
	std::cout<<"Total Pred Class: "<<std::endl;
	std::reverse(total_equivlences_.begin(),total_equivlences_.end());
	std::reverse(total_outputs_.begin(),total_outputs_.end());
	std::reverse(total_aggs_.begin(),total_aggs_.end());
	std::reverse(total_sorts_.begin(),total_sorts_.end()); 
	std::reverse(total_tbls_.begin(),total_tbls_.end());

	for(size_t i = 0; i< total_equivlences_.size();i++){
		ShowPredClass(i,depth+1);
	}
}
