#include "collect/level_mgr.hpp"
#include <algorithm>
#include <cfloat>
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
 * LevelManager::GetSubQueries
 */
void LevelManager::GetSubQueries(){
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

			/*collect plan_join type*/
			if(NodeTag(levels[i]->node_tag) == T_NestLoop 
				|| NodeTag(levels[i]->node_tag) == T_MergeJoin 
				|| NodeTag(levels[i]->node_tag) == T_HashJoin){
				assert(strlen(levels[i]->join_type));
				join_type_list_.push_back(levels[i]->join_type);
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

	/*calualate */
	auto lpes_list = total_equivlences_[cur_height_];
	auto early_stop_map = lpes_list->EarlyStopMap();

	auto child_map = lpes_list->GetChildLpesMap();

	for(const auto item : child_map){
		auto cur_lpes = lpes_list->GetLpes(item.first);
		auto child_lpes_list = total_equivlences_[cur_height_-1];
		for(const auto& child_id : item.second){
			assert(cur_height_>=1);
			if(cur_lpes->EarlyStop()){
				auto grandchilds = child_lpes_list->EarlyStopMap()[child_id];
				early_stop_map[item.first] = grandchilds;
			}else{
				for(const auto& id : item.second){
					early_stop_map[item.first].push_back({cur_height_-1,id});
				}
			}
		}
	}

	size_t idx = 0;
	for(const auto& e : *lpes_list){
		e->SetLpeId(idx++);
		e->BuildKey2PeMap();

		for(const auto& pe : *e){
			pe->CalSortInfo();
		}
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

std::string PrintIndent(int depth, std::string tag){
	std::string str;
	for(int i = 0; i<depth; ++i) {
		str += "  ";
	}
	return str;
}

void PrintLine(int len){
	for(int i=0;i<len;++i){
		std::cout << "-";
	}
	std::cout<<std::endl;
}

std::string PrintLine(int len,std::string tag){
	std::string str;
	for(int i=0;i<len;++i){
		str += "-";
	}
	return str;
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

	// if(hsps->n_output == 0){
	// 	std::cerr<<"output is empty"<<std::endl;
	// 	exit(-1);
	// }
	
	LevelOutputList* final_lo_list = new LevelOutputList();
	LevelOutputList* node_final_lo_list = new LevelOutputList();

	bool same_level_need_merged = true;
	if(cur_height_ == (int)total_outputs_.size()){
		total_outputs_.push_back(nullptr);	
		same_level_need_merged = false;
	}

	auto lpes_list =  total_equivlences_[cur_height_];
	final_lo_list->Insert(lpes_list,hsps);

	auto& node_lpes_list = nodes_collector_map_[cur_hsps_]->node_equivlences_;
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
		lpe_id_list_.resize(lpes_list->Size());
	}else{
		output2pe_list_.resize(1);
		output_extend_list_.resize(1);
		lpe_id_list_.resize(1);
	}

	/* lpes_list == nullptr , just for node_collector */
	if(!lpes_list->Size()){
		/*set id*/
		lpe_id_list_[0] = -1;
		for(size_t i=0;i<hsps->n_output;i++){
			std::string attr(hsps->output[i]);
			output2pe_list_[0].insert(std::make_pair(attr,nullptr));
			output_extend_list_[0].insert(attr);
		}
		return;
	}
	
	int lpes_idx = 0;
	for(const auto& e: *lpes_list){
		/*set id*/
		lpe_id_list_[lpes_idx] = e->LpeId();
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
		const auto& src_map = lo_list->output2pe_list_[i];
		for(const auto& src_attr: src_map){
			if(dst_map.find(src_attr.first) != dst_map.end()){
				/*here we need check if it is a equivlence,if not,it's a error*/
			}else{
				dst_map.insert(src_attr);
			}
		}

		auto& dst_set = output_extend_list_[i];
		const auto& src_set = lo_list->output_extend_list_[i];
		for(const auto& src_attr : src_set){
			dst_set.insert(src_attr);
		}
	}
}

void LevelOutputList::ShowLevelOutputList(int depth){
	for(size_t i = 0; i < output2pe_list_.size(); i++){
		PrintIndent(depth);
		std::cout<<"[id:"<<lpe_id_list_[i]<<"]->(";
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
		std::cout<<")\n";
	}
}

std::string  LevelOutputList::ShowLevelOutputList(int depth,std::string tag,SqmsLogger* logger){
	std::string str;
	for(size_t i = 0; i < output2pe_list_.size(); i++){
		str += PrintIndent(depth,tag);
		str += "[id:" + std::to_string(lpe_id_list_[i]) + "]->(";
		int j = 0;	
		for(const auto& e : output2pe_list_[i]){
			if(j) str +=",";
			if(e.second == nullptr){
				str += e.first;
			}else{
				str += e.first+"->";
				e.second->ShowPredEquivlenceSets(depth,tag,logger);
			}
			j++;
		}
		str += "), extend_list: (";
		j = 0;
		for(const auto& e : output_extend_list_[i]){
			if(j) str += ",";
			str += e;
			j++;
		}
		str += ")\n";
	}
	return str;
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
		if(cur_height_ >= 1 && !GetPreProcessed(PreProcessLabel::SORT)){
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
		if(cur_height_ >= 1 && !GetPreProcessed(PreProcessLabel::SORT)){
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

std::string AggAndSortEquivlence::ShowAggEquivlence(int depth,std::string tag,SqmsLogger* logger){
	std::string str;
	str += tag_ +":(";
	int j = 0;
	for(const auto& e : key2pe_){
		if(j) str += ",";
		if(e.second == nullptr){
			str += e.first;
		}else{
			str += e.first + "->";
			str += e.second->ShowPredEquivlenceSets(depth,tag,logger);
		}
		j++;
	}
	str += "), extends: (";
	j = 0;
	for(const auto& e : extends_){
		if(j) str += ",";
		str += e;
		j++;
	}
	str += ")";
	return str;
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
	std::cout<<"[id:"<<lpe_id_<<"]->{";
	int idx = 0;
	for(const auto& ae : level_agg_sets_){
		if(idx)std::cout<<",";
		ae->ShowAggEquivlence(depth+1);
		idx++;
	}
	std::cout<<"}\n";
}

std::string LevelAggAndSortEquivlences::ShowLevelAggEquivlence(int depth,std::string tag, SqmsLogger* logger){
	std::string str = PrintIndent(depth,tag);
	str += "[id:" + std::to_string(lpe_id_) + "]->{";
	int idx = 0;
	for(const auto& ae : level_agg_sets_){
		if(idx)str += ",";
		str += ae->ShowAggEquivlence(depth+1,tag,logger);
		idx++;
	}
	str += "}\n";
	return str;
}

void LevelAggAndSortList::Insert(HistorySlowPlanStat* hsps,std::string label){
	assert(lpes_list_);

	if(hsps->n_group_sort_keys == 0){
		return;
	}

	if(!lpes_list_->Size()){
		AggAndSortEquivlence* new_agg = new AggAndSortEquivlence(label);
		new_agg->Init(hsps,nullptr);
		LevelAggAndSortEquivlences* new_ae_list = new LevelAggAndSortEquivlences();
		new_ae_list->Insert(new_agg);
		
		new_ae_list->SetLpeId(-1);

		level_agg_list_.push_back(new_ae_list);
	}else{
		/*here we should keep lpes order equal to ae_list*/
		for(const auto& lpes: *lpes_list_){			
			AggAndSortEquivlence* new_agg = new AggAndSortEquivlence(label);
			new_agg->Init(hsps,lpes);
			LevelAggAndSortEquivlences* new_ae_list = new LevelAggAndSortEquivlences();
			
			/*set lpeid for every LevelAggAndSortEquivlences*/
			new_ae_list->SetLpeId(lpes->LpeId());

			new_ae_list->Insert(new_agg);
			level_agg_list_.push_back(new_ae_list);
		}
	}
}

void LevelAggAndSortList::Insert(LevelAggAndSortList* la_list){
	assert(la_list);
	if(!la_list->Size()){
		return;
	}
	if(level_agg_list_.empty()){
		LevelAggAndSortEquivlences *new_dst_lae = new LevelAggAndSortEquivlences();
		for(const auto& src_lae : la_list->GetLevelAggList()){
			new_dst_lae->Insert(src_lae);
			level_agg_list_.push_back(new_dst_lae);
		}
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

std::string LevelAggAndSortList::ShowLevelAggAndSortList(int depth,std::string tag, SqmsLogger* logger){
	std::string str;
	for(const auto& lae: level_agg_list_){
		str += lae->ShowLevelAggEquivlence(depth+1,tag,logger);
	}
	return str;
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

std::string LevelTblList::ShowLevelTblList(int depth,std::string tag,SqmsLogger* logger){
	std::string str;
	str += "{";
	int idx = 0;
	for(const auto& name: tbl_set_){
		if(idx++)str += ",";
		str += name;
	}
	str += "}\n";
	return str;	
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
	LevelPredEquivlencesList* final_lpes_list;

	if(!root){
		if(first_pred_check_){
			node_final_lpes_list = new LevelPredEquivlencesList();
			if(cur_height_ >= 1){
				for(size_t i = 0;i<cur_hsps_->n_childs; ++i){
					auto child_eq = nodes_collector_map_[cur_hsps_->childs[i]]->node_equivlences_;
					node_final_lpes_list->Insert(child_eq,false);
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
			final_lpes_list->Insert(total_equivlences_[cur_height_-1],false,true);
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
		final_lpes_list->Insert(lpes,false);
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
								auto type = cur_op->Child(i)->Type();
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
										auto child_op = static_cast<PredOperatorWrap*>(cur_op->Child(i));
										switch(child_op->GetOpType()){
											case PRED_OPERATOR__PRED_OPERATOR_TYPE__AND:{
												auto child_and_lpes_list = child_op->GetAndLpesList();
												assert(child_and_lpes_list->Size());
												and_lpes_list->Insert(child_and_lpes_list,false);
											}break;
											case PRED_OPERATOR__PRED_OPERATOR_TYPE__OR:{
												auto child_or_lpes_list = child_op->GetOrLpesList();
												assert(child_or_lpes_list->Size());
												and_lpes_list->Insert(child_or_lpes_list,false);
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
							LevelPredEquivlencesList* or_lpes_list = new LevelPredEquivlencesList();
							std::vector<AbstractPredNode*>or_op_list;
							for(size_t i=0;i<cur_op->ChildSize();i++){
								auto type = cur_op->Child(i)->Type();
								switch (type){
									case AbstractPredNodeType::QUALS:{
										auto qual = static_cast<QualsWarp*>(cur_op->Child(i))->GetQual();
										qual->hsps = cur_hsps_;
										LevelPredEquivlences * lpes = new LevelPredEquivlences();
										lpes->Insert(qual,false);
										or_lpes_list->Insert(lpes,true);
									}break;
									case AbstractPredNodeType::OPERATOR:{
										auto child_op = static_cast<PredOperatorWrap*>(cur_op->Child(i));
										switch (child_op->GetOpType()){
											case PRED_OPERATOR__PRED_OPERATOR_TYPE__AND:{
												auto child_and_lpes_list = child_op->GetAndLpesList();
												assert(child_and_lpes_list->Size());
												or_lpes_list->Insert(child_and_lpes_list,true);
											}break;
											case PRED_OPERATOR__PRED_OPERATOR_TYPE__OR:{
												auto child_or_lpes_list = child_op->GetOrLpesList();
												assert(child_or_lpes_list->Size());
												or_lpes_list->Insert(child_or_lpes_list,true);
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

	if(!total_equivlences_[cur_height_])
		total_equivlences_[cur_height_] = new LevelPredEquivlencesList();
	
	/*we should merge pre level's lpes with this level*/
	if(cur_height_ >= 1){	
		if(!GetPreProcessed(PreProcessLabel::PREDICATE)){
			total_equivlences_[cur_height_]->Insert(total_equivlences_[cur_height_-1],false,true,true);
			SetPreProcessed(PreProcessLabel::PREDICATE,true);
		}
	}
	total_equivlences_[cur_height_]->Insert(final_lpes_list,false,false,true);

	if(nodes_collector_map_[cur_hsps_]->node_equivlences_ && nodes_collector_map_[cur_hsps_]->node_equivlences_->Size())
		node_final_lpes_list->Insert(nodes_collector_map_[cur_hsps_]->node_equivlences_,false);
	if(cur_height_ >= 1){
		if(first_pred_check_){
			for(size_t i = 0;i<cur_hsps_->n_childs; ++i){
				auto child_eq = nodes_collector_map_[cur_hsps_->childs[i]]->node_equivlences_;
				node_final_lpes_list->Insert(child_eq,false);
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

				pred_subquery_map_[lpes_list->LpeId()].push_back(sublink.second.get());

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

	if(sub_final_lpes_list->Size()){
		//final_lpes_list->Insert(sub_final_lpes_list,false);
		total_equivlences_[cur_height_]->Insert(sub_final_lpes_list,false);
	}
		
	/*update toal_equivlences*/
	//total_equivlences_[cur_height_] = final_lpes_list;
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
				return PredEquivlenceRange::LimitCompare(range->UpperLimit(),range->PredVarType(),UpperLimit(),var_type_) == 0;
				//return range->UpperLimit() == UpperLimit();	
			}else{
				return false; 
			}
		}break;
		case PType::EQUAL:{
			if(range->PredType() == PType::NOT_EQUAL){
				return PredEquivlenceRange::LimitCompare(range->UpperLimit(),range->PredVarType(),UpperLimit(),var_type_) == 0;
				//return range->UpperLimit() == UpperLimit();	
			}else{
				return false;
			}
		}break;
		case PType::RANGE:{
			if(range->PredType() == PType::RANGE || range->PredType() == PType::EQUAL){
				bool left_no_intersected = false;
				if(range->LowerLimit() != LOWER_LIMIT){
					if(upper_limit_ != UPPER_LIMIT){
						// left_no_intersected = (std::stoi(range->LowerLimit()) > std::stoi(upper_limit_)) 
						// 	|| (range->LowerLimit() == upper_limit_ && (!range->GetLowerBoundaryConstraint()&GetUpperBoundaryConstraint()));
						auto ret = PredEquivlenceRange::LimitCompare(range->LowerLimit(),range->PredVarType(),upper_limit_,var_type_);
						left_no_intersected = (ret > 0 || (!ret && (!range->GetLowerBoundaryConstraint()&GetUpperBoundaryConstraint())));
					}else{
						left_no_intersected = false;
					}
				}else{
					left_no_intersected = false;
				}
				
				bool right_no_intersected = false;
				if(range->UpperLimit() !=  UPPER_LIMIT){
					if(lower_limit_ != LOWER_LIMIT){
						// right_no_intersected = (std::stoi(range->UpperLimit()) < std::stoi(lower_limit_))
						// 	|| (range->UpperLimit() == lower_limit_ && (!range->GetUpperBoundaryConstraint()&GetLowerBoundaryConstraint()));
						auto ret = PredEquivlenceRange::LimitCompare(range->UpperLimit(),range->PredVarType(),lower_limit_,var_type_);
						right_no_intersected = (ret < 0 || (!ret && (!range->GetUpperBoundaryConstraint()&GetLowerBoundaryConstraint())));						
					}else{
						right_no_intersected = false;
					}
				}else{
					right_no_intersected = false;
				}
				return !(right_no_intersected || left_no_intersected);
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

int PredEquivlenceRange::LimitCompare(const std::string& left_range,VarType left_type,const std::string& right_range,VarType right_type){
	assert(left_range.size() && right_range.size());
	/*pasrer string to int*/
	auto int_parser = [](std::string range)->int{
		int var = 0;
		if(range == UPPER_LIMIT){
			var = INT_MAX;
		}else if(range == LOWER_LIMIT){
			var = INT_MIN;
		}else{
			var = std::stoi(range);
		}
		return var;
	};
	
	/*parser string to double*/
	auto double_parser = [](std::string range)->double{
		double var = 0;
		if(range == UPPER_LIMIT){
			var = DBL_MAX;
		}else if(range == LOWER_LIMIT){
			var = DBL_MIN;
		}else{
			var = std::stod(range);
		}
		return var;
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
				exit(-1);
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
				exit(-1);
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
				exit(-1);
			}
		}break;
		case VarType::BOOL:{
			int left_var = int_parser(left_range);
			if(right_type == VarType::BOOL){
				int right_var = int_parser(right_range);
				return left_var == right_var;
			}else{
				std::cerr<<"right type not match left type <double>"<<std::endl;
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

void PredEquivlenceRange::Copy(PredEquivlenceRange* new_range){
	new_range->SetPredType(type_);
	new_range->SetLowerLimit(lower_limit_);
	new_range->SetUpperLimit(upper_limit_);
	new_range->SetBoundaryConstraint(boundary_constraint_);
	new_range->SetSubqueryName(subquery_name_);
	new_range->SetList(list_);
	new_range->SetPredVarType(new_range->PredVarType());
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

std::string PredEquivlenceRange::PrintPredEquivlenceRange(int depth,std::string tag,SqmsLogger* logger){
	std::string output;
	auto ll = lower_limit_ == LOWER_LIMIT?"-∞":lower_limit_;
	auto ul = upper_limit_ == UPPER_LIMIT?"+∞":upper_limit_;

	std::string var_type;
	switch(var_type_){
		case VarType::INT:{
			var_type = "int";
		}break;
		case VarType::STRING:{
			var_type = "string";
		}break;
		case VarType::DOUBLE:{
			var_type = "double";
		}break;
		case VarType::BOOL:{
			var_type = "bool";
		}break;
		case VarType::UNKNOWN:{
			var_type = "unknown";
		}break;
		default:{
			std::cerr<<"error var type!"<<std::endl;
			exit(-1);
		}
	}
	output += "<type:"+var_type+">";
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
			output+=subquery_name_;
		}break;
		default:{
			std::cerr<<"unknow type of pe_range: "<<int(type_)<<std::endl;
			exit(-1);
		}
	}
	return output;
}

PredEquivlence::PredEquivlence(Quals* qual){
	assert(qual);
	PType type = QualType(qual);
	auto var_type = QualVarType(qual);
	PredEquivlenceRange* range = new PredEquivlenceRange(); 
	switch(type){
		case PType::JOIN_EQUAL:{
			range->SetPredType(PType::RANGE);
			range->SetUpperLimit(UPPER_LIMIT);
			range->SetLowerLimit(LOWER_LIMIT);
			range->SetBoundaryConstraint(std::make_pair(true,true));
			range->SetPredVarType(var_type);
			ranges_.insert(range);
			set_.insert(qual->left);
			set_.insert(qual->right);
		}break;
		case PType::EQUAL:{
			set_.insert(qual->left);
			range->SetPredType(PType::RANGE);
			range->SetPredVarType(var_type);
			range->SetLowerLimit(qual->right);
			range->SetUpperLimit(qual->right);
			range->SetBoundaryConstraint(std::make_pair(true,true));
			ranges_.insert(range);
		}break;
		case PType::NOT_EQUAL:{
			set_.insert(qual->left);
			auto op = qual->op;
			if(!strcmp(op,"!=") or !strcmp(op,"<>")){
				range->SetPredType(PType::NOT_EQUAL);
				range->SetPredVarType(var_type);
				range->SetLowerLimit(qual->right);
				range->SetUpperLimit(qual->right);
			}else if(!strcmp(op,"!~")){
				range->SetPredType(PType::NOT_EQUAL);
				range->SetPredVarType(var_type);
				/**
				 * TODO: not implement: regular expression check
				*/
			}
			ranges_.insert(range);
		}break;
		case PType::RANGE:{
			set_.insert(qual->left);
			range->SetPredVarType(var_type);
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
				range->SetBoundaryConstraint(std::make_pair(true,true));
			}else{
				std::cerr<<"unkonw op type of range qual while init pred equivlence"<<std::endl;
				exit(-1);
			}
			ranges_.insert(range);
		}break;
		case PType::LIST:{
			range->SetPredVarType(var_type);
			set_.insert(qual->left);
			/**
			 * TODO: not implement yet: we should convert str_list into true list
			 */
			ranges_.insert(range);
		}break;
		case PType::SUBQUERY:{
			assert(qual->hsps);
			PlanFormatContext* pf_context = new PlanFormatContext();
			
			bool found = false;
			auto logger = (SqmsLogger*)ShmemInitStruct("SqmsLogger", sizeof(SqmsLogger), &found);
			assert(found);

			if(!strlen(qual->left)){
				/*subquery not must have left val*/
			}else{
				std::string l = extract_field(qual->left);
				if(l.empty()){
					set_.insert(qual->left);
				}else{
					set_.insert(l);
				}
			}

			/*calualate pred equivlence here*/
			for(size_t i = 0; i < qual->hsps->n_subplans; i++){
				auto sub_plan_hsp = qual->hsps->subplans[i];
	
				PredEquivlenceRange* new_range = new PredEquivlenceRange();
				range->SetPredVarType(var_type);
				new_range->SetPredType(PType::SUBQUERY);
				new_range->SetSubqueryName(sub_plan_hsp->sub_plan_name);
				ranges_.insert(new_range);
					
				auto sub_level_mgr = std::make_shared<LevelManager>(sub_plan_hsp,nullptr,logger);
				pf_context->SetStrategy(sub_level_mgr);
				pf_context->executeStrategy();
				sublink_level_pe_lists_[sub_plan_hsp->sub_plan_name] = sub_level_mgr; 
			}
		}break;
		case PType::SUBLINK:{
			assert(qual->hsps);
			PlanFormatContext* pf_context = new PlanFormatContext();

			bool found = false;
			auto logger = (SqmsLogger*)ShmemInitStruct("SqmsLogger", sizeof(SqmsLogger), &found);
			assert(found);

			if(!strlen(qual->left)){				
			}else{
				std::string l = extract_field(qual->left);
				if(l.empty()){
					set_.insert(qual->left);
				}else{
					set_.insert(l);
				}				
			}
			
			/*calualate pred equivlence here*/
			for(size_t i = 0; i < qual->hsps->n_subplans; i++){
				auto sub_plan_hsp = qual->hsps->subplans[i];
				PredEquivlenceRange* new_range = new PredEquivlenceRange(); 
				new_range->SetPredVarType(var_type);
				new_range->SetPredType(PType::SUBLINK);
				new_range->SetSubqueryName(sub_plan_hsp->sub_plan_name);
				ranges_.insert(new_range);

				auto sub_level_mgr = std::make_shared<LevelManager>(sub_plan_hsp,nullptr,logger);
				pf_context->SetStrategy(sub_level_mgr);
				pf_context->executeStrategy();
				sublink_level_pe_lists_[sub_plan_hsp->sub_plan_name] = sub_level_mgr; 
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
					return PType::UNKNOWN;
				}else if(!strcmp(op,">=")){
					/*not support*/
					return PType::UNKNOWN;
				}else if(!strcmp(op,"<")){
					/*not support*/
					return PType::UNKNOWN;
				}else if(!strcmp(op,"<=")){
					/*not support*/
					return PType::UNKNOWN;
				}else if(!strcmp(op,"!=") or !strcmp(op,"<>")){
					/*not support*/
					return PType::UNKNOWN;
				}else if(!strcmp(op,"!~")){
					/*not support*/
					return PType::UNKNOWN;
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
						return PType::UNKNOWN;
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
						return PType::UNKNOWN;
					}
				}
			}else{
				std::cerr<<"not support currnt type of quals: left: "<<left_type<<",right: "<<right_type<<std::endl;
				return PType::UNKNOWN;
			}
		}
	}else if(strlen(qual->sub_plan_name)){
		return PType::SUBQUERY;
	}else{
		auto left_type = NodeTag(qual->l_type);
		auto right_type = NodeTag(qual->r_type);
		std::cerr<<"not support currnt type of quals: left: "<<left_type<<",right: "<<right_type<<std::endl;
		return PType::UNKNOWN;
	}
}

VarType PredEquivlence::QualVarType(Quals* qual){
	assert(qual);
	auto get_var_type = [](uint32_t type){
		switch (type)
		{
			case BITOID:
				return VarType::UNKNOWN;
			case BOOLOID:
				return VarType::BOOL;
			case BPCHAROID:
				return VarType::STRING; 
			case FLOAT4OID:
			case FLOAT8OID:
				return VarType::DOUBLE;
			case INT2OID:
			case INT4OID:
			case INT8OID:
			case NUMERICOID:
				return VarType::INT;
			case INTERVALOID:
				return VarType::UNKNOWN;
			case TIMEOID:
				return VarType::UNKNOWN;
			case TIMETZOID:
				return VarType::UNKNOWN;
			case TIMESTAMPOID:
				return VarType::UNKNOWN;
			case TIMESTAMPTZOID:
				return VarType::UNKNOWN;
			case VARBITOID:
				return VarType::UNKNOWN;
			case VARCHAROID:
				return VarType::STRING;
			default:
				return VarType::UNKNOWN;
		}
	};

	if(qual->left_val_type_id){
		return get_var_type(qual->left_val_type_id);
	}else if(qual->left_val_type_id){
		return get_var_type(qual->right_val_type_id);
	}else{
		return VarType::UNKNOWN;
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
bool PredEquivlence::Insert(PredEquivlence* pe, bool check_can_merged, bool pre_merged,bool early_check){
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
			bool early_stop = MergePredEquivlenceRanges(merge_range_list,pre_merged,early_check);
			//if(!early_stop){
			//	child_ = std::shared_ptr<PredEquivlence>(pe);
			//}
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
 * ret: early_stop_
 */
bool PredEquivlence::MergePredEquivlenceRanges(const std::vector<PredEquivlenceRange*>& merge_range_list,bool pre_merge,bool early_check) {
	int idx = 0;
	
	std::string upper_bound;
	std::string lower_bound;
	VarType cur_type = VarType::UNKNOWN;
	bool left = false;
	bool right = false;

	assert(merge_range_list.size()>=2);
	if(merge_range_list.size() > 2){
		auto old_range = merge_range_list.back();
		for(auto iter = merge_range_list.begin();iter+1 != merge_range_list.end(); iter++){
			upper_bound = old_range->UpperLimit();
			lower_bound = old_range->LowerLimit();
			left = old_range->GetLowerBoundaryConstraint();
			right = old_range->GetUpperBoundaryConstraint();
			cur_type = old_range->PredVarType();

			auto r = *iter;
			if(r->PredType() != PType::RANGE){
				std::cerr<<"only support range type merge currently"<<std::endl;
				exit(-1);
			}
			auto ret = PredEquivlenceRange::LimitCompare(r->UpperLimit(),r->PredVarType(),upper_bound,cur_type);
			if((ret < 0 && r->UpperLimit() != UPPER_LIMIT && upper_bound != UPPER_LIMIT)
				|| (upper_bound == UPPER_LIMIT)
				|| (!ret && r->GetUpperBoundaryConstraint() && !right)
			){
				cur_type = r->PredVarType();
				upper_bound = r->UpperLimit();
				right = r->GetUpperBoundaryConstraint();
			}

			ret = PredEquivlenceRange::LimitCompare(r->LowerLimit(),r->PredVarType(),lower_bound,cur_type);
			if((ret > 0 && r->LowerLimit() != LOWER_LIMIT &&  lower_bound != LOWER_LIMIT)
				|| (lower_bound == UPPER_LIMIT)
				|| (!ret && r->GetLowerBoundaryConstraint() && !left)
			){
				cur_type = r->PredVarType();
				lower_bound = r->LowerLimit();
				left = r->GetLowerBoundaryConstraint();
			}

			ranges_.erase(r);
			PredEquivlenceRange* new_range = new PredEquivlenceRange();
			new_range->SetPredVarType(cur_type);
			new_range->SetPredType(PType::RANGE);
			new_range->SetLowerLimit(lower_bound);
			new_range->SetUpperLimit(upper_bound);
			new_range->SetLowerBoundaryConstraint(left);
			new_range->SetUpperBoundaryConstraint(right);
			ranges_.insert(new_range);
			
			
			auto before_range = *iter;
			if(pre_merge || early_check){
				bool range_decrease = false;
				if(before_range->LowerLimit() != LOWER_LIMIT){
					if(new_range->LowerLimit() != LOWER_LIMIT){
						ret = PredEquivlenceRange::LimitCompare(before_range->LowerLimit(),before_range->PredVarType(),new_range->LowerLimit(),new_range->PredVarType());
						range_decrease = (ret < 0) || (!ret && before_range->GetLowerBoundaryConstraint() && !new_range->GetLowerBoundaryConstraint());
						// range_decrease = (before_range->LowerLimit() < new_range->LowerLimit()) 
						// 			|| (before_range->LowerLimit() == new_range->LowerLimit() && before_range->GetLowerBoundaryConstraint() && !new_range->GetLowerBoundaryConstraint());
					}else{
						range_decrease = false;
					}
				}else{
					if(new_range->LowerLimit() != LOWER_LIMIT){
						range_decrease = true;
					}else{
						range_decrease = false;
					}
				}

				if(range_decrease){
					early_stop_ = false;
					continue;
				}

				if(before_range->UpperLimit() != UPPER_LIMIT){
					if(new_range->UpperLimit() != UPPER_LIMIT){
						ret = PredEquivlenceRange::LimitCompare(before_range->UpperLimit(),before_range->PredVarType(),new_range->UpperLimit(),new_range->PredVarType());
						range_decrease = (ret > 0) || (!ret && before_range->GetUpperBoundaryConstraint() && !new_range->GetUpperBoundaryConstraint());
						// range_decrease = (before_range->UpperLimit() > new_range->UpperLimit())
						// 			|| (before_range->UpperLimit() == new_range->UpperLimit() && before_range->GetUpperBoundaryConstraint() && !new_range->GetUpperBoundaryConstraint());
					}else{
						range_decrease = false;
					}
				}else{
					if(new_range->UpperLimit() != UPPER_LIMIT){
						range_decrease = true;
					}else{
						range_decrease = false;
					}
				}

				if(range_decrease){
					early_stop_ = false;
					continue;
				}
			}
		}
		return early_stop_;
	}

	for(const auto& r : merge_range_list){
		if(r->PredType() != PType::RANGE){
			std::cerr<<"only support range type merge currently"<<std::endl;
			exit(-1);
		}

		if(idx == 0){
			upper_bound = r->UpperLimit();
			lower_bound = r->LowerLimit();
			cur_type = r->PredVarType();
			left = r->GetLowerBoundaryConstraint();
			right = r->GetUpperBoundaryConstraint();
		}else{
			auto ret = PredEquivlenceRange::LimitCompare(r->UpperLimit(),r->PredVarType(),upper_bound,cur_type);
			if((ret < 0 && r->UpperLimit() != UPPER_LIMIT && upper_bound != UPPER_LIMIT)
				|| (upper_bound == UPPER_LIMIT)
				|| (!ret && r->GetUpperBoundaryConstraint() && !right)
			){
				cur_type = r->PredVarType();
				upper_bound = r->UpperLimit();
				right = r->GetUpperBoundaryConstraint();
			}

			ret = PredEquivlenceRange::LimitCompare(r->LowerLimit(),r->PredVarType(),lower_bound,cur_type);
			if((ret > 0 && r->LowerLimit() != LOWER_LIMIT &&  lower_bound != LOWER_LIMIT)
				|| (lower_bound == UPPER_LIMIT)
				|| (!ret && r->GetLowerBoundaryConstraint() && !left)
			){
				cur_type = r->PredVarType();
				lower_bound = r->LowerLimit();
				left = r->GetLowerBoundaryConstraint();
			}
		}
		ranges_.erase(r);
		++idx;
	}
	PredEquivlenceRange* new_range = new PredEquivlenceRange();
	new_range->SetPredVarType(cur_type);
	new_range->SetPredType(PType::RANGE);
	new_range->SetLowerLimit(lower_bound);
	new_range->SetUpperLimit(upper_bound);
	new_range->SetLowerBoundaryConstraint(left);
	new_range->SetUpperBoundaryConstraint(right);
	ranges_.insert(new_range);
	
	//std::cout<<"MERGE: "<<pre_merge<<" "<<early_check<<std::endl;
	/*if not merge range is not from pre level, not need check if early_stop*/
	if((!pre_merge) && (!early_check)){
		return true;
	}

	auto old_range = merge_range_list.front();

	bool range_decrease = false;
	if(old_range->LowerLimit() != LOWER_LIMIT){
		if(new_range->LowerLimit() != LOWER_LIMIT){
			auto ret = PredEquivlenceRange::LimitCompare(old_range->LowerLimit(),old_range->PredVarType(),new_range->LowerLimit(),new_range->PredVarType());
			range_decrease = (ret < 0) || (!ret && old_range->GetLowerBoundaryConstraint() && !new_range->GetLowerBoundaryConstraint());
			// range_decrease = (old_range->LowerLimit() < new_range->LowerLimit()) 
			// 			|| (old_range->LowerLimit() == new_range->LowerLimit() && old_range->GetLowerBoundaryConstraint() && !new_range->GetLowerBoundaryConstraint());
		}else{
			range_decrease = false;
		}
	}else{
		if(new_range->LowerLimit() != LOWER_LIMIT){
			range_decrease = true;
		}else{
			range_decrease = false;
		}
	}

	if(range_decrease){
		early_stop_ = false;
		return false;
	}

	if(old_range->UpperLimit() != UPPER_LIMIT){
		if(new_range->UpperLimit() != UPPER_LIMIT){
			auto ret = PredEquivlenceRange::LimitCompare(old_range->UpperLimit(),old_range->PredVarType(),new_range->UpperLimit(),new_range->PredVarType());
			range_decrease = (ret > 0) || (!ret && old_range->GetUpperBoundaryConstraint() && !new_range->GetUpperBoundaryConstraint());
			// range_decrease = (old_range->UpperLimit() > new_range->UpperLimit())
			// 			|| (old_range->UpperLimit() == new_range->UpperLimit() && old_range->GetUpperBoundaryConstraint() && !new_range->GetUpperBoundaryConstraint());
		}else{
			range_decrease = false;
		}
	}else{
		if(new_range->UpperLimit() != UPPER_LIMIT){
			range_decrease = true;
		}else{
			range_decrease = false;
		}
	}

	if(range_decrease){
		early_stop_ = false;
		return false;
	}
	
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
 * PredEquivlence::SuperSet: check if this input pe is a superset of the current pe
 */
bool PredEquivlence::SuperSet(PredEquivlence* pe){
	assert(pe);
	
	/*if one pe has more then one range,it must be return 0 tuple*/
	if(RangeCnt() >= 2 && !has_subquery_){
		return true;
	}

	/*here we just compare subquery name,such as SUBQUERY1, SUBQUERY2, we will compare */
	auto ranges = pe->GetRanges();
	std::vector<std::string> subquery_names;
	for(const auto r :ranges_){
		bool match = false;
		if(r->PredType() == PType::SUBQUERY || r->PredType() == PType::SUBLINK){
			assert(!r->GetSubqueryName().empty());
			subquery_names.push_back(r->GetSubqueryName());
			match = true;
		}else if(r->PredType() == PType::EQUAL || r->PredType() == PType::NOT_EQUAL || r->PredType() == PType::RANGE){
			for(const auto& src_r : ranges){
				bool super = true;
				/* check lowlimit */
				if(r->LowerLimit() == LOWER_LIMIT){
					if(src_r->LowerLimit() != LOWER_LIMIT){
						super = false;
						continue;
					}
				}else{
					if(src_r->LowerLimit() != LOWER_LIMIT){
						auto ret = PredEquivlenceRange::LimitCompare(r->LowerLimit(),r->PredVarType(),src_r->LowerLimit(),src_r->PredVarType());
						if(ret<0){
							super = false;
							continue;							
						}
						// if(r->LowerLimit() < src_r->LowerLimit()){
						// 	super = false;
						// 	continue;
						// }
					}			
				}

				/* check upperlimit */
				if(r->UpperLimit() == UPPER_LIMIT){
					if(src_r->UpperLimit() != UPPER_LIMIT){
						super = false;
						continue;
					}
				}else{
					if(src_r->UpperLimit() != UPPER_LIMIT){
						auto ret = PredEquivlenceRange::LimitCompare(r->UpperLimit(),r->PredVarType(),src_r->UpperLimit(),src_r->PredVarType());
						if(ret>0){
							super = false;
							continue;							
						}
						// if(r->UpperLimit() > src_r->UpperLimit()){
						// 	super = false;
						// 	continue;
						// }
					}			
				}
				if(super){
					match = true;
					break;
				}
			}
		}else{
			/*not support yet*/
			match = true;
		}
		if(!match){
			return false;
		}
	}

	/*externel check: subquery names in pe*/
	size_t sub_src_idx = 0;
	for(const auto& r : ranges){
		if(r->PredType() == PType::SUBQUERY || r->PredType() == PType::SUBLINK){
			if(sub_src_idx >= subquery_names.size()){
				return false;
			}
			if(r->GetSubqueryName() != subquery_names[sub_src_idx]){
				return false;
			}
			/**
			 * TODO: here we check the true subquery
			 */
			++sub_src_idx;
		}
	}
	return true;
}

bool PredEquivlence::Copy(PredEquivlence* pe){
	for(const auto& set : set_){
		pe->GetPredSet().insert(set);
	}
	for(const auto& range : ranges_){
		auto new_range = new PredEquivlenceRange();
		range->Copy(new_range);
		pe->UpdateRanges(new_range);
	}
	pe->SetSubLinkLevelPeLists(sublink_level_pe_lists_);
	pe->SetEarlyStop(early_stop_);
	//pe->SetChild(std::shared_ptr<PredEquivlence>(this));
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

std::string PredEquivlence::ShowPredEquivlence(int depth,std::string tag,SqmsLogger* logger){
	std::string str;
	str += "(name_sets: [";
	int idx = 0;
	for(auto iter = set_.begin();iter != set_.end();iter++){
		if (idx) 
			str += ",";
		str += (*iter);
		idx++;
	}
	
	str += "] , ";
	idx = 0;

	str += "range_sets:[";
	for(const auto& range : ranges_){
		if(idx)
			str += ",";
		str += range->PrintPredEquivlenceRange(depth,tag,logger);
		idx++;
	}

	if(sublink_level_pe_lists_.empty()){
		str += "])";
	}else{
		str += "] , sublinks: [\n";
		str += PrintIndent(depth+1,tag);
		for(const auto& subklink : sublink_level_pe_lists_){
			str += subklink.first;
			str += PrintIndent(depth+1,tag);
			str += subklink.second->GetTotalPredClassStr(depth+3);
		}
		str += PrintIndent(depth,tag);
		str += "])";
	}
	return str;
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

std::string PredEquivlence::ShowPredEquivlenceSets(int depth,std::string tag,SqmsLogger* logger){
	std::string str;
	str += "[";
	int idx = 0;
	for(auto iter = set_.begin();iter != set_.end();iter++){
		if (idx) 
			str +=",";
		str += *iter;
		idx++;
	}
	str += "]";
	return str;
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

	bool early_stop = true;
	for(const auto& pe : level_pe_sets_){
		early_stop &= pe->EarlyStop();
	}
	early_stop_ = early_stop;
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

	bool early_stop = true;
	for(const auto& pe : level_pe_sets_){
		early_stop &= pe->EarlyStop();
	}
	early_stop_ = early_stop;

	return true;
}

/**
 * LevelPredEquivlences::Insert
 */
bool LevelPredEquivlences::Insert(LevelPredEquivlences* lpes,bool pre_merged,bool early_check){
	assert(lpes);
	for(const auto& pe : *lpes){
		std::vector<PredEquivlence*>merge_pe_list;
		if(Serach(pe,merge_pe_list)){
			merge_pe_list.push_back(pe);
			if(!MergePredEquivlences(merge_pe_list,pre_merged,early_check)){
				return false;
			}
		}else{
			PredEquivlence* new_pe =  new PredEquivlence();
			pe->Copy(new_pe);
			level_pe_sets_.insert(new_pe);
		}
	}

	bool early_stop = true;
	for(const auto& pe : level_pe_sets_){
		early_stop &= pe->EarlyStop();
	}
	early_stop_ = early_stop;

	return true;
}

/**
 * LevelPredEquivlences::MergePredEquivlences,here 
 */
bool LevelPredEquivlences::MergePredEquivlences(const std::vector<PredEquivlence*>& merge_pe_list,bool pre_merged,bool early_check){
	PredEquivlence* new_pe = new PredEquivlence();
	int idx = 0;
	assert(merge_pe_list.size() >=2);
	for(const auto& mpe : merge_pe_list){
		if(idx == 0){
			mpe->Copy(new_pe);
		}else{
			new_pe->Insert(mpe,true,pre_merged,early_check);
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
	lpes->SetEarlyStop(early_stop_);
	lpes->SetLpeId(lpe_id_);
	return true;
}

/**
 * LevelPredEquivlences::Match
 */
bool LevelPredEquivlences::Match(LevelPredEquivlences* lpes){
	assert(lpes);
	for(const auto& pe : *lpes){
		for(const auto& attr : pe->GetPredSet()){
			if(key2pe_.find(attr) != key2pe_.end()){
				auto dst_pe = key2pe_[attr];
				if(dst_pe->SuperSet(pe)){
					continue;
				}else{
					return false;
				}
			}
		}
	}
	return true;
}

void LevelPredEquivlences::ShowLevelPredEquivlences(int depth){
	int idx  = 0; 
	for(const auto& pe : level_pe_sets_){
		if(idx)
			std::cout<<",";
		pe->ShowPredEquivlence(depth+1);
		idx++;
	}
	std::cout<<"}\n";
}

std::string LevelPredEquivlences::ShowLevelPredEquivlences(int depth,std::string tag, SqmsLogger* logger){
	std::string str;
	int idx = 0;
	for(const auto& pe : level_pe_sets_){
		if(idx)
			str += ",";
		str += pe->ShowPredEquivlence(depth+1,tag,logger);
		idx++;
	}
	str += "}\n";
	return str;
}

void LevelPredEquivlences::BuildKey2PeMap(){
	assert(!key2pe_.size());
	for(const auto& pe : level_pe_sets_){
		auto set = pe->GetPredSet();
		for(const auto& attr : set){
			key2pe_[attr] = pe;
		}
	}
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
bool LevelPredEquivlencesList::Insert(LevelPredEquivlencesList* lpes_list,bool is_or,bool pre_merge,bool early_check){
	assert(lpes_list);
	if(!lpes_list->Size()){
		return true;
	}	
	if(is_or){ /*or_model*/
		size_t src_idx = 0;
		for(const auto& lpes: *lpes_list){
			if(pre_merge)
				child_lpes_map_[lpes_list_.size()].push_back(src_idx++);
			Insert(lpes,true);
		}
	}else{ /*and_model*/
		if(lpes_list_.empty()){
			/*current level don't has any predicates,we just copy the pre level*/
			size_t src_idx = 0;
			for(const auto& src : *lpes_list){
				LevelPredEquivlences* new_lpes = new LevelPredEquivlences();
				src->Copy(new_lpes);
				if(pre_merge)
					child_lpes_map_[src_idx].push_back(src_idx);
				lpes_list_.push_back(new_lpes);
				src_idx++;
			}
		}else{
			int sz = lpes_list->Size()-1;
			const size_t lpes_sz = lpes_list_.size();
			while(sz > 0){
				/*here create new lpes, we should give them id */
				for(size_t i = 0;i < lpes_sz; ++i){
					LevelPredEquivlences* new_lpes = new LevelPredEquivlences();
					lpes_list_[i]->Copy(new_lpes);
					
					lpes_list_.push_back(new_lpes);

					auto child_pos = child_lpes_map_.find(i);
					if(child_pos != child_lpes_map_.end()){
						child_lpes_map_[lpes_list_.size()-1] = child_pos->second;
					}
					//new_lpes->SetLpeId(lpes_list_.size()-1);
				}
				--sz;
			}
			
			for(size_t i = 0;i<lpes_list_.size();){
				size_t src_idx = 0;
				for(const auto& src : *lpes_list){
					if(pre_merge){
						child_lpes_map_[i].push_back(src_idx++);
					}
					lpes_list_[i]->Insert(src,pre_merge,early_check);
					++i;
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
	new_lpes_list->SetChildLpesMap(child_lpes_map_);
}


void LevelPredEquivlencesList::ShowLevelPredEquivlencesList(int depth){
	for(size_t i = 0; i< lpes_list_.size();i++){
		PrintIndent(depth+1);
		std::cout<<"[id:"<<i<<", childs:";
		for(const auto& child : child_lpes_map_[i]){
			std::cout<<child<<",";
		}
		if(!child_lpes_map_[i].size()){
			std::cout<<",";
		}
		std::cout<<"ealry_stop:"<<lpes_list_[i]->EarlyStop();
		std::cout<<"]->";
		lpes_list_[i]->ShowLevelPredEquivlences(depth+1);
	}
}

std::string LevelPredEquivlencesList::ShowLevelPredEquivlencesList(int depth, std::string tag, SqmsLogger* logger){
	std::string str;
	for(size_t i = 0; i < lpes_list_.size();++i){
		str += PrintIndent(depth+1,tag);
		str += "[id:"+ std::to_string(i) + ", childs:";
		for(const auto& child : child_lpes_map_[i]){
			str += std::to_string(child) + ",";
		}
		str += "early_stop:" + std::string(lpes_list_[i]->EarlyStop()? "ture":"false");
		str += "]->";
		str +=  lpes_list_[i]->ShowLevelPredEquivlences(depth+1,tag,logger);
	}
	return str;
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

bool LevelManager::CancelQuery(){
    if(QueryCancelPending){
        return false;
    }
    QueryCancelPending = true;
    InterruptPending = true;
    ProcessInterrupts();
    elog(WARNING, "Query is canceled by sqms.");
    logger_->Logger("comming","Query is canceled by sqms.");
    return true;
}

void LevelManager::ShowPredClass(int height,std::string tag,int depth){
	assert(height>=0);
	
	PrintIndent(depth);
	std::cout << "level<" << height << ">pred_equivelnces: "<<std::endl;
	total_equivlences_[height]->ShowLevelPredEquivlencesList(depth+1);
	
	PrintIndent(depth);
	std::cout << "output cols:" << std::endl;
	total_outputs_[height]->ShowLevelOutputList(depth+1);

	PrintIndent(depth);
	std::cout << "tables: " << std::endl;
	total_tbls_[height]->ShowLevelTblList(depth+1);

	PrintIndent(depth);
	std::cout<<"groupby keys:"<<std::endl;
	total_aggs_[height]->ShowLevelAggAndSortList(depth+1);

	PrintIndent(depth);
	std::cout<<"sort keys:"<<std::endl;
	total_sorts_[height]->ShowLevelAggAndSortList(depth+1);
	
	PrintIndent(depth);
	PrintLine(50-depth);
}

std::string LevelManager::GetPredClassStr(int height,std::string tag,int depth){
	assert(height>=0);
	std::string str;
	
	str += PrintIndent(depth,log_tag_) + "level<" + std::to_string(height) +">pred_equivelnces: \n";
	str += total_equivlences_[height]->ShowLevelPredEquivlencesList(depth+1,tag,logger_);
	
	str += PrintIndent(depth,log_tag_) + std::string("output cols: \n");
	str += total_outputs_[height]->ShowLevelOutputList(depth+1,tag,logger_);
	
	str += PrintIndent(depth,log_tag_) + std::string("tables:");
	str += total_tbls_[height]->ShowLevelTblList(depth+1,tag,logger_);
	
	str += PrintIndent(depth,log_tag_) + std::string("groupby keys: \n");
	str += total_aggs_[height]->ShowLevelAggAndSortList(depth+1,tag,logger_);

	str += PrintIndent(depth,log_tag_)+std::string("sort keys: \n");
	str += total_sorts_[height]->ShowLevelAggAndSortList(depth+1,tag,logger_);

	str += PrintIndent(depth,log_tag_);
	str += PrintLine(50-depth,log_tag_);
	str += "\n";
	return str;
}

void LevelManager::ShowTotalPredClass(int depth){
	logger_->Logger(log_tag_.c_str(),GetTotalPredClassStr(depth).c_str());
}

std::string LevelManager::GetTotalPredClassStr(int depth){
	std::string str;
	str += "\n" + PrintIndent(depth,log_tag_) + "Total Pred Class: \n";
	for(int i = total_equivlences_.size()-1;i>=0;--i){
		str += GetPredClassStr(i,log_tag_,depth+1);
	}
	return str;
}
