#include "collect/stat_format.hpp"
#include <cstdlib>
#include <vector>
#include <functional>

static std::hash<std::string> hash_fn;

PlanStatFormat& PlanStatFormat::getInstance() {
    static PlanStatFormat instance;
    return instance;
}

PlanStatFormat::~PlanStatFormat() {
    delete pool_;
    pool_ = nullptr;
}

PlanStatFormat::PlanStatFormat()
    : pool_(new ThreadPool(pool_size_)){
    history_slow_plan_stat__init(&hsps_);
    storage_ = new RedisSlowPlanStatProvider(std::string(redis_host),redis_port,totalFetchTimeoutMillis,totalSetTimeMillis,defaultTTLSeconds); 
}

bool PlanStatFormat::ProcQueryDesc(QueryDesc* qd){
    ExcavateContext *context = new ExcavateContext();
    Preprocessing(qd);
    if(!&hsps_){
        std::cout<<"hsps is nullptr"<<std::endl;
        return -1;
    }
    size_t msg_size = history_slow_plan_stat__get_packed_size(&hsps_);
    uint8_t *buffer = (uint8_t*)malloc(msg_size);
    if (buffer == NULL) {
        perror("Failed to allocate memory");
        return 1;
    }
    history_slow_plan_stat__pack(&hsps_,buffer);
    pool_->submit([&]() -> bool {
        /**
         * Due to the use of thread separation to ensure the main thread flow, we need 
         * to deeply copy the proto structure to the child threads
         */
        HistorySlowPlanStat *hsps = history_slow_plan_stat__unpack(NULL, msg_size, buffer);

        /*execute core slow query execavate strategy*/
        context->setStrategy(std::make_shared<CostBasedExcavateStrategy>(hsps));
        std::vector<HistorySlowPlanStat*> list;
        context->executeStrategy(list);

        SlowPlanStat *sps= new SlowPlanStat();
        for(const auto& p : list){
            ComputeEquivlenceClass(p,sps);
        }

        /*build fast filter tree, here need ensure thread safe,moreover,we
        need rebuild it after db restarting*/
 
        /**
         * TODO: 11-23 storage the slow sub query
         */
        for(auto q : list){
            std::string hash_val = HashCanonicalPlan(q->json_plan);
            storage_->PutStat(hash_val,hsps);
        }
        return true;
    });
    return true;
}

/**
 * ComputeEquivlenceClass: calulate the equivelence class and its containment for
 * each level for plan
 */
void PlanStatFormat::ComputeEquivlenceClass(HistorySlowPlanStat* hsps,SlowPlanStat *sps){
    
    std::vector<HistorySlowPlanStat*>levels;
    std::vector<HistorySlowPlanStat*>tmp_levels;
    std::vector<std::vector<HistorySlowPlanStat*>> level_collector;

    levels.push_back(hsps);
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

    for(auto &lc : level_collector){
        ComputLevlEquivlenceClass(lc,sps);
    }
}

void PlanStatFormat::ComputLevlEquivlenceClass(const std::vector<HistorySlowPlanStat*>& list,SlowPlanStat *sps){
    for(const auto& s : list){
        /*parse exprstr first,then caluate equivlence class*/
        ParseExprs(s);    
    }
}

void PlanStatFormat::ParseExprs(HistorySlowPlanStat* hsps){
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


typedef struct PredLevelList{
    std::vector<Quals*> and_list;
    std::vector<Quals*> or_list;
    std::vector<Quals*> not_list;
}PredLevelList;

/**PredDecompose: merge expr form bottom to top
 * example:
 *  n_quals: 4
 *  pred: tt.a > 10 | parent: 2,and
 *  pred: max(tt.c) > 10 | parent: 2,and
 *  pred: tt.a < 20 | parent: 1,or
 *  pred: tt.a > 0 | parent: 1,or
 *  
 *  we first get preds value on parent = 2, use a stack to merge, stop until
 *  the stack onlu has one/two element,then we get preds value on parent =1,repeated
 *  action above
 * 
 */
void PlanStatFormat::PredDecompose(HistorySlowPlanStat* hsp){
    std::unordered_map<int,PredLevelList> location2qual;
    int max_parent = 0;
    for(size_t i=0;i<hsp->n_quals;i++){
        auto qual = hsp->quals[i];
        max_parent = std::max(max_parent,qual->parent_location);
        if(strcmp(qual->op,"and")){
            location2qual[qual->parent_location].and_list.push_back(qual);
        }else if(strcmp(qual->op,"or")){
            location2qual[qual->parent_location].or_list.push_back(qual);
        }else if(strcmp(qual->op,"not")){
            location2qual[qual->parent_location].not_list.push_back(qual);
        }else{
            std::cerr<<"unknow compare predicate opeartor type: "<<qual->op<<std::endl;
            return;
        }
    }

    for(int i = max_parent; i>=0; i--){
        auto pred_level_list = location2qual[i];
    }
}

/**
 *Preprocessing: parse all sub query (include itself) into json format 
 */
bool PlanStatFormat::Preprocessing(QueryDesc* qd){
    ExplainState *total_es = NewFormatState();
    ExplainState *total_ces = NewFormatState();
    if(total_es == NULL || total_ces == NULL)return false;

    FormatBeginOutput(total_es);
    FormatBeginOutput(total_ces);
    hsps_ = FormatPrintPlan(total_es,total_ces,qd);
    FormatEndOutput(total_ces);
    FormatEndOutput(total_es);

    if(debug){
       
        // std::cout<<"n_quals: "<<hsps_.n_quals<<std::endl;
        // for(int i=0;i<hsps_.n_quals;i++){ 
            
        //     std::string str = std::string(hsps_.quals[i]->left) + " " + 
        //         std::string(hsps_.quals[i]->op) +  " " +std::string(hsps_.quals[i]->right);
        //     std::cout<<"pred: "<<str.c_str()<<" | parent: "<<hsps_.quals[i]->parent_location<<","<<hsps_.quals[i]->parent_op<<hsps_.quals[i]->parent_op_id<<std::endl;
        // }
        // std::cout<<"and locations:"<<std::endl;
        // for(int i=0;i<hsps_.n_and_locations;i++){
        //     std::cout<<hsps_.and_locations[i]<<" ";
        // }
        // std::cout<<std::endl;
        // std::cout<<"or locations:"<<std::endl;
        // for(int i=0;i<hsps_.n_or_locations;i++){
        //     std::cout<<hsps_.or_locations[i]<<" ";
        // }
        // std::cout<<std::endl;


        // for(int i=0;i<hsps_.n_childs;i++){
        //     auto hsp = hsps_.childs[i]; 
        //     std::cout<<"n_quals: "<<hsp->n_quals<<std::endl;
        //     for(int i=0;i<hsp->n_quals;i++){
        //         std::string str = std::string(hsp->quals[i]->left) + " " + 
        //             std::string(hsp->quals[i]->op) +  " " +std::string(hsp->quals[i]->right);
        //         std::cout<<"pred: "<<str.c_str()<<" | parent: "<<hsp->quals[i]->parent_location<<","<<hsp->quals[i]->parent_op<<hsp->quals[i]->parent_op_id<<std::endl;
        //     }
        //     std::cout<<"and locations:"<<std::endl;
        //     for(int i=0;i<hsp->n_and_locations;i++){
        //         std::cout<<hsp->and_locations[i]<<" ";
        //     }
        //     std::cout<<std::endl;
        //     std::cout<<"or locations:"<<std::endl;
        //     for(int i=0;i<hsp->n_or_locations;i++){
        //         std::cout<<hsp->or_locations[i]<<" ";
        //     }            
        //     std::cout<<std::endl;
        // }
        ShowPredTree(hsps_.expr_root);
    }
    
    pfree(total_es);
    pfree(total_ces);
    return true;
}

std::string PlanStatFormat::HashCanonicalPlan(char *json_plan){
    return std::to_string(hash_fn(std::string(json_plan)));
}

void PlanStatFormat::PrintIndent(int depth) {
    for (int i = 0; i < depth; ++i) {
        std::cout << "  ";  // 每层深度输出两个空格
    }
}

void PlanStatFormat::ShowPredTree(PredExpression* p_expr, int depth) {
    if (p_expr == nullptr) {
        PrintIndent(depth);
        std::cout << "pred expression is nullptr" << std::endl;
        return;
    }

    if (p_expr->expr_case == PRED_EXPRESSION__EXPR_OP) {
        // 打印 PredOperator 节点
        PrintIndent(depth);
        std::cout << "Operator: ";

        switch (p_expr->op->type) {
            case PRED_OPERATOR__PRED_OPERATOR_TYPE__AND:
                std::cout << "AND";
                break;
            case PRED_OPERATOR__PRED_OPERATOR_TYPE__OR:
                std::cout << "OR";
                break;
            case PRED_OPERATOR__PRED_OPERATOR_TYPE__NOT:
                std::cout << "NOT";
                break;
            default:
                std::cout << "UNKNOWN";
                break;
        }
        std::cout << std::endl;

        // 遍历子节点
        for (size_t i = 0; i < p_expr->op->n_childs; ++i) {
            ShowPredTree(p_expr->op->childs[i], depth + 1);
        }
    } else if (p_expr->expr_case == PRED_EXPRESSION__EXPR_QUAL) {
        // 打印 Quals 节点
        PrintIndent(depth);
        std::cout << "Quals: " << std::endl;

        PrintIndent(depth + 1);
        std::cout << "Left: " << p_expr->qual->left << std::endl;

        PrintIndent(depth + 1);
        std::cout << "Right: " << p_expr->qual->right << std::endl;

        PrintIndent(depth + 1);
        std::cout << "Operator: " << p_expr->qual->op << std::endl;

        PrintIndent(depth + 1);
        std::cout << "Parent Location: " << p_expr->qual->parent_location << std::endl;

        PrintIndent(depth + 1);
        std::cout << "Parent Operator: " << p_expr->qual->parent_op << std::endl;

        PrintIndent(depth + 1);
        std::cout << "Parent Operator ID: " << p_expr->qual->parent_op_id << std::endl;
    } else {
        PrintIndent(depth);
        std::cerr << "Unknown expression type." << std::endl;
    }
}
