#include "collect/stat_format.hpp"
#include "collect/equivlence.hpp"
#include <cstdlib>
#include <vector>
#include <functional>

static std::hash<std::string> hash_fn;

PlanStatFormat& PlanStatFormat::getInstance() {
    static PlanStatFormat instance;
    return instance;
}

PlanStatFormat::~PlanStatFormat() {
    history_slow_plan_stat__free_unpacked(&hsps_,NULL);
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
        EquivelenceManager* qm =  new EquivelenceManager(hsps,sps);
        for(const auto& p : list){
            qm->ComputeEquivlenceClass();
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

        history_slow_plan_stat__free_unpacked(hsps,NULL);

        return true;
    });
    return true;
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
        ShowAllHspsTree(&hsps_);
        //ShowAllPredTree(&hsps_);
    }
    
    pfree(total_es);
    pfree(total_ces);
    return true;
}

std::string PlanStatFormat::HashCanonicalPlan(char *json_plan){
    return std::to_string(hash_fn(std::string(json_plan)));
}

void PlanStatFormat::PrintIndent(int depth) {
    for (size_t i = 0; i < depth; ++i) {
        std::cout << "  ";
    }
}

void PlanStatFormat::ShowAllHspsTree(HistorySlowPlanStat* hsps,int h_depth){
    if(!hsps)return;
    PrintIndent(h_depth);
    std::cout << "["<<hsps->node_type<<"]:"<<std::endl;
    //PrintIndent(h_depth+1);
    ShowAllPredTree(hsps,h_depth+1);
    
    for(size_t i=0;i<hsps->n_childs;i++){
        ShowAllHspsTree(hsps->childs[i],h_depth+1);
    }

}

void PlanStatFormat::ShowAllPredTree(HistorySlowPlanStat* hsps,int depth){
    assert(hsps);
    if(hsps->join_cond_expr_tree){
        PrintIndent(depth);
        std::cout<<"<join_cond>:"<<std::endl;
        ShowPredTree(hsps->join_cond_expr_tree,depth+1);
    }
    if(hsps->join_filter_expr_tree){
        PrintIndent(depth);
        std::cout<<"<join_filter>:"<<std::endl;
        ShowPredTree(hsps->join_filter_expr_tree,depth+1);
    }
    if(hsps->filter_tree){
        PrintIndent(depth);
        std::cout<<"<filter>:"<<std::endl;
        ShowPredTree(hsps->filter_tree,depth+1);
    }
    if(hsps->one_time_filter_tree){
        PrintIndent(depth);
        std::cout<<"<one_time_cond>:"<<std::endl;
        ShowPredTree(hsps->one_time_filter_tree,depth+1);
    }
	if(hsps->tid_cond_expr_tree){
        PrintIndent(depth);
        std::cout<<"<tid_cond>:"<<std::endl;
        ShowPredTree(hsps->tid_cond_expr_tree,depth+1);
    }
    if(hsps->recheck_cond_tree){
        PrintIndent(depth);
        std::cout<<"<recheck_cond>:"<<std::endl;
        ShowPredTree(hsps->recheck_cond_tree,depth+1);
    }
	if(hsps->index_cond_expr_tree){
        PrintIndent(depth);
        std::cout<<"<index_cond>:"<<std::endl;
        ShowPredTree(hsps->index_cond_expr_tree,depth+1);
    }
    if(hsps->order_by_expr_tree){
        PrintIndent(depth);
        std::cout<<"<join_cond:true>"<<std::endl;
        //ShowPredTree(hsps->order_by_expr_tree);
    }
}

void PlanStatFormat::ShowPredTree(PredExpression* p_expr, int depth) {
    if (p_expr == nullptr) {
        PrintIndent(depth);
        std::cout << "pred expression is nullptr, no preducate exsist in node" << std::endl;
        return;
    }

    if (p_expr->expr_case == PRED_EXPRESSION__EXPR_OP) {
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

        for (size_t i = 0; i < p_expr->op->n_childs; ++i) {
            ShowPredTree(p_expr->op->childs[i], depth + 1);
        }
    } else if (p_expr->expr_case == PRED_EXPRESSION__EXPR_QUAL) {
       
        PrintIndent(depth);
        std::cout << "Quals: " << std::endl;

        PrintIndent(depth + 1);
        std::cout << "Left: " << p_expr->qual->left << std::endl;

        PrintIndent(depth + 1);
        std::cout << "Right: " << p_expr->qual->right << std::endl;

        PrintIndent(depth + 1);
        std::cout << "Operator: " << p_expr->qual->op << std::endl;
    } else {
        PrintIndent(depth);
        std::cerr << "Unknown expression type." << std::endl;
    }
}
