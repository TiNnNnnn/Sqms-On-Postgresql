#include "collect/stat_format.hpp"
#include <cstdlib>
#include <vector>
#include <functional>
#include "collect/node_mgr.hpp"

static std::hash<std::string> hash_fn;

PlanStatFormat& PlanStatFormat::getInstance() {
    static PlanStatFormat instance;
    return instance;
}

PlanStatFormat::~PlanStatFormat() {}

PlanStatFormat::PlanStatFormat(int psize)
    : pool_(std::make_shared<ThreadPool>(psize)),pool_size_(psize){
    history_slow_plan_stat__init(&hsps_);
    storage_ = std::make_shared<RedisSlowPlanStatProvider>(std::string(redis_host),redis_port,totalFetchTimeoutMillis,totalSetTimeMillis,defaultTTLSeconds); 
}

bool PlanStatFormat::ProcQueryDesc(QueryDesc* qd){
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
    
    pool_->submit([msg_size,buffer,this]() -> bool {

        ExcavateContext *context = new ExcavateContext();
        std::cout<<"Thread: "<<ThreadPool::GetTid()<<" Begin Working..."<<std::endl;
        /**
         * Due to the use of thread separation to ensure the main thread flow, we need 
         * to deeply copy the proto structure to the child threads
         */
        HistorySlowPlanStat *hsps = history_slow_plan_stat__unpack(NULL, msg_size, buffer);
        if(!hsps){
            std::cerr<<"history_slow_plan_stat__unpack failed in thered: "<<ThreadPool::GetTid()<<std::endl;
        }

        /*execute core slow query execavate strategy*/
        context->setStrategy(std::make_shared<NoProcessedExcavateStrategy>(hsps));
        std::vector<HistorySlowPlanStat*> list;
        context->executeStrategy(list);
        
        PlanFormatContext* pf_context = new PlanFormatContext();
        for(const auto& p : list){
            /*format strategy 1*/
            SlowPlanStat *sps= new SlowPlanStat();
            auto level_mgr = std::make_shared<LevelManager>(p,sps);
            pf_context->SetStrategy(level_mgr);
            pf_context->executeStrategy();

            auto node_collect_map = level_mgr->GetNodeCollector();
            if(debug){
                ShowAllNodeCollect(hsps,node_collect_map);
            }

            /*format strategt 2*/
            pf_context->SetStrategy(std::make_shared<NodeManager>(p,level_mgr));
            pf_context->executeStrategy();
        }

        /**
         * build fast filter tree, here need ensure thread safe,moreover,we
         * need rebuild it after db restarting
        */
        

        /**
         * TODO: 11-23 storage the slow sub query
         */
        // for(auto q : list){
        //     std::string hash_val = HashCanonicalPlan(q->json_plan);
        //     storage_->PutStat(hash_val,hsps);
        // }

        history_slow_plan_stat__free_unpacked(hsps,NULL);
        std::cout<<"Thread: "<<ThreadPool::GetTid()<<" Finish Working..."<<std::endl;
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
        std::cout << "  ";
    }
}

void PlanStatFormat::ShowAllHspsTree(HistorySlowPlanStat* hsps,int h_depth){
    if(!hsps)return;
    PrintIndent(h_depth);
    std::cout << "["<<hsps->node_type<<"]:"<<std::endl;
    ShowAllPredTree(hsps,h_depth+1);
    
    for(size_t i=0;i<hsps->n_childs;i++){
        ShowAllHspsTree(hsps->childs[i],h_depth+1);
    }
}

void PlanStatFormat::ShowAllNodeCollect(HistorySlowPlanStat* hsps,std::unordered_map<HistorySlowPlanStat*, NodeCollector*> map,int h_depth){
    if(!hsps)return;
    PrintIndent(h_depth);
    std::cout << "["<<hsps->node_type<<"]:"<<std::endl;
    
    auto& node_collector = map.at(hsps);
    assert(node_collector);

    ShowNodeCollect(node_collector,h_depth+1);
    
    for(size_t i=0;i<hsps->n_childs;i++){
        ShowAllNodeCollect(hsps->childs[i],map,h_depth+1);
    }
}

void PlanStatFormat::ShowNodeCollect(NodeCollector *nc ,int depth){
    if(nc->node_equivlences_){
        nc->node_equivlences_->ShowLevelPredEquivlencesList(depth);
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
        if(strlen(p_expr->qual->op)){
            PrintIndent(depth + 1);
            std::cout << "Left: " << p_expr->qual->left << std::endl;
            PrintIndent(depth + 1);
            std::cout << "Right: " << p_expr->qual->right << std::endl;
            PrintIndent(depth + 1);
            std::cout << "Operator: " << p_expr->qual->op << std::endl;
            if(strlen(p_expr->qual->use_or)){
                PrintIndent(depth + 1);
                std::cout << "Use_or: " << p_expr->qual->use_or << std::endl;
            }
            if(strlen(p_expr->qual->format_type)){
                PrintIndent(depth + 1);
                std::cout << "Format_type: " << p_expr->qual->format_type << std::endl;
            }
            
        }else if(p_expr->qual->sub_plan_name){
            PrintIndent(depth + 1);
            std::cout << "Hashed: " << ((p_expr->qual->hash_sub_plan == 0)?"false":"true")<< std::endl;
            PrintIndent(depth + 1);
            std::cout << "SubPlan: " << p_expr->qual->sub_plan_name << std::endl;
        }else{
        }
    } else {
        PrintIndent(depth);
        std::cerr << "Unknown expression type." << std::endl;
    }
}
