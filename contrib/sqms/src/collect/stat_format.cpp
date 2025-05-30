#include "discovery/query_index.hpp"
#include "collect/stat_format.hpp"
#include <cstdlib>
#include <vector>
#include <functional>
#include <queue>
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

    bool found = false;
    logger_ = (SqmsLogger*)ShmemInitStruct("SqmsLogger", sizeof(SqmsLogger), &found);
    assert(found);
}

bool PlanStatFormat::ProcQueryDesc(QueryDesc* qd, MemoryContext oldcxt,bool slow){
    Preprocessing(qd);
    if(!&hsps_){
        std::cout<<"hsps is nullptr"<<std::endl;
        return -1;
    }
 
    // size_t msg_size = history_slow_plan_stat__get_packed_size(&hsps_);
    // uint8_t *buffer = (uint8_t*)malloc(msg_size);
    // if (buffer == NULL) {
    //     perror("Failed to allocate memory");
    //     return 1;
    // }
    // history_slow_plan_stat__pack(&hsps_,buffer);
    pid_t pid = getpid();

    //std::thread t([msg_size,buffer,slow,pid,this,oldcxt,qd]() -> bool{
        std::cout<<"Thread: "<<ThreadPool::GetTid()<<" Begin Working..."<<std::endl;
        /**
         * Due to the use of thread separation to ensure the main thread flow, we need 
         * to deeply copy the proto structure to the child threads
         */
        // HistorySlowPlanStat *hsps = history_slow_plan_stat__unpack(NULL, msg_size, buffer);
        // if(!hsps){
        //     std::cerr<<"history_slow_plan_stat__unpack failed in thered: "<<ThreadPool::GetTid()<<std::endl;
        // }

        auto hsps = &hsps_;

        bool found = true;
        auto shared_index = (HistoryQueryLevelTree*)ShmemInitStruct(shared_index_name, sizeof(HistoryQueryLevelTree), &found);
        if(!found || !shared_index){
            std::cerr<<"shared_index not exist!"<<std::endl;
            exit(-1);
        }

        if(slow){
            //logger_->Logger("slow","begin process comming query...");
            std::cout<<"begin process slow query"<<std::endl;
            ExcavateContext *context = new ExcavateContext();
            /*execute core slow query execavate strategy*/
            context->setStrategy(std::make_shared<NoProcessedExcavateStrategy>(hsps));
            std::vector<HistorySlowPlanStat*> list;
            context->executeStrategy(list);
            std::cout<<"[slow] finish execute strategy"<<std::endl;
            for(const auto& p : list){
                /*format strategy 1*/
                SlowPlanStat *sps= new SlowPlanStat();
                auto level_mgr = std::make_shared<LevelManager>(p,sps,logger_,"slow");

                PlanFormatContext* pf_context_1 = new PlanFormatContext();
                pf_context_1->SetStrategy(level_mgr);
                pf_context_1->executeStrategy();
                //level_mgr->ShowTotalPredClass();

                // if(debug){
                //     auto node_collect_map = level_mgr->GetNodeCollector();
                //     logger_->Logger("slow",ShowAllNodeCollect(hsps,node_collect_map,"slow").c_str());
                // }

                /*format strategt 2*/
                PlanFormatContext* pf_context_2 = new PlanFormatContext();
                auto node_manager = std::make_shared<NodeManager>(p,level_mgr,pid);
                pf_context_2->SetStrategy(node_manager);
                pf_context_2->executeStrategy();
                
                // if(!shared_index->Insert(level_mgr.get(),1)){
                //     logger_->Logger("slow","shared_index insert error in strategy 1");
                //     exit(-1);
                // }else{
                //     logger_->Logger("slow","shared_index insert level_mgr success");
                // }
                /*node strategy*/

                for(const auto& node_collector : node_manager->GetNodeCollectorList()){
                    //std::cout<<"NODE time: "<<node_collector->time<<",output:"<<node_collector->output<<std::endl;
                    if(!shared_index->Insert(node_collector)){
                        logger_->Logger("slow","shared_index insert error in strategy 2");
                        exit(-1);
                    }
                }
            }
            /**
             * TODO: 11-23 storage the slow sub query
             */
            // for(auto q : list){
            //     std::string hash_val = HashCanonicalPlan(q->json_plan);
            //     storage_->PutStat(hash_val,hsps);
            // }
            //logger_->Logger("slow","finish process slow query...");
            std::cout<<"[slow]finish process slow query..."<<std::endl;
        }else{
            std::cout<<"begin process comming query"<<std::endl;
            //logger_->Logger("comming","begin process comming query...");
            // /*check all subuqueries in plan*/
            std::vector<HistorySlowPlanStat*>sub_list;
            LevelOrder(hsps,sub_list);
            std::cout<<"[comming] finish level ordering"<<std::endl;
            //logger_->Logger("comming",("subplan size: "+std::to_string(sub_list.size())).c_str());
            size_t sub_idx = 0;
            // for(const auto& p : sub_list){
            //     /**
            //      * MARK: to make test more easy,here move muti-thread while testing
            //      */

            //     // size_t msg_size = history_slow_plan_stat__get_packed_size(p);
            //     // uint8_t *buffer = (uint8_t*)malloc(msg_size);
            //     // if (buffer == NULL) {
            //     //     perror("Failed to allocate memory");
            //     //     exit(-1);
            //     // }
            //     // history_slow_plan_stat__pack(p,buffer);
            //     //pool_->submit([shared_index,msg_size,buffer,pid,this]()->bool{
            //         //HistorySlowPlanStat *p = history_slow_plan_stat__unpack(NULL, msg_size, buffer);
            //         // if(!p){
            //         //     std::cerr<<"history_slow_plan_stat__unpack failed in thered: "<<ThreadPool::GetTid()<<std::endl;
            //         // }
            //         //logger_->Logger("comming",("**********sub_query ["+std::to_string(sub_idx)+"]***************").c_str());
            //         SlowPlanStat *sps= new SlowPlanStat();
            //         PlanFormatContext* pf_context_1 = new PlanFormatContext();
            //         auto level_mgr = std::make_shared<LevelManager>(p,sps,logger_,"comming");
            //         pf_context_1->SetStrategy(level_mgr);
            //         pf_context_1->executeStrategy();
            //         //level_mgr->ShowTotalPredClass();

            //         // if(debug){
            //         //     auto node_collect_map = level_mgr->GetNodeCollector();
            //         //     logger_->Logger("comming",ShowAllNodeCollect(p,node_collect_map,"comming").c_str());
            //         // }

            //         // if(shared_index->Search(level_mgr.get(),1)){
            //         //     CancelQuery(pid);
            //         //     //elog(NOTICE, "query has been canceled!");
            //         //     logger_->Logger("comming","****************************");
            //         //     break;
            //         // }
            //         // logger_->Logger("comming","****************************");
            //         //return true;
            //     //});
            //     ++sub_idx;
            // }

            /*node strategy*/
            auto top_p = sub_list[0];
            SlowPlanStat *sps= new SlowPlanStat();
            PlanFormatContext* pf_context_1 = new PlanFormatContext();
            auto level_mgr = std::make_shared<LevelManager>(top_p,sps,logger_,"comming");
            pf_context_1->SetStrategy(level_mgr);
            pf_context_1->executeStrategy();
            //level_mgr->ShowTotalPredClass();
            std::cout<<"finish execute strategy1"<<std::endl;

            PlanFormatContext* pf_context_2 = new PlanFormatContext();
            auto node_mgr = std::make_shared<NodeManager>(top_p,level_mgr,pid);
            pf_context_2->SetStrategy(node_mgr);
            pf_context_2->executeStrategy();
            node_mgr->Search();
            std::cout<<"finish process comming query"<<std::endl;
            //logger_->Logger("comming","finish process comming query...");
        }
        //history_slow_plan_stat__free_unpacked(hsps,NULL);
        std::cout<<"Thread: "<<ThreadPool::GetTid()<<" Finish Working..."<<std::endl;
        return true;
    //});
    //t.detach();
    //return true;
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

void PlanStatFormat::LevelOrder(HistorySlowPlanStat* hsps,std::vector<HistorySlowPlanStat*>& sub_list){
    std::queue<HistorySlowPlanStat*>q;
    int level_size = 1;
    q.push(hsps);
    while(!q.empty()){
        std::vector<HistorySlowPlanStat*>v;
        while(level_size--){
            auto front = q.front();
            q.pop();
            sub_list.push_back(front);
            v.push_back(front);
            for(size_t i=0;i<front->n_childs;++i){
                q.push(front->childs[i]);
            }
        }
        level_size = q.size();
    }
}

bool PlanStatFormat::CancelQuery(pid_t pid){
    Datum arg = Int32GetDatum(pid);
    Datum result = DirectFunctionCall1(pg_cancel_backend, arg);
    bool success = DatumGetBool(result);
    if(success){
        logger_->Logger("comming","cancel query success...");
        return true;
    } else {
        logger_->Logger("comming","cancel query failed or has been canceled...");
        return false;
    }
}

void PlanStatFormat::PrintIndent(int depth) {
    for (int i = 0; i < depth; ++i) {
        std::cout << "  ";
    }
}

std::string PlanStatFormat::GetIndent(int depth){
    std::string str;
    for(int i = 0;i < depth; ++i ){
        str += "  ";
    }
    return str;
}

void PlanStatFormat::ShowAllHspsTree(HistorySlowPlanStat* hsps,int h_depth){
    if(!hsps)return;
    PrintIndent(h_depth);
    std::cout << "["<<hsps->node_type<<"]:"<<std::endl;

    ShowAllPredTree(hsps,h_depth+1);
    ShowSubPlansTree(hsps,h_depth);
    
    for(size_t i=0;i<hsps->n_childs;i++){
        ShowAllHspsTree(hsps->childs[i],h_depth+1);
    }
}

std::string PlanStatFormat::ShowAllNodeCollect(HistorySlowPlanStat* hsps,
    std::unordered_map<HistorySlowPlanStat*, NodeCollector*> map,std::string tag,int h_depth){
    assert(hsps);
    std::string str = "\n";
    str += GetIndent(h_depth);
    str += "[" + std::string(hsps->node_type) + "]:\n";
    
    auto& node_collector = map.at(hsps);
    assert(node_collector);

    str += ShowNodeCollect(node_collector,tag,h_depth+1);
    
    for(size_t i=0;i<hsps->n_childs;i++){
        str += ShowAllNodeCollect(hsps->childs[i],map,tag,h_depth+1);
    }

    return str;
}

std::string PlanStatFormat::ShowNodeCollect(NodeCollector *nc ,std::string tag,int depth){
    std::string str;
    if(nc->node_equivlences_){
        str += nc->node_equivlences_->ShowLevelPredEquivlencesList(depth,tag,logger_);
    }
    
    if(nc->node_tbls_){
        str += GetIndent(depth+1);
        str += "tables: ";
        str += nc->node_tbls_->ShowLevelTblList(depth,tag,logger_);
    }

    if(nc->node_outputs_){
        str += nc->node_outputs_->ShowLevelOutputList(depth+1,tag,logger_);
    }

    if(nc->node_aggs_){
        str += GetIndent(depth+1);
        str += "groupby keys:";
        str += nc->node_aggs_->ShowLevelAggAndSortList(depth-1,tag,logger_);
    }

    if(nc->node_sorts_){
        str += GetIndent(depth+1);
        str += "sort keys:";
        str += nc->node_sorts_->ShowLevelAggAndSortList(depth-1,tag,logger_);
    }
    return str;
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

void PlanStatFormat::ShowSubPlansTree(HistorySlowPlanStat* hsps,int depth){
    auto subplans = hsps->subplans;
    for(size_t i=0;i<hsps->n_subplans;i++){
        PrintIndent(depth+1);
        auto plan = subplans[i];
        std::cout<<plan->sub_plan_name<<":"<<std::endl;
        ShowAllHspsTree(plan,depth+2);
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
            if(strlen(p_expr->qual->sub_plan_name)){
                std::cout << "Right:" << p_expr->qual->sub_plan_name <<std::endl;
            }else{
                std::cout << "Right: " << p_expr->qual->right << std::endl;
            }
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
        }else if(strlen(p_expr->qual->sub_plan_name) && !strlen(p_expr->qual->op)){
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
