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
    
    pfree(total_es);
    pfree(total_ces);
    return true;
}

std::string PlanStatFormat::HashCanonicalPlan(char *json_plan){
    return std::to_string(hash_fn(std::string(json_plan)));
}