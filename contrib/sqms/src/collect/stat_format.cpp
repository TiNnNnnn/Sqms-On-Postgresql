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

        /*build fast filter tree, here need ensure thread safe,moreover,we
        need rebuild it after db restarting*/
        

        /**
         * TODO: 11-23 storage the slow sub query
         */
        for(auto q : list){
            std::string hash_val = HashCanonicalPlan(q->json_plan_);
            storage_->PutStat(hash_val,hsps);
        }
        return true;
    });
    return true;
}

bool PlanStatFormat::Preprocessing(QueryDesc* qd){
    ExplainState *total_es = NewFormatState();
    if(total_es == NULL)return false;

    FormatBeginOutput(total_es);
    hsps_ = FormatPrintPlan(total_es,qd);
    FormatEndOutput(total_es);

    pfree(total_es);
    return true;
}

std::string PlanStatFormat::HashCanonicalPlan(char *json_plan){
    return std::to_string(hash_fn(std::string(json_plan)));
}