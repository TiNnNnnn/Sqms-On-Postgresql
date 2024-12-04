#include "collect/stat_format.hpp"
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
    storage_ = new RedisSlowPlanStatProvider(std::string(redis_host),redis_port,totalFetchTimeoutMillis,totalSetTimeMillis,defaultTTLSeconds); 
}

bool PlanStatFormat::ProcQueryDesc(QueryDesc* qd){
    ExcavateContext *context = new ExcavateContext();
    Preprocessing(qd);

    size_t msg_size = history_slow_plan_stat__get_packed_size(hsps_);
    uint8_t *buffer = (uint8_t*)malloc(msg_size);
    if (buffer == NULL) {
        perror("Failed to allocate memory");
        return 1;
    }
    history_slow_plan_stat__pack(hsps_,buffer);
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
    ExplainState *es = NewExplainState();
    if(es == NULL)return false;

    es->format = EXPLAIN_FORMAT_JSON;
	es->verbose = true;
	es->analyze = true;
	es->costs = true;
    es->wal = false;
    es->summary = true;

    FormatBeginOutput(es);
    *hsps_ = FormatPrintPlan(es,qd);
    FormatEndOutput(es);
    return true;
}

std::string PlanStatFormat::HashCanonicalPlan(char *json_plan){
    return std::to_string(hash_fn(std::string(json_plan)));
}