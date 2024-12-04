#include "collect/stat_format.hpp"
#include <vector>

PlanStatFormat& PlanStatFormat::getInstance() {
    static PlanStatFormat instance;
    return instance;
}

PlanStatFormat::~PlanStatFormat() {
    delete pool_;
    pool_ = nullptr;
}

PlanStatFormat::PlanStatFormat()
    : pool_(new ThreadPool(pool_size_)){}

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
        
        context->setStrategy(std::make_shared<CostBasedExcavateStrategy>(hsps));
        std::vector<HistorySlowPlanStat*> list;
        context->executeStrategy(list);

        /**
         * TODO: 11-23 storage the slow sub query
         */
        
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