#include "collect/stat_format.hpp"

PlanStatFormat& PlanStatFormat::getInstance() {
    static PlanStatFormat instance;
    return instance;
}

PlanStatFormat::~PlanStatFormat() {
    delete pool_;
    pool_ = nullptr;
}

PlanStatFormat::PlanStatFormat()
    : ps_(nullptr), pool_(new ThreadPool(pool_size_)){}

bool PlanStatFormat::ProcQueryDesc(QueryDesc* qd){
    ExcavateContext *context = new ExcavateContext();
    Preprocessing(qd);
    // pool_->submit([&]() -> bool {
    //     context->setStrategy(std::make_shared<CostBasedExcavateStrategy>(ps_));
    //     context->executeStrategy();
    //     /**
    //      * TODO: 11-23 storage the slow sub query
    //      */
    //     return true;
    // });
    return true;
}

bool PlanStatFormat::Preprocessing(QueryDesc* qd){
    ExplainState *es = NewExplainState();
    
    es->format = EXPLAIN_FORMAT_JSON;
	es->verbose = true;
	es->analyze = true;
	es->costs = true;
    es->wal = false;
    es->summary = true;

    FormatBeginOutput(es);
    FormatPrintPlan(es,qd);
    FormatEndOutput(es);
    
    return true;
}

void PlanStatFormat::ParsePlan(QueryDesc *qd, List *ancestors,const char *relationship, const char *plan_name){
	
}