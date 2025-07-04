/* Generated by the protocol buffer compiler.  DO NOT EDIT! */
/* Generated from: src/include/collect/format.proto */

#ifndef PROTOBUF_C_src_2finclude_2fcollect_2fformat_2eproto__INCLUDED
#define PROTOBUF_C_src_2finclude_2fcollect_2fformat_2eproto__INCLUDED

#include <protobuf-c/protobuf-c.h>

PROTOBUF_C__BEGIN_DECLS

#if PROTOBUF_C_VERSION_NUMBER < 1003000
# error This file was generated by a newer version of protoc-c which is incompatible with your libprotobuf-c headers. Please update your headers.
#elif 1003003 < PROTOBUF_C_MIN_COMPILER_VERSION
# error This file was generated by an older version of protoc-c which is incompatible with your libprotobuf-c headers. Please regenerate this file with a newer version of protoc-c.
#endif


typedef struct _HistorySlowPlanStat HistorySlowPlanStat;
typedef struct _GroupSortKey GroupSortKey;
typedef struct _GroupKeys GroupKeys;
typedef struct _PredExpression PredExpression;
typedef struct _PredOperator PredOperator;
typedef struct _Quals Quals;
typedef struct _ProtoPredEquivlence ProtoPredEquivlence;
typedef struct _Range Range;
typedef struct _SlowPlanLevelStat SlowPlanLevelStat;
typedef struct _SlowPlanStat SlowPlanStat;


/* --- enums --- */

typedef enum _PredOperator__PredOperatorType {
  PRED_OPERATOR__PRED_OPERATOR_TYPE__AND = 0,
  PRED_OPERATOR__PRED_OPERATOR_TYPE__OR = 1,
  PRED_OPERATOR__PRED_OPERATOR_TYPE__NOT = 2
    PROTOBUF_C__FORCE_ENUM_TO_BE_INT_SIZE(PRED_OPERATOR__PRED_OPERATOR_TYPE)
} PredOperator__PredOperatorType;
typedef enum _PredTypeTag {
  PRED_TYPE_TAG__NONE = 0,
  PRED_TYPE_TAG__JOIN_COND = 1,
  PRED_TYPE_TAG__JOIN_FILTER = 2,
  PRED_TYPE_TAG__FILTER = 3,
  PRED_TYPE_TAG__ONE_TIME_FILTER = 4,
  PRED_TYPE_TAG__TID_COND = 5,
  PRED_TYPE_TAG__RECHECK_COND = 6,
  PRED_TYPE_TAG__INDEX_COND = 7,
  PRED_TYPE_TAG__ORDER_BY = 8
    PROTOBUF_C__FORCE_ENUM_TO_BE_INT_SIZE(PRED_TYPE_TAG)
} PredTypeTag;
typedef enum _ValType {
  VAL_TYPE__UNKNOWN = 0,
  VAL_TYPE__INT = 1,
  VAL_TYPE__DOUBLE = 2,
  VAL_TYPE__STRING = 3,
  VAL_TYPE__BOOL = 4,
  VAL_TYPE__NULL = 5
    PROTOBUF_C__FORCE_ENUM_TO_BE_INT_SIZE(VAL_TYPE)
} ValType;

/* --- messages --- */

struct  _HistorySlowPlanStat
{
  ProtobufCMessage base;
  double custom_plan_provider;
  double estimate_plan_width;
  double actual_nloops;
  double actual_start_up;
  double actual_total;
  double actual_rows;
  size_t n_childs;
  HistorySlowPlanStat **childs;
  char *node_type;
  char *strategy;
  /*
   *for aggregate
   */
  char *partial_mode;
  /*
   *for subplan
   */
  char *sub_plan_name;
  /*
   *for insert/update/delete
   */
  char *operation;
  /*
   *for scan (object tag and object name is a kv)
   */
  char *object_name;
  char *schema;
  char *object_tag;
  char *alia_name;
  /*
   *addtional info for indexscan/indexonlyscan
   */
  char *scan_dir;
  char *idx_name;
  /*
   *for join
   */
  char *join_type;
  protobuf_c_boolean inner_unique;
  /*
   *for setOp
   */
  char *command;
  /*
   *output cols
   */
  size_t n_output;
  char **output;
  char *relationship;
  double total_cost;
  char *sample_method;
  char *sampling_parameters;
  char *repeatable_seed;
  /*
   *total cost of current subquery
   */
  double sub_cost;
  /*
   *json of current subquery
   */
  char *json_plan;
  char *canonical_json_plan;
  /*
   *all node type of current subquery
   */
  size_t n_sub_node_type_set;
  char **sub_node_type_set;
  /*
   *for filter
   */
  char *qlabel;
  char *exprstr;
  /*
   *for groupby
   */
  char *key_name;
  char *keysetname;
  protobuf_c_boolean is_g_keys;
  /*
   *for groupby set, a set contains serval keys
   */
  size_t n_g_sets;
  GroupKeys **g_sets;
  /*
   *for groupby or sort keys
   */
  char *group_sort_qlabel;
  size_t n_group_sort_keys;
  GroupSortKey **group_sort_keys;
  int32_t node_tag;
  PredExpression *expr_root;
  PredTypeTag cur_expr_tag;
  /*
   *map<string,PredExpression> expr_tree = 45;
   */
  PredExpression *join_cond_expr_tree;
  PredExpression *join_filter_expr_tree;
  PredExpression *filter_tree;
  PredExpression *one_time_filter_tree;
  PredExpression *tid_cond_expr_tree;
  PredExpression *recheck_cond_tree;
  PredExpression *index_cond_expr_tree;
  PredExpression *order_by_expr_tree;
  /*
   *Auxiliary parameters
   */
  int32_t child_idx;
  int32_t subplan_idx;
  /*
   *we storage subplan divide from sommon childs
   */
  size_t n_subplans;
  HistorySlowPlanStat **subplans;
  char *canonical_node_json_plan;
  double node_cost;
  /*
   *cur hsp postiton in opne query's all subplan
   */
  int32_t sub_id;
};
#define HISTORY_SLOW_PLAN_STAT__INIT \
 { PROTOBUF_C_MESSAGE_INIT (&history_slow_plan_stat__descriptor) \
    , 0, 0, 0, 0, 0, 0, 0,NULL, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, 0, (char *)protobuf_c_empty_string, 0,NULL, (char *)protobuf_c_empty_string, 0, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, 0, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, 0,NULL, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, 0, 0,NULL, (char *)protobuf_c_empty_string, 0,NULL, 0, NULL, PRED_TYPE_TAG__NONE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0,NULL, (char *)protobuf_c_empty_string, 0, 0 }


struct  _GroupSortKey
{
  ProtobufCMessage base;
  /*
   * sort or groupby key
   * group_sort_qlabel == "Group Key" --> groupby key
   * group_sort_qlable == "Sort Key"  --> sort key
   */
  char *key;
  /*
   *sort order options
   */
  protobuf_c_boolean sort_operators;
  char *sort_collation;
  /*
   * DESC or ASC or NO_DEFAULT
   */
  char *sort_direction;
  /*
   * NULLS FIRST or NULLS LAST
   */
  char *sort_null_pos;
  protobuf_c_boolean presorted_key;
};
#define GROUP_SORT_KEY__INIT \
 { PROTOBUF_C_MESSAGE_INIT (&group_sort_key__descriptor) \
    , (char *)protobuf_c_empty_string, 0, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, 0 }


struct  _GroupKeys
{
  ProtobufCMessage base;
  char *key_name;
  size_t n_keys;
  char **keys;
};
#define GROUP_KEYS__INIT \
 { PROTOBUF_C_MESSAGE_INIT (&group_keys__descriptor) \
    , (char *)protobuf_c_empty_string, 0,NULL }


typedef enum {
  PRED_EXPRESSION__EXPR__NOT_SET = 0,
  PRED_EXPRESSION__EXPR_OP = 1,
  PRED_EXPRESSION__EXPR_QUAL = 2
    PROTOBUF_C__FORCE_ENUM_TO_BE_INT_SIZE(PRED_EXPRESSION__EXPR)
} PredExpression__ExprCase;

struct  _PredExpression
{
  ProtobufCMessage base;
  PredExpression__ExprCase expr_case;
  union {
    PredOperator *op;
    Quals *qual;
  };
};
#define PRED_EXPRESSION__INIT \
 { PROTOBUF_C_MESSAGE_INIT (&pred_expression__descriptor) \
    , PRED_EXPRESSION__EXPR__NOT_SET, {0} }


struct  _PredOperator
{
  ProtobufCMessage base;
  PredOperator__PredOperatorType type;
  size_t n_childs;
  PredExpression **childs;
};
#define PRED_OPERATOR__INIT \
 { PROTOBUF_C_MESSAGE_INIT (&pred_operator__descriptor) \
    , PRED_OPERATOR__PRED_OPERATOR_TYPE__AND, 0,NULL }


struct  _Quals
{
  ProtobufCMessage base;
  /*
   *Common qual
   */
  char *left;
  char *right;
  char *op;
  int64_t l_type;
  int64_t r_type;
  /*
   *T_ScalarArrayOpExpr
   */
  char *format_type;
  char *use_or;
  /*
   *SubQuery
   */
  protobuf_c_boolean hash_sub_plan;
  char *sub_plan_name;
  HistorySlowPlanStat *hsps;
  uint32_t left_val_type_id;
  uint32_t right_val_type_id;
  HistorySlowPlanStat *root_hsps;
};
#define QUALS__INIT \
 { PROTOBUF_C_MESSAGE_INIT (&quals__descriptor) \
    , (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, 0, 0, (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string, 0, (char *)protobuf_c_empty_string, NULL, 0, 0, NULL }


/*
 *TODO: reserved protobuf structure,no used currently
 */
struct  _ProtoPredEquivlence
{
  ProtobufCMessage base;
  size_t n_sets;
  char **sets;
  size_t n_ranges;
  Range **ranges;
};
#define PROTO_PRED_EQUIVLENCE__INIT \
 { PROTOBUF_C_MESSAGE_INIT (&proto_pred_equivlence__descriptor) \
    , 0,NULL, 0,NULL }


struct  _Range
{
  ProtobufCMessage base;
  char *str_lower_limit;
  char *str_upper_limit;
};
#define RANGE__INIT \
 { PROTOBUF_C_MESSAGE_INIT (&range__descriptor) \
    , (char *)protobuf_c_empty_string, (char *)protobuf_c_empty_string }


/*
 *plan stat in single level
 */
struct  _SlowPlanLevelStat
{
  ProtobufCMessage base;
  /*
   *join,filter
   */
  size_t n_pe_list;
  ProtoPredEquivlence **pe_list;
  size_t n_output_col_set;
  char **output_col_set;
  /*
   *Agg
   */
  /*
   *Limit
   */
  size_t n_group_by_set;
  char **group_by_set;
  /*
   *Sort
   */
  size_t n_sort_key_set;
  char **sort_key_set;
  protobuf_c_boolean stop;
};
#define SLOW_PLAN_LEVEL_STAT__INIT \
 { PROTOBUF_C_MESSAGE_INIT (&slow_plan_level_stat__descriptor) \
    , 0,NULL, 0,NULL, 0,NULL, 0,NULL, 0 }


struct  _SlowPlanStat
{
  ProtobufCMessage base;
  /*
   *size of stats is qual with slow plan's level num
   */
  size_t n_stats;
  SlowPlanLevelStat **stats;
};
#define SLOW_PLAN_STAT__INIT \
 { PROTOBUF_C_MESSAGE_INIT (&slow_plan_stat__descriptor) \
    , 0,NULL }


/* HistorySlowPlanStat methods */
void   history_slow_plan_stat__init
                     (HistorySlowPlanStat         *message);
size_t history_slow_plan_stat__get_packed_size
                     (const HistorySlowPlanStat   *message);
size_t history_slow_plan_stat__pack
                     (const HistorySlowPlanStat   *message,
                      uint8_t             *out);
size_t history_slow_plan_stat__pack_to_buffer
                     (const HistorySlowPlanStat   *message,
                      ProtobufCBuffer     *buffer);
HistorySlowPlanStat *
       history_slow_plan_stat__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data);
void   history_slow_plan_stat__free_unpacked
                     (HistorySlowPlanStat *message,
                      ProtobufCAllocator *allocator);
/* GroupSortKey methods */
void   group_sort_key__init
                     (GroupSortKey         *message);
size_t group_sort_key__get_packed_size
                     (const GroupSortKey   *message);
size_t group_sort_key__pack
                     (const GroupSortKey   *message,
                      uint8_t             *out);
size_t group_sort_key__pack_to_buffer
                     (const GroupSortKey   *message,
                      ProtobufCBuffer     *buffer);
GroupSortKey *
       group_sort_key__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data);
void   group_sort_key__free_unpacked
                     (GroupSortKey *message,
                      ProtobufCAllocator *allocator);
/* GroupKeys methods */
void   group_keys__init
                     (GroupKeys         *message);
size_t group_keys__get_packed_size
                     (const GroupKeys   *message);
size_t group_keys__pack
                     (const GroupKeys   *message,
                      uint8_t             *out);
size_t group_keys__pack_to_buffer
                     (const GroupKeys   *message,
                      ProtobufCBuffer     *buffer);
GroupKeys *
       group_keys__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data);
void   group_keys__free_unpacked
                     (GroupKeys *message,
                      ProtobufCAllocator *allocator);
/* PredExpression methods */
void   pred_expression__init
                     (PredExpression         *message);
size_t pred_expression__get_packed_size
                     (const PredExpression   *message);
size_t pred_expression__pack
                     (const PredExpression   *message,
                      uint8_t             *out);
size_t pred_expression__pack_to_buffer
                     (const PredExpression   *message,
                      ProtobufCBuffer     *buffer);
PredExpression *
       pred_expression__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data);
void   pred_expression__free_unpacked
                     (PredExpression *message,
                      ProtobufCAllocator *allocator);
/* PredOperator methods */
void   pred_operator__init
                     (PredOperator         *message);
size_t pred_operator__get_packed_size
                     (const PredOperator   *message);
size_t pred_operator__pack
                     (const PredOperator   *message,
                      uint8_t             *out);
size_t pred_operator__pack_to_buffer
                     (const PredOperator   *message,
                      ProtobufCBuffer     *buffer);
PredOperator *
       pred_operator__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data);
void   pred_operator__free_unpacked
                     (PredOperator *message,
                      ProtobufCAllocator *allocator);
/* Quals methods */
void   quals__init
                     (Quals         *message);
size_t quals__get_packed_size
                     (const Quals   *message);
size_t quals__pack
                     (const Quals   *message,
                      uint8_t             *out);
size_t quals__pack_to_buffer
                     (const Quals   *message,
                      ProtobufCBuffer     *buffer);
Quals *
       quals__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data);
void   quals__free_unpacked
                     (Quals *message,
                      ProtobufCAllocator *allocator);
/* ProtoPredEquivlence methods */
void   proto_pred_equivlence__init
                     (ProtoPredEquivlence         *message);
size_t proto_pred_equivlence__get_packed_size
                     (const ProtoPredEquivlence   *message);
size_t proto_pred_equivlence__pack
                     (const ProtoPredEquivlence   *message,
                      uint8_t             *out);
size_t proto_pred_equivlence__pack_to_buffer
                     (const ProtoPredEquivlence   *message,
                      ProtobufCBuffer     *buffer);
ProtoPredEquivlence *
       proto_pred_equivlence__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data);
void   proto_pred_equivlence__free_unpacked
                     (ProtoPredEquivlence *message,
                      ProtobufCAllocator *allocator);
/* Range methods */
void   range__init
                     (Range         *message);
size_t range__get_packed_size
                     (const Range   *message);
size_t range__pack
                     (const Range   *message,
                      uint8_t             *out);
size_t range__pack_to_buffer
                     (const Range   *message,
                      ProtobufCBuffer     *buffer);
Range *
       range__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data);
void   range__free_unpacked
                     (Range *message,
                      ProtobufCAllocator *allocator);
/* SlowPlanLevelStat methods */
void   slow_plan_level_stat__init
                     (SlowPlanLevelStat         *message);
size_t slow_plan_level_stat__get_packed_size
                     (const SlowPlanLevelStat   *message);
size_t slow_plan_level_stat__pack
                     (const SlowPlanLevelStat   *message,
                      uint8_t             *out);
size_t slow_plan_level_stat__pack_to_buffer
                     (const SlowPlanLevelStat   *message,
                      ProtobufCBuffer     *buffer);
SlowPlanLevelStat *
       slow_plan_level_stat__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data);
void   slow_plan_level_stat__free_unpacked
                     (SlowPlanLevelStat *message,
                      ProtobufCAllocator *allocator);
/* SlowPlanStat methods */
void   slow_plan_stat__init
                     (SlowPlanStat         *message);
size_t slow_plan_stat__get_packed_size
                     (const SlowPlanStat   *message);
size_t slow_plan_stat__pack
                     (const SlowPlanStat   *message,
                      uint8_t             *out);
size_t slow_plan_stat__pack_to_buffer
                     (const SlowPlanStat   *message,
                      ProtobufCBuffer     *buffer);
SlowPlanStat *
       slow_plan_stat__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data);
void   slow_plan_stat__free_unpacked
                     (SlowPlanStat *message,
                      ProtobufCAllocator *allocator);
/* --- per-message closures --- */

typedef void (*HistorySlowPlanStat_Closure)
                 (const HistorySlowPlanStat *message,
                  void *closure_data);
typedef void (*GroupSortKey_Closure)
                 (const GroupSortKey *message,
                  void *closure_data);
typedef void (*GroupKeys_Closure)
                 (const GroupKeys *message,
                  void *closure_data);
typedef void (*PredExpression_Closure)
                 (const PredExpression *message,
                  void *closure_data);
typedef void (*PredOperator_Closure)
                 (const PredOperator *message,
                  void *closure_data);
typedef void (*Quals_Closure)
                 (const Quals *message,
                  void *closure_data);
typedef void (*ProtoPredEquivlence_Closure)
                 (const ProtoPredEquivlence *message,
                  void *closure_data);
typedef void (*Range_Closure)
                 (const Range *message,
                  void *closure_data);
typedef void (*SlowPlanLevelStat_Closure)
                 (const SlowPlanLevelStat *message,
                  void *closure_data);
typedef void (*SlowPlanStat_Closure)
                 (const SlowPlanStat *message,
                  void *closure_data);

/* --- services --- */


/* --- descriptors --- */

extern const ProtobufCEnumDescriptor    pred_type_tag__descriptor;
extern const ProtobufCEnumDescriptor    val_type__descriptor;
extern const ProtobufCMessageDescriptor history_slow_plan_stat__descriptor;
extern const ProtobufCMessageDescriptor group_sort_key__descriptor;
extern const ProtobufCMessageDescriptor group_keys__descriptor;
extern const ProtobufCMessageDescriptor pred_expression__descriptor;
extern const ProtobufCMessageDescriptor pred_operator__descriptor;
extern const ProtobufCEnumDescriptor    pred_operator__pred_operator_type__descriptor;
extern const ProtobufCMessageDescriptor quals__descriptor;
extern const ProtobufCMessageDescriptor proto_pred_equivlence__descriptor;
extern const ProtobufCMessageDescriptor range__descriptor;
extern const ProtobufCMessageDescriptor slow_plan_level_stat__descriptor;
extern const ProtobufCMessageDescriptor slow_plan_stat__descriptor;

PROTOBUF_C__END_DECLS


#endif  /* PROTOBUF_C_src_2finclude_2fcollect_2fformat_2eproto__INCLUDED */
