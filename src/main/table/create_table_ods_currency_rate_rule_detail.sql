CREATE TABLE if not exists ods_currency_rate_rule_detail (
     crrd_id             int COMMENT ''
    ,crr_id              int COMMENT '规则ID'
    ,crrd_currency_code  string COMMENT '币种简称'
    ,crrd_currency_rate  decimal(8,4) COMMENT '系统汇率'
    ,created_time        string COMMENT '创建时间'
    ,updated_time        string COMMENT '更新时间'
    ,ods_create_time     string comment '导入数据时间'
) COMMENT '汇率明细表'
row format delimited fields terminated by '\t' stored as textfile;