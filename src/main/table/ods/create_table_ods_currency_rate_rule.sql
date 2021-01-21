CREATE TABLE if not exists ymx.ods_currency_rate_rule (
     crr_id       int comment ''
    ,crr_date     string COMMENT '汇率时间时间'
    ,crr_local    string COMMENT '本位币'
    ,created_time string COMMENT '创建时间'
    ,updated_time string COMMENT '更新时间'
    ,ods_create_time   string comment '导入数据时间'
) COMMENT '汇率表'
row format delimited fields terminated by '\t' stored as textfile;

create table if not exists ymx_tmp.ods_currency_rate_rule like ymx.ods_currency_rate_rule;