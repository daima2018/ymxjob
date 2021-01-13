CREATE TABLE if not exists dwd_currency_rate_d (
    `date` string  COMMENT '汇率日期',
    `currency_local` string  COMMENT '本位币',
    `currency_code` string COMMENT '币种简称',
    `currency_rate` decimal(8,4) COMMENT '系统汇率'
) comment '汇率表'
row format delimited fields terminated by '\t' stored as orc;