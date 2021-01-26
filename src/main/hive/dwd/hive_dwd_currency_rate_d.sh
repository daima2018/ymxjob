#!/bin/bash
#############################################################
#名称   汇率表
#目标表 dwd_currency_rate_d
#############################################################

source /home/ecm/ymx/ymxjob/src/main/common/functions.sh

#获取脚本参数
opts=$@

#解析脚本参数
start_date=`getparam start_date "$opts"`
end_date=`getparam end_date "$opts"`

start_date=`getdate "$start_date"`
end_date=`getdate "$end_date"`

hive -e "
insert overwrite table ymx.dwd_currency_rate_d
select
     a.crr_date            as currency_date     
    ,a.crr_local           as currency_local  --本位币
    ,b.crrd_currency_code  as currency_code   --币种
    ,b.crrd_currency_rate  as currency_rate   --汇率
from ymx.ods_currency_rate_rule a
left join ymx.ods_currency_rate_rule_detail b
    on a.crr_id=b.crr_id
"

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] hive execute failed!"
     exit $?
fi