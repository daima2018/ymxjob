#!/bin/bash
#############################################################
#名称   listing汇率表
#目标表 dwd_listing_currency_rate_d
#############################################################

#获取脚本参数
opts=$@
getparam(){
    arg=$1
    echo $opts |xargs -n1|cut -b 2- |awk -F '=' '{if($1=="'"$arg"'") print $2}'
}

#解析脚本参数,传入格式为yyyy-MM-dd
start_date=`getparam start_date`
end_date=`getparam end_date`
company_code=`getparam company_code`

#默认情况只跑一天的
if [ "$end_date" = "" ]  
then end_date=`date -d "${start_date} 1 days" "+%Y-%m-%d"`
fi

start_flag=$start_date
string_array=''
while [[ $start_flag < $end_date ]]
do
    echo $start_flag
    string_array=$string_array","$start_flag
    start_flag=`date -d "${start_flag} 1 days" "+%Y-%m-%d"`
done
echo $start_flag

if [[ $string_array != "" ]]
then string_array=${string_array:1} #去掉第一个, 
fi
echo $string_array


hive -e "
set hive.exec.dynamic.partition=true;            
set hive.exec.dynamic.partition.mode=nonstrict; 
set mapred.reduce.tasks=4;
set hive.exec.parallel=true;

insert overwrite table ymx.dwd_listing_currency_rate_d partition(company_code='${company_code}',currency_date)
select
     ttt1.site               -- 站点                         
    ,ttt1.user_account       -- 店铺账号                                         
    ,ttt1.parent_asin        -- 父asin和独立asin                                       
    ,ttt1.asin               -- 子asin和独立asin                         
    ,ttt1.seller_sku         -- 卖家sku                                     
    ,ttt1.asin_type          -- asin类型，1子ASIN 2独立                        
    ,ttt1.currency_site      -- 站点币种                                           
    ,ttt1.currency_local     -- 本位币                                              
    ,min(case when ttt1.currency_site = ttt1.currency_local then 1
              when ttt1.currency_local= ttt2.currency_code then ttt2.currency_rate
     end)  as local_rate      -- 本位币汇率                                                
    ,min(case when ttt1.currency_site='USD' then 1
            when ttt2.currency_code='USD' then ttt2.currency_rate end) as usd_rate  -- '美元汇率'     
    ,min(case when ttt1.currency_site='EUR' then 1
            when ttt2.currency_code='EUR' then ttt2.currency_rate end) as eur_rate  -- '欧元汇率'     
    ,min(case when ttt1.currency_site='GBP' then 1
            when ttt2.currency_code='GBP' then ttt2.currency_rate end) as gbp_rate  -- '英镑汇率'     
    ,min(case when ttt1.currency_site='JPY' then 1 
            when ttt2.currency_code='JPY' then ttt2.currency_rate end) as jpy_rate  -- '日元汇率'     
    ,ttt1.currency_date        --汇率日期
from (select a.*,b.currency_date from ymx.dwd_listing_d a
     lateral view explode(split('${string_array}',',')) b as currency_date
     where a.company_code='${company_code}'
)ttt1 
left join (
    select * from ymx.dwd_currency_rate_d
    where currency_date>='${start_date}' and currency_date<'${end_date}'
) ttt2                --汇率有多条会发散,聚合取一条
on ttt1.currency_date=ttt2.currency_date
    and ttt1.currency_site = ttt2.currency_local     
group by  ttt1.site
       ,ttt1.user_account
       ,ttt1.parent_asin
       ,ttt1.asin
       ,ttt1.seller_sku
       ,ttt1.asin_type
       ,ttt1.currency_site
       ,ttt1.currency_local
       ,ttt1.currency_date
"

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] hive execute failed!"
     exit $?
fi