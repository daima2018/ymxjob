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
then end_date=`date -d "${end_date} 1 days" "+%Y-%m-%d"`
fi

/opt/module/hive-3.1.2/bin/hive -e "
set hive.exec.dynamic.partition=true;  -- 开启动态分区，默认是false
set hive.exec.dynamic.partition.mode=nonstrict; -- 开启允许所有分区都是动态的，否则必须要有静态分区才能使用。

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
    ,min(case when ttt2.currency_code='USD' then ttt2.currency_rate end) as usd_rate  -- '美元汇率'     
    ,min(case when ttt2.currency_code='EUR' then ttt2.currency_rate end) as eur_rate  -- '欧元汇率'     
    ,min(case when ttt2.currency_code='GBP' then ttt2.currency_rate end) as gbp_rate  -- '英镑汇率'     
    ,min(case when ttt2.currency_code='JPY' then ttt2.currency_rate end) as jpy_rate  -- '日元汇率'     
    ,ttt2.currency_date        --汇率日期
from ymx.dwd_listing_d ttt1 
left join (
    select * from ymx.dwd_currency_rate_d
    where currency_date>='${start_date}' and currency_date<'${end_date}'
) ttt2                --汇率有多条会发散,聚合取一条
on ttt1.currency_site = ttt2.currency_local      --会导致数据倾斜,RMB币种的站点比较多
where ttt1.company_code='${company_code}'
group by  ttt1.site
       ,ttt1.user_account
       ,ttt1.parent_asin
       ,ttt1.asin
       ,ttt1.seller_sku
       ,ttt1.asin_type
       ,ttt1.currency_site
       ,ttt1.currency_local
       ,ttt2.currency_date
"

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] hive execute failed!"
     exit $?
fi