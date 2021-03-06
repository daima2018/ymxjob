#!/bin/bash
#############################################################
#名称   子asin和独立asin表
#目标表 dwd_listing_d
#############################################################

source /home/ecm/ymx/ymxjob/src/main/common/functions.sh

#获取脚本参数
opts=$@

#解析脚本参数
start_date=`getparam start_date "$opts"`
end_date=`getparam end_date "$opts"`
company_code=`getparam company_code "$opts"`

start_date=`getdate "$start_date"`
end_date=`getdate "$end_date"`

hive -e "
set hive.exec.parallel=true;
set hive.strict.checks.cartesian.product=false;

insert overwrite table ymx.dwd_listing_d partition(company_code='${company_code}')
select
    tt1.site                  -- 站点                               
    ,tt1.user_account          -- 店铺账号                                  
    ,tt1.asin as parent_asin   -- 父asin和独立asin                                         
    ,tt2.asin1 as asin         -- 子asin和独立asin                            
    ,tt2.seller_sku            -- 卖家sku                                               
    ,1 as  asin_type           -- asin类型，1子ASIN 2独立                  
    ,tt1.currency_site         -- 站点币种                                                        
    ,tt1.currency_local        -- 本位币                                                            
from (select
        t2.site
        ,t1.user_account
        ,t1.asin
        ,t1.seller_sku
        ,t1.is_parent
        ,t2.currency_code currency_site   --站点币种
        ,t3.currency_local                --本位币
    from (select * from ymx.ods_amazon_parent_listing 
            where company_code = '${company_code}' and is_parent=1) t1
    left join (select site,user_account,currency_code from ymx.ods_platform_user
            where company_code = '${company_code}') t2
        on t1.user_account = t2.user_account
    left join (select currency_local from ymx.ods_company where company_code = '${company_code}') t3
) tt1 join ymx.ods_amazon_get_merchant_listings_data tt2
        on  tt2.company_code='${company_code}'
            and tt2.parent_asin != tt2.asin1
            and tt2.item_status='on_sale'
            and tt1.user_account=tt2.user_account
            and tt1.asin=tt2.parent_asin

union all

select
    t2.site
    ,t1.user_account
    ,t1.asin    as parent_asin
    ,t1.asin
    ,t1.seller_sku
    ,2 as asin_type
    ,t2.currency_code currency_site   --站点币种
    ,t3.currency_local                --本位币
from (select * from ymx.ods_amazon_parent_listing 
        where company_code = '${company_code}' and is_parent!=1) t1
left join (select site,user_account,currency_code from ymx.ods_platform_user
        where company_code = '${company_code}') t2
    on t1.user_account = t2.user_account
left join (select currency_local from ymx.ods_company where company_code = '${company_code}') t3
"

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] hive execute failed!"
     exit $?
fi