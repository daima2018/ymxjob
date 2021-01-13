#!/bin/bash
#############################################################
#名称   子asin和独立asin表
#目标表 dwd_listing_d
#############################################################

#获取脚本参数
opts=$@
getparam(){
    arg=$1
    echo $opts |xargs -n1|cut -b 2- |awk -F '=' '{if($1=="'"$arg"'") print $2}'
}
#解析脚本参数
start_date=`getparam start_date`
end_date=`getparam end_date`
company_code=`getparam company_code`

/opt/module/hive-3.1.2/bin/hive -e "
insert overwrite table ymx.dwd_listing_d partition(company_code='${company_code}')
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
              when ttt1.currency_local= ttt2.crrd_currency_code then ttt2.crrd_currency_rate
     end)  as local_rate      -- 本位币汇率                                                
    ,min(case when ttt2.crrd_currency_code='USD' then ttt2.crrd_currency_rate end) as usd_rate  -- '美元汇率'     
    ,min(case when ttt2.crrd_currency_code='EUR' then ttt2.crrd_currency_rate end) as eur_rate  -- '欧元汇率'     
    ,min(case when ttt2.crrd_currency_code='GBP' then ttt2.crrd_currency_rate end) as gbp_rate  -- '英镑汇率'     
    ,min(case when ttt2.crrd_currency_code='JPY' then ttt2.crrd_currency_rate end) as jpy_rate  -- '日元汇率'     
from (select
        tt1.site
       ,tt1.user_account
       ,tt1.asin   as parent_asin
       ,tt2.asin1            as asin
       ,tt2.seller_sku
       ,1 as asin_type
       ,tt1.currency_site
       ,tt1.currency_local
    from (select
            t2.site
            ,t1.user_account
            ,t1.asin
            ,t1.seller_sku
            ,t1.is_parent
            ,t2.currency_code currency_site   --站点币种
            ,t3.currency_local                --本位币
        from (select * from ymx.ods_amazon_parent_listing 
               where company_code = '${company_code}') t1
        left join (select site,user_account,currency_code from ymx.ods_platform_user
                where company_code = '${company_code}') t2
           on t1.user_account = t2.user_account
        left join (select currency_local from ymx.ods_company where company_code = '${company_code}') t3
    ) tt1 join ymx.ods_amazon_get_merchant_listings_data tt2
            on tt1.is_parent=1
                and tt2.parent_asin != tt2.asin1
                and tt2.item_status='on_sale'
                and tt1.user_account=tt2.user_account
                and tt1.asin=tt2.parent_asin
    
    union all

    select
        tt1.site
        ,tt1.user_account
        ,tt1.asin   as parent_asin
        ,tt1.asin
        ,tt1.seller_sku
        ,2 as asin_type
        ,tt1.currency_site
        ,tt1.currency_local
    from (select
            t2.site
            ,t1.user_account
            ,t1.asin
            ,t1.seller_sku
            ,t1.is_parent
            ,t2.currency_code currency_site   --站点币种
            ,t3.currency_local                --本位币
        from (select * from ymx.ods_amazon_parent_listing 
               where company_code = '${company_code}') t1
        left join (select site,user_account,currency_code from ymx.ods_platform_user
                where company_code = '${company_code}') t2
           on t1.user_account = t2.user_account
        left join (select currency_local from ymx.ods_company where company_code = '${company_code}') t3
    ) tt1
    where tt1.is_parent!=1
) ttt1 left join (
    select
        a.crr_local  --本位币
         ,b.crrd_currency_code  --币种
         ,b.crrd_currency_rate  --汇率
    from ymx.ods_currency_rate_rule a
    left join ymx.ods_currency_rate_rule_detail b
        on a.crr_id=b.crr_id
    where a.crr_date='${start_date}'
) ttt2                --汇率有多条会发散,聚合取一条
on ttt1.currency_site = ttt2.crr_local      --会导致数据倾斜
group by  ttt1.site
       ,ttt1.user_account
       ,ttt1.parent_asin
       ,ttt1.asin
       ,ttt1.seller_sku
       ,ttt1.asin_type
       ,ttt1.currency_site
       ,ttt1.currency_local
"

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] hive execute failed!"
     exit $?
fi