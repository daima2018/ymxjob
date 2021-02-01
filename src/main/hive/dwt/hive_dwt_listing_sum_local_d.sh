#!/bin/bash
#############################################################
#名称   listing统计表(本地时间)
#目标表 dwt_listing_sum_local_d
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
set hive.exec.dynamic.partition=true;           
set hive.exec.dynamic.partition.mode=nonstrict; 
set hive.exec.parallel=true;

insert overwrite table ymx.dwt_listing_sum_local_d partition(company_code='${company_code}',stat_date)
select
      user_account                   -- '店铺账号'
     ,site                           --  '站点'
     ,'' as seller_sku             --
     ,parent_asin as asin            --
     ,nvl(sum(qty)                 ,0) as qty    -- '销售数量',
     ,nvl(sum(sale_amount)         ,0) as sale_amount      -- '销售额(本位币)',
     ,nvl(sum(sale_amount_usd)     ,0) as sale_amount_usd      -- '销售额美元',
     ,nvl(sum(sale_amount_eur)     ,0) as sale_amount_eur      -- '销售额欧元',
     ,nvl(sum(sale_amount_gbp)     ,0) as sale_amount_gbp      -- '销售额英镑',
     ,nvl(sum(sale_amount_jpy)     ,0) as sale_amount_jpy      -- '销售额日元',
     ,nvl(sum(sale_amount_original),0) as sale_amount_original      -- '销售额（站点原始金额）',
     ,date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') as created_time         -- '创建时间',
     ,date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') as updated_time         -- '更新时间',
     ,nvl(sum(sale_order_num  )  ,0)   as sale_order_num       -- '订单数',
     ,nvl(sum(refund_amount   )  ,0)   as refund_amount       -- '退款listing个数',
     ,nvl(sum(refund_money    )  ,0)   as refund_money       -- '退款金额(站点币种)',
     ,nvl(sum(refund_money_usd)  ,0)   as refund_money_usd       -- '退款金额美元',
     ,nvl(sum(refund_money_eur)  ,0)   as refund_money_eur       -- '退款金额欧元',
     ,nvl(sum(refund_money_gbp)  ,0)   as refund_money_gbp       -- '退款金额英镑',
     ,nvl(sum(refund_money_jpy)  ,0)   as refund_money_jpy       -- '退款金额日元',
     ,nvl(sum(refund_money_local),0)   as refund_money_local       -- '退款金额(本位币)',
     ,md5(concat(user_account,parent_asin)) as key1   --父asin没有seller_sku     -- 'user_account+asin+seller_sku 的MD5值',
     ,nvl(sum(return_amount),0)                                  -- '退货数',
     ,3 as asin_type                                      -- '0未确定, 子asin:1，独立产品:2, 父asin3',
     ,nvl(sum(ad_qty)                 ,0) -- '广告销售数量',
     ,nvl(sum(ad_sale_amount)         ,0) -- '广告销售额(本位币)',
     ,nvl(sum(ad_sale_amount_usd     ),0) -- '广告销售额美元',
     ,nvl(sum(ad_sale_amount_eur     ),0) -- '广告销售额欧元',
     ,nvl(sum(ad_sale_amount_gbp     ),0) -- '广告销售额英镑',
     ,nvl(sum(ad_sale_amount_jpy     ),0) -- '广告销售额日元',
     ,nvl(sum(ad_sale_order_num      ),0) -- '广告订单数',
     ,nvl(sum(ad_sale_amount_original),0) -- '广告销售额（站点原始金额）',
     ,nvl(sum(cost                   ),0) -- '广告花费(站点币种)',
     ,nvl(sum(cost_local   ),0)           -- '广告花费(本位币)',
     ,nvl(sum(cost_usd     ),0)     
     ,nvl(sum(cost_eur     ),0)     
     ,nvl(sum(cost_gbp     ),0)     
     ,nvl(sum(cost_jpy     ),0)     
     ,nvl(sum(clicks       ),0)                              -- '广告访问次数,也即广告点击数'
     ,nvl(sum(impressions  ),0)                              -- 广告曝光量
     ,nvl(max(sessions)             ,0) as sessions                 -- 访客次数
     ,nvl(max(page_views)           ,0) as page_views               --浏览次数
     ,nvl(max(buy_box_percentage)   ,0) as buy_box_percentage       --  buy_box_percentage
     ,nvl(max(session_percentage)   ,0) as session_percentage       --买家访问次数比率
     ,nvl(max(page_views_percentage),0) as page_views_percentage    --   浏览次数比率
     ,stat_date
from (
    select                                                                       
         stat_date
        ,user_account
        ,site
        ,parent_asin
        ,qty
        ,sale_amount
        ,sale_amount_usd
        ,sale_amount_eur
        ,sale_amount_gbp
        ,sale_amount_jpy
        ,sale_amount_original
        ,sale_order_num
        ,refund_amount
        ,refund_money
        ,refund_money_usd
        ,refund_money_eur
        ,refund_money_gbp
        ,refund_money_jpy
        ,refund_money_local
        ,return_amount
        ,ad_qty
        ,ad_sale_amount
        ,ad_sale_amount_usd
        ,ad_sale_amount_eur
        ,ad_sale_amount_gbp
        ,ad_sale_amount_jpy
        ,ad_sale_order_num
        ,ad_sale_amount_original
        ,cost
        ,cost_local
        ,cost_usd
        ,cost_eur
        ,cost_gbp
        ,cost_jpy
        ,clicks
        ,impressions
        ,0 as sessions
        ,0 as page_views
        ,0 as buy_box_percentage
        ,0 as session_percentage
        ,0 as page_views_percentage
    from ymx.dwm_child_listing_sum_local_d 
    where company_code='${company_code}' and stat_date>='${start_date}' and stat_date<'${end_date}'        
    union all 
    SELECT
         generate_date as stat_date
        ,user_account
        ,site
        ,parent_asin
        ,0 as qty
        ,0 as sale_amount
        ,0 as sale_amount_usd
        ,0 as sale_amount_eur
        ,0 as sale_amount_gbp
        ,0 as sale_amount_jpy
        ,0 as sale_amount_original
        ,0 as sale_order_num
        ,0 as refund_amount
        ,0 as refund_money
        ,0 as refund_money_usd
        ,0 as refund_money_eur
        ,0 as refund_money_gbp
        ,0 as refund_money_jpy
        ,0 as refund_money_local
        ,0 as return_amount
        ,0 as ad_qty
        ,0 as ad_sale_amount
        ,0 as ad_sale_amount_usd
        ,0 as ad_sale_amount_eur
        ,0 as ad_sale_amount_gbp
        ,0 as ad_sale_amount_jpy
        ,0 as ad_sale_order_num
        ,0 as ad_sale_amount_original
        ,0 as cost
        ,0 as cost_local
        ,0 as cost_usd
        ,0 as cost_eur
        ,0 as cost_gbp
        ,0 as cost_jpy
        ,0 as clicks
        ,0 as impressions
        ,nvl(max(sessions),0) as sessions
        ,nvl(max(page_views),0) as page_views
        ,nvl(max(buy_box_percentage),0) as buy_box_percentage
        ,nvl(max(session_percentage),0) as session_percentage
        ,nvl(max(page_views_percentage),0) as page_views_percentage
    from ymx.ods_amazon_business_report_by_parent
    WHERE generate_date>='${start_date}' and generate_date<'${end_date}'
    group by generate_date
         ,user_account
         ,site
         ,parent_asin        
) c
group by c.stat_date
        ,c.user_account                    
        ,c.site                               
        ,c.parent_asin            

union all

select 
    user_account
    ,site
    ,seller_sku
    ,asin
    ,qty
    ,sale_amount
    ,sale_amount_usd
    ,sale_amount_eur
    ,sale_amount_gbp
    ,sale_amount_jpy
    ,sale_amount_original
    ,created_time
    ,updated_time
    ,sale_order_num
    ,refund_amount
    ,refund_money
    ,refund_money_usd
    ,refund_money_eur
    ,refund_money_gbp
    ,refund_money_jpy
    ,refund_money_local
    ,key1
    ,return_amount
    ,asin_type
    ,ad_qty
    ,ad_sale_amount
    ,ad_sale_amount_usd
    ,ad_sale_amount_eur
    ,ad_sale_amount_gbp
    ,ad_sale_amount_jpy
    ,ad_sale_order_num
    ,ad_sale_amount_original
    ,cost
    ,cost_local
    ,cost_usd
    ,cost_eur
    ,cost_gbp
    ,cost_jpy
    ,clicks
    ,impressions
    ,sessions
    ,page_views
    ,buy_box_percentage
    ,session_percentage
    ,page_views_percentage
    ,stat_date
from ymx.dwm_child_listing_sum_local_d 
where company_code='${company_code}' 
    and stat_date>='${start_date}' and stat_date<'${end_date}'
"

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] hive execute failed!"
     exit $?
fi