#!/bin/bash
#############################################################
#名称   listing统计表(本地时间)
#目标表 dwt_listing_sum_local_d
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
insert overwrite table ymx.dwt_listing_sum_local_d partition(company_code='${company_code}',stat_date='${start_date}')
select
      c.user_account                   -- '店铺账号'
     ,c.site                           --  '站点'
     ,null as seller_sku             --
     ,c.parent_asin as asin            --
     ,sum(d.qty)                  as qty    -- '销售数量',
     ,sum(d.sale_amount)          as sale_amount      -- '销售额(本位币)',
     ,sum(d.sale_amount_usd)      as sale_amount_usd      -- '销售额美元',
     ,sum(d.sale_amount_eur)      as sale_amount_eur      -- '销售额欧元',
     ,sum(d.sale_amount_gbp)      as sale_amount_gbp      -- '销售额英镑',
     ,sum(d.sale_amount_jpy)      as sale_amount_jpy      -- '销售额日元',
     ,sum(d.sale_amount_original) as sale_amount_original      -- '销售额（站点原始金额）',
     ,date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') as created_time         -- '创建时间',
     ,date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') as updated_time         -- '更新时间',
     ,sum(d.sale_order_num  )     as sale_order_num       -- '订单数',
     ,sum(d.refund_amount   )     as refund_amount       -- '退款listing个数',
     ,sum(d.refund_money    )     as refund_money       -- '退款金额(站点币种)',
     ,sum(d.refund_money_usd)     as refund_money_usd       -- '退款金额美元',
     ,sum(d.refund_money_eur)     as refund_money_eur       -- '退款金额欧元',
     ,sum(d.refund_money_gbp)     as refund_money_gbp       -- '退款金额英镑',
     ,sum(d.refund_money_jpy)     as refund_money_jpy       -- '退款金额日元',
     ,sum(d.refund_money_local)   as refund_money_local       -- '退款金额(本位币)',
     ,md5(concat(c.user_account,c.parent_asin)) as key1   --父asin没有seller_sku     -- 'user_account+asin+seller_sku 的MD5值',
     ,sum(d.return_amount)                                  -- '退货数',
     ,3 as asin_type                                      -- '0未确定, 子asin:1，独立产品:2, 父asin3',
     ,sum(d.ad_qty)                  -- '广告销售数量',
     ,sum(d.ad_sale_amount)          -- '广告销售额(本位币)',
     ,sum(d.ad_sale_amount_usd     ) -- '广告销售额美元',
     ,sum(d.ad_sale_amount_eur     ) -- '广告销售额欧元',
     ,sum(d.ad_sale_amount_gbp     ) -- '广告销售额英镑',
     ,sum(d.ad_sale_amount_jpy     ) -- '广告销售额日元',
     ,sum(d.ad_sale_order_num      ) -- '广告订单数',
     ,sum(d.ad_sale_amount_original) -- '广告销售额（站点原始金额）',
     ,sum(d.cost                   ) -- '广告花费(站点币种)',
     ,sum(d.cost_local   )           -- '广告花费(本位币)',
     ,sum(d.cost_usd     )     
     ,sum(d.cost_eur     )     
     ,sum(d.cost_gbp     )     
     ,sum(d.cost_jpy     )     
     ,sum(d.clicks       )                              -- '广告访问次数,也即广告点击数'
     ,sum(d.impressions  )                              -- 广告曝光量
     ,nvl(max(h.sessions)             ,0) as sessions                 -- 访客次数
     ,nvl(max(h.page_views)           ,0) as page_views               --浏览次数
     ,nvl(max(h.buy_box_percentage)   ,0) as buy_box_percentage       --  buy_box_percentage
     ,nvl(max(h.session_percentage)   ,0) as session_percentage       --买家访问次数比率
     ,nvl(max(h.page_views_percentage),0) as page_views_percentage    --   浏览次数比率
from (select * from ymx.dwd_listing_currency_rate_d 
     where company_code='${company_code}' and currency_date='${start_date}'
) c
left join (select * from ymx.dwm_child_listing_sum_local_d 
            where company_code='${company_code}' and stat_date='${start_date}'
) d
on  c.user_account = d.user_account
    and c.site = d.site
    and c.asin = d.asin
    and c.seller_sku = d.seller_sku
left join (SELECT
                 user_account
                ,parent_asin
                ,nvl(max(sessions),0) as sessions
                ,nvl(max(page_views),0) as page_views
                ,nvl(max(buy_box_percentage),0) as buy_box_percentage
                ,nvl(max(session_percentage),0) as session_percentage
                ,nvl(max(page_views_percentage),0) as page_views_percentage
           from ymx.ods_amazon_business_report_by_parent
           WHERE generate_date='${start_date}'
           group by user_account
                  ,parent_asin
) h
on  c.user_account = h.user_account
    and c.parent_asin = h.parent_asin
where c.asin_type=1
group by c.user_account                    
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
from ymx.dwm_child_listing_sum_local_d 
where company_code='${company_code}' and stat_date='${start_date}'
"

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] hive execute failed!"
     exit $?
fi