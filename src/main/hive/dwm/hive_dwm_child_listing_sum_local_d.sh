#!/bin/bash
#############################################################
#名称   子asin和独立asi统计表(本地时间)
#目标表 dwm_child_listing_sum_local_d
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

start_all=$start_date" 00:00:00"
end_all=$end_date" 00:00:00"


hive -e "
set hive.exec.dynamic.partition=true;           
set hive.exec.dynamic.partition.mode=nonstrict;   
set mapred.reduce.tasks=4;
set hive.exec.parallel=true;
set mapreduce.input.fileinputformat.split.maxsize=128000000;

insert overwrite table ymx.dwm_child_listing_sum_local_d partition(company_code='${company_code}',stat_date)
select                                                                       
     c.user_account                                           -- '店铺账号'                
    ,c.site                                           --  '站点'                      
    ,c.seller_sku                                             --
    ,c.parent_asin            --在结果集可以去掉该字段
    ,c.asin                                                   --                     
    ,nvl(d.sale_amount,0) as qty                                     -- '销售数量',                 
    ,nvl(case when local_rate=1 then floor(100*d.sale_money_original)/100
              else floor(100*d.sale_money_original/c.local_rate)/100
         end,0) as sale_amount                                                      -- '销售额(本位币)',          
    ,nvl(floor(100*d.sale_money_original/c.usd_rate)/100,0) as  sale_amount_usd     -- '销售额美元',              
    ,nvl(floor(100*d.sale_money_original/c.eur_rate)/100,0) as  sale_amount_eur     -- '销售额欧元',               
    ,nvl(floor(100*d.sale_money_original/c.gbp_rate)/100,0) as  sale_amount_gbp     -- '销售额英镑',               
    ,nvl(floor(100*d.sale_money_original/c.jpy_rate)/100,0) as  sale_amount_jpy     -- '销售额日元',               
    ,nvl(d.sale_money_original,0) as sale_amount_original                           -- '销售额（站点原始金额）',   
    ,date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') as created_time         -- '创建时间',            
    ,date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') as updated_time         -- '更新时间',            
    ,nvl(d.sale_order_num,0)                                                        -- '订单数',              
    ,nvl(d.refund_amount ,0)                                                        -- '退款listing个数',       
    ,nvl(floor(100*d.refund_money)/100           ,0)                                -- '退款金额(站点币种)',      
    ,nvl(floor(100*d.refund_money/c.usd_rate)/100,0) as refund_money_usd            -- '退款金额美元',          
    ,nvl(floor(100*d.refund_money/c.eur_rate)/100,0) as refund_money_eur            -- '退款金额欧元',          
    ,nvl(floor(100*d.refund_money/c.gbp_rate)/100,0) as refund_money_gbp            -- '退款金额英镑',          
    ,nvl(floor(100*d.refund_money/c.jpy_rate)/100,0) as refund_money_jpy            -- '退款金额日元',          
    ,nvl(case when local_rate=1 then floor(100*d.refund_money)/100
              else floor(100*d.refund_money/c.local_rate)/100
         end,0) as refund_money_local                                              -- '退款金额(本位币)',                            
    ,md5(concat(c.user_account,c.asin,c.seller_sku)) as key1                       -- 'user_account+asin+seller_sku 的MD5值',           
    ,nvl(d.return_amount,0)                                                               -- '退货数',                                    
    ,c.asin_type                                                                   -- '0未确定, 子asin:1，独立产品:2, 父asin3',       
    ,nvl(d.ad_sale_amount,0) as ad_qty                                                    -- '广告销售数量',                             
    ,nvl(case when local_rate=1 then floor(100*d.ad_sale_money_original)/100
              else floor(100*d.ad_sale_money_original/c.local_rate)/100
         end,0) as ad_sale_amount                                                         -- '广告销售额(本位币)',               
    ,nvl(floor(100*d.ad_sale_money_original/c.usd_rate)/100,0) as ad_sale_amount_usd      -- '广告销售额美元',                   
    ,nvl(floor(100*d.ad_sale_money_original/c.eur_rate)/100,0) as ad_sale_amount_eur      -- '广告销售额欧元',                   
    ,nvl(floor(100*d.ad_sale_money_original/c.gbp_rate)/100,0) as ad_sale_amount_gbp      -- '广告销售额英镑',                   
    ,nvl(floor(100*d.ad_sale_money_original/c.jpy_rate)/100,0) as ad_sale_amount_jpy      -- '广告销售额日元',                   
    ,nvl(d.ad_sale_order_num,0)                                as ad_sale_order_num       -- '广告订单数',                  
    ,nvl(d.ad_sale_money_original,0)                           as ad_sale_amount_original -- '广告销售额（站点原始金额）',           
    ,nvl(d.cost,0)                                             as cost                    -- '广告花费(站点币种)',          
    ,nvl(case when local_rate=1 then floor(100*d.cost)/100
              else floor(100*d.cost/c.local_rate)/100
         end,0) as cost_local                                              -- '广告花费(本位币)',       
    ,nvl(floor(100*d.cost/c.usd_rate)/100,0) as cost_usd                                            
    ,nvl(floor(100*d.cost/c.eur_rate)/100,0) as cost_eur                                           
    ,nvl(floor(100*d.cost/c.gbp_rate)/100,0) as cost_gbp                                           
    ,nvl(floor(100*d.cost/c.jpy_rate)/100,0) as cost_jpy                                           
    ,nvl(d.clicks     ,0) as clicks                           -- '广告访问次数,也即广告点击数'           
    ,nvl(d.impressions,0) as impressions                      -- 广告曝光量                              
    ,nvl(d.sessions             ,0) as sessions                 -- 访客次数                           
    ,nvl(d.page_views           ,0) as page_views               --浏览次数                              
    ,nvl(d.buy_box_percentage   ,0) as buy_box_percentage       --  buy_box_percentage           
    ,nvl(d.session_percentage   ,0) as session_percentage       --买家访问次数比率             
    ,nvl(d.page_views_percentage,0) as page_views_percentage    --   浏览次数比率        
    ,c.currency_date  as stat_date
from (select * from ymx.dwd_listing_currency_rate_d 
     where company_code='${company_code}' and currency_date>='${start_date}' and currency_date<'${end_date}'
) c 
inner join (
     select
           stat_date
          ,user_account 
          ,asin         
          ,seller_sku   
          ,sum(sale_order_num        ) as sale_order_num                          
          ,sum(sale_amount           ) as sale_amount                        
          ,sum(sale_money_original   ) as sale_money_original                        
          ,sum(ad_sale_order_num     ) as ad_sale_order_num                        
          ,sum(ad_sale_amount        ) as ad_sale_amount                        
          ,sum(ad_sale_money_original) as ad_sale_money_original                        
          ,sum(impressions           ) as impressions                        
          ,sum(clicks                ) as clicks                        
          ,sum(cost                  ) as cost                        
          ,sum(return_amount         ) as return_amount                        
          ,sum(refund_amount         ) as refund_amount                        
          ,sum(refund_money          ) as refund_money                        
          ,sum(sessions              ) as sessions                                         
          ,sum(page_views            ) as page_views                        
          ,sum(buy_box_percentage    ) as buy_box_percentage                        
          ,sum(session_percentage    ) as session_percentage                        
          ,sum(page_views_percentage ) as page_views_percentage                        
     from (SELECT
                date_format(aoo.purchase_date_local,'yyyy-MM-dd') as stat_date
               ,aod.user_account  --'店铺账号',
               ,aod.asin          -- '商品的亚马逊商品编码',
               ,aod.seller_sku    --'销售SKU',
               ,COUNT(DISTINCT aod.amazon_order_id) AS sale_order_num  --订单数
               ,nvl(SUM(aod.quantity_ordered), 0) AS sale_amount
               ,nvl(SUM(aod.item_sale_amount), 0) AS sale_money_original
               ,0 as ad_sale_order_num
               ,0 as ad_sale_amount
               ,0 as ad_sale_money_original
               ,0 as impressions
               ,0 as clicks
               ,0 as cost
               ,0 as return_amount
               ,0 as refund_amount
               ,0 as refund_money
               ,0 as sessions                 
               ,0 as page_views
               ,0 as buy_box_percentage
               ,0 as session_percentage
               ,0 as page_views_percentage
          FROM  ymx.ods_amazon_order_original aoo
                    JOIN ymx.ods_amazon_order_detail aod
                         ON aoo.company_code='${company_code}'
                              and aod.company_code='${company_code}'                
                              and aoo.aoo_id = aod.aoo_id
          where date_format(aoo.purchase_date_local,'yyyy-MM-dd') >= '${start_date}'
            and date_format(aoo.purchase_date_local,'yyyy-MM-dd') < '${end_date}'
            and aoo.order_status != 'Canceled'
          group by   aod.user_account
                 ,aod.asin
                 ,aod.seller_sku
                 ,date_format(aoo.purchase_date_local,'yyyy-MM-dd')
          union all
          SELECT
                generated_date as stat_date
               ,user_account
               ,asin
               ,sku as seller_sku
               ,0 as sale_order_num 
               ,0 as sale_amount
               ,0 as sale_money_original
               ,nvl(SUM(conversions7d_same_sku),0) AS ad_sale_order_num
               ,nvl(SUM(units_ordered7d_same_sku),0) AS ad_sale_amount
               ,nvl(SUM(sales7d_same_sku), 0) AS ad_sale_money_original
               ,nvl(SUM(impressions), 0) AS impressions
               ,nvl(SUM(clicks), 0) AS clicks
               ,nvl(SUM(cost), 0) AS cost
               ,0 as return_amount
               ,0 as refund_amount
               ,0 as refund_money
               ,0 as sessions                 
               ,0 as page_views
               ,0 as buy_box_percentage
               ,0 as session_percentage
               ,0 as page_views_percentage
          FROM ymx.ods_product_ad_products_report_daily
          WHERE company_code='${company_code}'
               and generated_date>='${start_date}'
              and generated_date<'${end_date}'
          group by user_account
                 ,asin
                 ,sku   
                 ,generated_date 
          union all
          SELECT
                substr(return_date,1,10) as stat_date
               ,user_account
               ,asin
               ,sku as seller_sku
               ,0 as sale_order_num  
               ,0 as sale_amount
               ,0 as sale_money_original
               ,0 as ad_sale_order_num
               ,0 as ad_sale_amount
               ,0 as ad_sale_money_original
               ,0 as impressions
               ,0 as clicks
               ,0 as cost
               ,nvl(sum(quantity),0) as return_amount
               ,0 as refund_amount
               ,0 as refund_money
               ,0 as sessions                 
               ,0 as page_views
               ,0 as buy_box_percentage
               ,0 as session_percentage
               ,0 as page_views_percentage
          FROM ymx.ods_amazon_fba_fulfillment_customer_returns_data
          WHERE  company_code='${company_code}'
               and return_date>=to_utc_timestamp('${start_all}','GMT+8')  
               AND return_date< to_utc_timestamp('${end_all}','GMT+8')
          group by user_account
                 ,asin
                 ,sku   
                 ,substr(return_date,1,10) 
          union all
          select
                t1.stat_date
               ,t1.user_account
               ,t2.asin
               ,t1.seller_sku
               ,0 as sale_order_num  
               ,0 as sale_amount
               ,0 as sale_money_original
               ,0 as ad_sale_order_num
               ,0 as ad_sale_amount
               ,0 as ad_sale_money_original
               ,0 as impressions
               ,0 as clicks
               ,0 as cost
               ,0 as return_amount
               ,t1.refund_amount
               ,t1.refund_money
               ,0 as sessions                 
               ,0 as page_views
               ,0 as buy_box_percentage
               ,0 as session_percentage
               ,0 as page_views_percentage
          from (SELECT
                     substr(posted_date_time,1,10) as stat_date
                    ,user_account
                    ,sku as seller_sku
                    ,nvl(SUM(quantity_purchased),0) AS refund_amount
                    ,nvl(SUM(amount),0) AS refund_money
               from ymx.ods_amazon_v2_settlement_detail
               WHERE company_code='${company_code}'
                    and transaction_type='Refund'
                 AND posted_date_time>=to_utc_timestamp('${start_all}','GMT+8')
                 AND posted_date_time< to_utc_timestamp('${end_all}','GMT+8')
               group by user_account
                      ,sku
                      ,substr(posted_date_time,1,10)
          ) t1
          inner join ymx.dwd_listing_d t2
               on t2.company_code='${company_code}'
                    and t1.user_account=t2.user_account
                    and t1.seller_sku=t2.seller_sku 
          union all
          select
                t1.stat_date
               ,t1.user_account
               ,t1.asin
               ,t2.seller_sku
               ,0 as sale_order_num  
               ,0 as sale_amount
               ,0 as sale_money_original
               ,0 as ad_sale_order_num
               ,0 as ad_sale_amount
               ,0 as ad_sale_money_original
               ,0 as impressions
               ,0 as clicks
               ,0 as cost
               ,0 as return_amount
               ,0 as refund_amount
               ,0 as refund_money
               ,t1.sessions                 --取一条即可
               ,t1.page_views
               ,t1.buy_box_percentage
               ,t1.session_percentage
               ,t1.page_views_percentage
          from(SELECT
                     generate_date as stat_date
                    ,user_account
                    ,child_asin as asin
                    ,nvl(max(sessions),0) as sessions                 --取一条即可
                    ,nvl(max(page_views),0) as page_views
                    ,nvl(max(buy_box_percentage),0) as buy_box_percentage
                    ,nvl(max(session_percentage),0) as session_percentage
                    ,nvl(max(page_views_percentage),0) as page_views_percentage
               from ymx.ods_amazon_business_report_by_child
               WHERE company_code='${company_code}'
                    and generate_date>='${start_date}'
                   and generate_date<'${end_date}'
               group by user_account
                      ,child_asin
                      ,generate_date
          ) t1
          inner join ymx.dwd_listing_d t2
               on t2.company_code='${company_code}'
                    and t1.user_account=t2.user_account
                    and t1.asin = t2.asin 
     ) t
     group by stat_date
              ,user_account 
              ,asin         
              ,seller_sku   
) d
on  c.currency_date=d.stat_date
    and c.user_account = d.user_account
    and c.asin = d.asin        
    and c.seller_sku = d.seller_sku  
"

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] hive execute failed!"
     exit $?
fi