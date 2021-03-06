CREATE TABLE if not exists `ymx.dwd_listing_currency_rate_d` (
     site           string comment '站点'      
    ,user_account   string comment '店铺账号'              
    ,parent_asin    string comment '父asin和独立asin'             
    ,asin           string comment '子asin和独立asin'      
    ,seller_sku     string comment '卖家sku'            
    ,asin_type      string comment 'asin类型，1子ASIN 2独立'           
    ,currency_site  string comment '站点币种'               
    ,currency_local string comment '本位币'        
    ,local_rate     string comment '本位币汇率'                                                
    ,usd_rate       string comment '美元汇率'     
    ,eur_rate       string comment '欧元汇率'     
    ,gbp_rate       string comment '英镑汇率'     
    ,jpy_rate       string comment '日元汇率'      
) comment 'listing汇率表'
partitioned by (company_code string comment '公司代码',currency_date string comment '汇率日期')
row format delimited fields terminated by '\t' stored as orc;
