CREATE TABLE if not exists `dwd_listing_d` (
     site           string comment '站点'      
    ,user_account   string comment '店铺账号'              
    ,parent_asin    string comment '父asin和独立asin'             
    ,asin           string comment '子asin和独立asin'      
    ,seller_sku     string comment '卖家sku'            
    ,asin_type      string comment 'asin类型，1子ASIN 2独立'           
    ,currency_site  string comment '站点币种'               
    ,currency_local string comment '本位币'                 
) comment '子asin和独立asin表'
partitioned by (company_code string comment '公司代码')
row format delimited fields terminated by '\t' stored as orc;
