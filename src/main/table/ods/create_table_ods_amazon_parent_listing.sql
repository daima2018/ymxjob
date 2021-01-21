CREATE TABLE if not exists ymx.ods_amazon_parent_listing  (
     id           int    comment '主键'
    ,asin         string comment '父asin或者独立asin' 
    ,user_account string comment '店铺账号'        
    ,seller_sku   string comment 'seller_sku'      
    ,created_time string comment ''  
    ,updated_time string comment ''  
    ,is_parent    int    comment '1是父asin0是独立asin' 
    ,top_time     string comment '置顶时间' 
    ,image_url    string comment '图片地址' 
    ,item_name    string comment 'sku名称'  
    ,md5_key      string comment 'user_account.asin.seller_sku的MD5值'    
    ,ods_create_time string comment '导入数据时间'
) COMMENT '父listing数据' 
partitioned by (company_code string comment '公司代码')
row format delimited fields terminated by '\t' stored as textfile;

create table if not exists ymx_tmp.ods_amazon_parent_listing like ymx.ods_amazon_parent_listing;