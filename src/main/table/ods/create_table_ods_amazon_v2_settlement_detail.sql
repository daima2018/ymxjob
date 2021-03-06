CREATE TABLE if not exists `ymx.ods_amazon_v2_settlement_detail` (
     ras_id                        int           comment '自增长ID'       
    ,seller_id                     string        comment '卖家销售id'      
    ,user_account                  string        comment '店铺账号'         
    ,site                          string        comment '站点' 
    ,settlement_id                 string        comment ''          
    ,currency                      string        comment '币种'     
    ,transaction_type              string        comment ''             
    ,order_id                      string        comment '订单号'     
    ,merchant_order_id             string        comment ''              
    ,adjustment_id                 string        comment ''          
    ,shipment_id                   string        comment ''        
    ,marketplace_name              string        comment ''             
    ,amount_type                   string        comment '费用类型'        
    ,amount_description            string        comment '费用描述'               
    ,amount                        decimal(10,3) comment '费用金额'          
    ,fulfillment_id                string        comment '订单的配送方式'           
    ,posted_date                   string        comment '发起日期'        
    ,posted_date_time              string        comment '发起时间'             
    ,order_item_code               string        comment ''            
    ,merchant_order_item_id        string        comment ''                   
    ,sku                           string        comment 'SKU'
    ,quantity_purchased            int           comment '数量'            
    ,promotion_id                  string        comment '折扣描述'         
    ,merchant_adjustment_item_id   string        comment ''                        
    ,row_key                       string        comment '行唯一值唯一索引'    
    ,row_index                     string        comment '行唯一值'      
    ,report_id                     bigint        comment ''      
    ,settlement_index              string        comment '结算数据唯一值'             
    ,created_time                  string        comment ''         
    ,updated_time                  string        comment ''         
    ,ods_create_time           string           comment '导入数据时间'
) comment '亚马逊结算报告V2版本详情'
partitioned by (company_code string comment '公司代码')
row format delimited fields terminated by '\t' stored as textfile;

create table if not exists ymx_tmp.ods_amazon_v2_settlement_detail like ymx.ods_amazon_v2_settlement_detail;
