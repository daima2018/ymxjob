CREATE TABLE if not exists `ymx.ods_amazon_fba_fulfillment_customer_returns_data` (
     affcrd_id             int     comment ''      
    ,seller_id             string  comment 'seller_id'         
    ,user_account          string  comment '店铺账号'                 
    ,sku                   string  comment '卖家sku'   
    ,asin                  string  comment 'asin'    
    ,fnsku                 string  comment 'amazon fba仓库sku'     
    ,order_id              string  comment 'amazon订单id'        
    ,return_date           string  comment '退回时间'           
    ,product_name          string  comment 'SKU名称'            
    ,quantity              int     comment '数量'     
    ,fulfillment_center_id string  comment '中心编号'                     
    ,detailed_disposition  string  comment '详细配置'                    
    ,reason                string  comment '原因'      
    ,status                string  comment '状态Reimbursed：已补偿  Unit returned to inventory退回库存'      
    ,license_plate_number  string  comment '执照号码'                    
    ,customer_comments     string  comment '客户评论'                 
    ,row_index             string  comment 'md5'         
    ,created_time          string  comment '创建时间'            
    ,updated_time          string  comment '更新时间'            
    ,ods_create_time       string           comment '导入数据时间'
) comment '亚马逊客户退货数据'
partitioned by (company_code string comment '公司代码')
row format delimited fields terminated by '\t' stored as textfile;

create table if not exists ymx_tmp.ods_amazon_fba_fulfillment_customer_returns_data like ymx.ods_amazon_fba_fulfillment_customer_returns_data;