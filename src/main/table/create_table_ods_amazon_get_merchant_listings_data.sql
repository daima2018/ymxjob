CREATE TABLE if not exists  ods_amazon_get_merchant_listings_data  (
     id                            int             comment ''                              
    ,user_account                  string          comment ''                                 
    ,listing_id                    string          comment ''                               
    ,seller_sku                    string          comment ''                               
    ,product_id                    string          comment ''                               
    ,product_id_lite               string          comment ''                                    
    ,item_name                     string          comment ''                              
    ,item_description              string          comment ''                                   
    ,price                         string          comment ''                          
    ,quantity                      int             comment ''                          
    ,open_date                     string          comment ''                              
    ,image_url                     string          comment ''                              
    ,item_is_marketplace           string          comment ''                                        
    ,product_id_type               string          comment ''                                    
    ,zshop_shipping_fee            string          comment ''                                       
    ,item_note                     string          comment ''                              
    ,item_condition                string          comment ''                                   
    ,zshop_category1               string          comment ''                                    
    ,zshop_browse_path             string          comment ''                                      
    ,zshop_storefront_feature      string          comment ''                                             
    ,asin1                         string          comment ''                          
    ,asin2                         string          comment ''                          
    ,asin3                         string          comment ''                          
    ,parent_asin                   string          comment '父ASIN'                                
    ,sales_num                     int             comment '销售数量'                           
    ,last_update_time              string          comment '销售数量最后更新时间'                                     
    ,will_ship_internationally     string          comment ''                                              
    ,expedited_shipping            string          comment ''                                       
    ,zshop_boldface                string          comment ''                                   
    ,bid_for_featured_placement    string          comment ''                                               
    ,add_delete                    string          comment ''                               
    ,pending_quantity              string          comment ''                                     
    ,fulfillment_channel           string          comment ''                                        
    ,created_time                  string          comment ''                                 
    ,updated_time                  string          comment ''                                 
    ,item_status                   string          comment '在售：on_sale,停售stop_sale'                                
    ,fulfillment_type              string          comment '配送渠道，FBA，MERCHANT'                                     
    ,business_price                decimal(10,4)   comment ''                                          
    ,quantity_price_type           string          comment ''                                        
    ,quantity_lower_bound1         string          comment ''                                          
    ,quantity_price1               decimal(10,4)   comment ''                                           
    ,quantity_lower_bound2         string          comment ''                                          
    ,quantity_price2               decimal(10,4)   comment ''                                           
    ,quantity_lower_bound3         string          comment ''                                          
    ,quantity_price3               decimal(10,4)   comment ''                                           
    ,quantity_lower_bound4         string          comment ''                                          
    ,quantity_price4               decimal(10,4)   comment ''                                           
    ,quantity_lower_bound5         string          comment ''                                          
    ,merchant_shipping_group       string          comment ''                                            
    ,progressive_price_type        string          comment ''                                           
    ,progressive_lower_bound1      string          comment ''                                             
    ,progressive_price1            decimal(10,4)   comment ''                                              
    ,progressive_lower_bound2      string          comment ''                                             
    ,progressive_price2            decimal(10,4)   comment ''                                              
    ,progressive_lower_bound3      string          comment ''                                             
    ,progressive_price3            decimal(10,4)   comment ''                                              
    ,seller_id                     string          comment ''                              
    ,top_time                      string          comment '置顶时间'                             
    ,is_new                        int             comment '是否新品（1是2不是）'                        
    ,inventory_min_num             int             comment '历史库存最低数'                                   
    ,inventory_max_num             int             comment '历史库存最高数'                                   
    ,history_lowest_price          decimal(15,4)   comment '历史最低售价'                                                
    ,history_highest_price         decimal(15,4)   comment '历史最高售价'                                                 
    ,auto_inventory_id             int             comment '自动调库存id'                                   
    ,auto_price_id                 int             comment '自动调价id'                               
    ,md5_key                       string          comment 'user_accoun.asin1.seller_sku 的MD5 加密串'                            
    ,parent_md5_key                string          comment '父asin的MD5加密串'                        
    ,ods_create_time               string          comment '导入数据时间'
) comment 'listing报表原始表'
partitioned by (company_code string comment '公司代码')
row format delimited fields terminated by '\t' stored as textfile;                                                               
