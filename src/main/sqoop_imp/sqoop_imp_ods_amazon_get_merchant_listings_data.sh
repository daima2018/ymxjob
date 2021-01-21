#!/bin/bash
#############################################################
#源表   ec_amazon_get_merchant_listings_data
#名称   listing报表原始表
#目标表 ods_amazon_get_merchant_listings_data
#############################################################

source /home/ecm/ymx/conf/sqoop-job-conf.sh

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

ods_dbname=ymx
tmp_dbname=ymx_tmp
ods_tbname=ods_amazon_get_merchant_listings_data
tmp_tbname=ods_amazon_get_merchant_listings_data

target_dir=${HDFS_BASE_DIR}/${tmp_dbname}.db/${tmp_tbname}/company_code=${company_code}

echo "--target_dir:${target_dir}"

db_connect=`eval echo  '$'"${company_code}_db_connect"`
db_username=`eval echo '$'"${company_code}_db_username"`
db_password=`eval echo '$'"${company_code}_db_password"`

echo "--connect:${db_connect}"
echo "--db_username:${db_username}"
echo "--db_password:${db_password}"


sqoop import \
--connect ${db_connect} \
--username ${db_username} \
--password ${db_password} \
--hive-overwrite \
--target-dir $target_dir \
--num-mappers 1        \
--delete-target-dir \
--hive-database $tmp_dbname \
--hive-drop-import-delims \
--null-string '\\N'  \
--null-non-string '\\N' \
--hive-table $tmp_tbname     \
--fields-terminated-by '\t' \
--lines-terminated-by '\n'   \
--query "
select
     id                            -- ''                                  
    ,user_account                  -- ''                                 
    ,listing_id                    -- ''                               
    ,seller_sku                    -- ''                               
    ,product_id                    -- ''                               
    ,product_id_lite               -- ''                                    
    ,item_name                     -- ''                              
    ,item_description              -- ''                                   
    ,price                         -- ''                          
    ,quantity                      -- ''                          
    ,open_date                     -- ''                              
    ,image_url                     -- ''                              
    ,item_is_marketplace           -- ''                                        
    ,product_id_type               -- ''                                    
    ,zshop_shipping_fee            -- ''                                       
    ,item_note                     -- ''                              
    ,item_condition                -- ''                                   
    ,zshop_category1               -- ''                                    
    ,zshop_browse_path             -- ''                                      
    ,zshop_storefront_feature      -- ''                                             
    ,asin1                         -- ''                          
    ,asin2                         -- ''                          
    ,asin3                         -- ''                          
    ,parent_asin                   -- '父ASIN'                                
    ,sales_num                     -- '销售数量'                           
    ,last_update_time              -- '销售数量最后更新时间'                                     
    ,will_ship_internationally     -- ''                                              
    ,expedited_shipping            -- ''                                       
    ,zshop_boldface                -- ''                                   
    ,bid_for_featured_placement    -- ''                                               
    ,add_delete                    -- ''                               
    ,pending_quantity              -- ''                                     
    ,fulfillment_channel           -- ''                                        
    ,created_time                  -- ''                                 
    ,updated_time                  -- ''                                 
    ,item_status                   -- '在售：on_sale,停售stop_sale'                                
    ,fulfillment_type              -- '配送渠道，FBA，MERCHANT'                                     
    ,business_price                -- ''                                          
    ,quantity_price_type           -- ''                                        
    ,quantity_lower_bound1         -- ''                                          
    ,quantity_price1               -- ''                                           
    ,quantity_lower_bound2         -- ''                                          
    ,quantity_price2               -- ''                                           
    ,quantity_lower_bound3         -- ''                                          
    ,quantity_price3               -- ''                                           
    ,quantity_lower_bound4         -- ''                                          
    ,quantity_price4               -- ''                                           
    ,quantity_lower_bound5         -- ''                                          
    ,merchant_shipping_group       -- ''                                            
    ,progressive_price_type        -- ''                                           
    ,progressive_lower_bound1      -- ''                                             
    ,progressive_price1            -- ''                                              
    ,progressive_lower_bound2      -- ''                                             
    ,progressive_price2            -- ''                                              
    ,progressive_lower_bound3      -- ''                                             
    ,progressive_price3            -- ''                                              
    ,seller_id                     -- ''                              
    ,top_time                      -- '置顶时间'                             
    ,is_new                        -- '是否新品（1是2不是）'                        
    ,inventory_min_num             -- '历史库存最低数'                                   
    ,inventory_max_num             -- '历史库存最高数'                                   
    ,history_lowest_price          -- '历史最低售价'                                                
    ,history_highest_price         -- '历史最高售价'                                                 
    ,auto_inventory_id             -- '自动调库存id'                                   
    ,auto_price_id                 -- '自动调价id'                               
    ,md5_key                       -- 'user_accoun.asin1.seller_sku 的MD5 加密串'                            
    ,parent_md5_key                -- '父asin的MD5加密串'   
    ,now() as ods_create_time   -- '导入数据时间'
from ec_amazon_get_merchant_listings_data            
where ('${start_date}'<updated_time and updated_time<'${end_date}')
and \$CONDITIONS" 

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] sqoop execute failed!"
     exit $?
fi

echo "--开始导入数据到ods表--"

DISK_SPACE=$(hadoop fs -du -h -s ${target_dir} | awk -F ' ' '{print int($1)}')
if [ $DISK_SPACE -gt 0 ];then
    sql="insert overwrite table ymx.ods_amazon_get_merchant_listings_data partition(company_code='${company_code}') 
        select 
            id                                                          
            ,user_account                                              
            ,listing_id                                              
            ,seller_sku                                              
            ,product_id                                              
            ,product_id_lite                                              
            ,item_name                                              
            ,item_description                                            
            ,price                                              
            ,quantity                                           
            ,open_date                                              
            ,image_url                                              
            ,item_is_marketplace                                              
            ,product_id_type                                              
            ,zshop_shipping_fee                                              
            ,item_note                                              
            ,item_condition                                              
            ,zshop_category1                                              
            ,zshop_browse_path                                              
            ,zshop_storefront_feature                                              
            ,asin1                                              
            ,asin2                                              
            ,asin3                                              
            ,parent_asin                                                   
            ,sales_num                                               
            ,last_update_time                                                   
            ,will_ship_internationally                                              
            ,expedited_shipping                                              
            ,zshop_boldface                                              
            ,bid_for_featured_placement                                              
            ,add_delete                                              
            ,pending_quantity                                              
            ,fulfillment_channel                                              
            ,created_time                                              
            ,updated_time                                              
            ,item_status                                           
            ,fulfillment_type                                            
            ,business_price                                                     
            ,quantity_price_type                                              
            ,quantity_lower_bound1                                              
            ,quantity_price1                                                     
            ,quantity_lower_bound2                                              
            ,quantity_price2                                                     
            ,quantity_lower_bound3                                              
            ,quantity_price3                                                     
            ,quantity_lower_bound4                                              
            ,quantity_price4                                                     
            ,quantity_lower_bound5                                              
            ,merchant_shipping_group                                              
            ,progressive_price_type                                              
            ,progressive_lower_bound1                                              
            ,progressive_price1                                                     
            ,progressive_lower_bound2                                              
            ,progressive_price2                                                     
            ,progressive_lower_bound3                                              
            ,progressive_price3                                                     
            ,seller_id                                              
            ,top_time                                                  
            ,is_new                       
            ,inventory_min_num                   
            ,inventory_max_num                   
            ,history_lowest_price                            
            ,history_highest_price                            
            ,auto_inventory_id                   
            ,auto_price_id                         
            ,md5_key                                                  
            ,parent_md5_key                
            ,ods_create_time         
        from(select 
                t.*
                ,row_number() over(partition by id order by updated_time desc) rn
            from(select 
                    id,user_account,listing_id,seller_sku,product_id,product_id_lite,item_name,item_description,price,quantity,open_date,image_url,
                    item_is_marketplace,product_id_type,zshop_shipping_fee,item_note,item_condition,zshop_category1,zshop_browse_path,zshop_storefront_feature,
                    asin1,asin2,asin3,parent_asin,sales_num,last_update_time,will_ship_internationally,expedited_shipping,zshop_boldface,
                    bid_for_featured_placement,add_delete,pending_quantity,fulfillment_channel,created_time,updated_time,item_status,fulfillment_type,
                    business_price,quantity_price_type,quantity_lower_bound1,quantity_price1,quantity_lower_bound2,quantity_price2,quantity_lower_bound3,
                    quantity_price3,quantity_lower_bound4,quantity_price4,quantity_lower_bound5,merchant_shipping_group,progressive_price_type,
                    progressive_lower_bound1,progressive_price1,progressive_lower_bound2,progressive_price2,progressive_lower_bound3,progressive_price3,
                    seller_id,top_time,is_new,inventory_min_num,inventory_max_num,history_lowest_price,history_highest_price,auto_inventory_id,auto_price_id,
                    md5_key,parent_md5_key,ods_create_time
                from ymx_tmp.ods_amazon_get_merchant_listings_data where company_code='${company_code}'
                union all 
                select 
                    id,user_account,listing_id,seller_sku,product_id,product_id_lite,item_name,item_description,price,quantity,open_date,image_url,
                    item_is_marketplace,product_id_type,zshop_shipping_fee,item_note,item_condition,zshop_category1,zshop_browse_path,zshop_storefront_feature,
                    asin1,asin2,asin3,parent_asin,sales_num,last_update_time,will_ship_internationally,expedited_shipping,zshop_boldface,
                    bid_for_featured_placement,add_delete,pending_quantity,fulfillment_channel,created_time,updated_time,item_status,fulfillment_type,
                    business_price,quantity_price_type,quantity_lower_bound1,quantity_price1,quantity_lower_bound2,quantity_price2,quantity_lower_bound3,
                    quantity_price3,quantity_lower_bound4,quantity_price4,quantity_lower_bound5,merchant_shipping_group,progressive_price_type,
                    progressive_lower_bound1,progressive_price1,progressive_lower_bound2,progressive_price2,progressive_lower_bound3,progressive_price3,
                    seller_id,top_time,is_new,inventory_min_num,inventory_max_num,history_lowest_price,history_highest_price,auto_inventory_id,auto_price_id,
                    md5_key,parent_md5_key,ods_create_time
                from ymx.ods_amazon_get_merchant_listings_data where company_code='${company_code}'
            ) t 
        ) tt
        where rn=1     
        "
    echo "--$DISK_SPACE 文件目录已经存在，执行数据写入操作$sql"
    hive -e "
        set hive.exec.parallel=true;    
        ${sql}"
else
        echo '未获取到数据！！！'
fi

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] hive execute failed!"
     exit $?
fi                                 
