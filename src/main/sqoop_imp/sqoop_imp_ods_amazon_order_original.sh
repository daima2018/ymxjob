#!/bin/bash
#############################################################
#源表   ec_amazon_order_original
#名称   amazon原始订单表
#目标表 ods_amazon_order_original
#############################################################

source /home/ecm/ymx/conf/sqoop-job-conf.sh
source /home/ecm/ymx/ymxjob/src/main/common/functions.sh

#获取脚本参数
opts=$@

#解析脚本参数
start_date=`getparam start_date "$opts"`
end_date=`getparam end_date "$opts"`
company_code=`getparam company_code "$opts"`

start_date=`getdate "$start_date"`
end_date=`getdate "$end_date"`

ods_dbname=ymx
tmp_dbname=ymx_tmp
ods_tbname=ods_amazon_order_original
tmp_tbname=ods_amazon_order_original

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
     aoo_id                          -- '序列ID'                               
    ,amazon_order_id                 -- '亚马逊定义的订单编码，格式3-7-7'                             
    ,seller_order_id                 -- '卖家所定义的订单编码'                             
    ,site                            -- '站点信息'                        
    ,user_account                    -- 'amazon账户'                          
    ,purchase_date_site              -- '订单创建时间 site 站点时间'                                
    ,purchase_date_local             -- '订单创建时间 北京时间'                                 
    ,purchase_date                   -- '订单创建时间 UTC时间'                           
    ,last_update_date                -- '订单最后更新日期'                              
    ,order_status                    -- '当前的订单状态'                          
    ,fulfillment_channel             -- '订单的配送方式：亚马逊配送 (AFN) 或卖家自行配送 (MFN)。'                                 
    ,sales_channel                   -- '销售渠道'                           
    ,order_channel                   -- '订单渠道'                           
    ,ship_service_level              -- '订单的发货服务水平.针对中国地区卖家自行配送（MFN）有效值为：Std CN D2D：快递，Std CN Postal：平邮，Exp CN EMS：EMS'                                
    ,order_type                      -- '订单类型(StandardOrder - 包含当前有库存商品的订单。)'                        
    ,currency_code                   -- '币种'                           
    ,amount                          -- '接口返回原始金额'                           
    ,sale_amount                     -- '销售额，自己计算的'                                
    ,payment_method                  -- '订单的付款方式。有效值为： COD、CVS 和 Other。'                            
    ,marketplace_id                  -- '商城编码'                            
    ,buyer_email                     -- '买家邮箱'                         
    ,buyer_name                      -- '买家姓名'                        
    ,earliest_ship_date              -- '订单配送的最早日期，仅当 OrderType = Preorder 时才返回'                                
    ,latest_ship_date                -- '订单配送的最晚日期，仅当 OrderType = Preorder 时才返回'                              
    ,shipment_service_level_category -- '运费服务等级分类'                                             
    ,shipped_amazon_tfm              -- '指明订单配送方是否是亚马逊配送服务 (Amazon TFM，仅适用于中国地区。)。'                                
    ,tfm_shipment_status             -- '亚马逊 TFM 订单的状态，PendingPickUp'                                 
    ,cba_displayable_shipping_label  -- '卖家自定义的配送方式，属于 Checkout By Amazon (CBA) 所支持的四种标准配送设置中的一种.(仅适用于美国 (US)、英国 (UK) 和德国 (DE) 的卖家)'                                            
    ,number_items_shipped            -- '已配送商品数量'                               
    ,number_items_unshipped          -- '未配送商品数量'                                 
    ,shipping_address_name           -- '配送地址-姓名'                                   
    ,shipping_address_phone          -- '配送地址-电话'                                    
    ,shipping_address_country_code   -- '配送地址-国家二字码'                                           
    ,shipping_address_state          -- '配送地址-州/省'                                    
    ,shipping_address_district       -- '配送地址-区'                                       
    ,shipping_address_county         -- '配送地址-县'                                     
    ,shipping_address_city           -- '配送地址-城市'                                   
    ,shipping_address_postal_code    -- '配送地址-邮政编码'                                          
    ,shipping_address_address1       -- '配送地址-地址1'                                       
    ,shipping_address_address2       -- '配送地址-地址2'                                       
    ,shipping_address_address3       -- '配送地址-地址3'                                       
    ,shipping_address_type           -- '配置地址类型'                                   
    ,is_loaded                       -- '订单信息是否下载完毕。0:下载订单未下载items，1:订单、items信息都已下载，2:已生成订单，3:存在相同的单号'                    
    ,purchase_order_number           -- '采购单号'                                   
    ,is_business_order               -- '是否为B2B订单'                               
    ,is_prime                        -- ''                      
    ,earliest_delivery_date          -- '订单预计妥投最早时间 ，仅当 OrderType = Preorder 时才返回'                                    
    ,latest_delivery_date            -- '订单预计妥投最晚时间，仅当 OrderType = Preorder 时才返回'                                  
    ,is_sold_by_ab                   -- 'IsSoldByAB'                           
    ,created_time                    -- '创建时间'                          
    ,updated_time                    -- '系统最后更新时间'                     
    ,now() as ods_create_time   -- '导入数据时间'
from ec_amazon_order_original            
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
    sql="insert overwrite table ymx.ods_amazon_order_original partition(company_code='${company_code}') 
        select 
            aoo_id                            
            ,amazon_order_id                  
            ,seller_order_id                  
            ,site                              
            ,user_account                     
            ,purchase_date_site               
            ,purchase_date_local              
            ,purchase_date                    
            ,last_update_date                 
            ,order_status                     
            ,fulfillment_channel                                               
            ,sales_channel                    
            ,order_channel                    
            ,ship_service_level                                             
            ,order_type                                             
            ,currency_code                    
            ,amount                                                    
            ,sale_amount                                                     
            ,payment_method                   
            ,marketplace_id                   
            ,buyer_email                      
            ,buyer_name                       
            ,earliest_ship_date                                            
            ,latest_ship_date                                              
            ,shipment_service_level_category  
            ,shipped_amazon_tfm                                              
            ,tfm_shipment_status                                            
            ,cba_displayable_shipping_label                                              
            ,number_items_shipped             
            ,number_items_unshipped           
            ,shipping_address_name            
            ,shipping_address_phone           
            ,shipping_address_country_code    
            ,shipping_address_state           
            ,shipping_address_district        
            ,shipping_address_county          
            ,shipping_address_city            
            ,shipping_address_postal_code     
            ,shipping_address_address1        
            ,shipping_address_address2        
            ,shipping_address_address3        
            ,shipping_address_type            
            ,is_loaded                                          
            ,purchase_order_number            
            ,is_business_order                
            ,is_prime          
            ,earliest_delivery_date                                                  
            ,latest_delivery_date                                   
            ,is_sold_by_ab                    
            ,created_time                     
            ,updated_time                     
            ,ods_create_time
        from (
            select 
                t.*
                ,row_number() over(partition by aoo_id order by updated_time desc) rn
            from(select 
                    aoo_id,amazon_order_id,seller_order_id,site,user_account,purchase_date_site,purchase_date_local,purchase_date,
                    last_update_date,order_status,fulfillment_channel,sales_channel,order_channel,ship_service_level,order_type,currency_code,
                    amount,sale_amount,payment_method,marketplace_id,buyer_email,buyer_name,earliest_ship_date,latest_ship_date,
                    shipment_service_level_category,shipped_amazon_tfm,tfm_shipment_status,cba_displayable_shipping_label,number_items_shipped,
                    number_items_unshipped,shipping_address_name,shipping_address_phone,shipping_address_country_code,shipping_address_state,
                    shipping_address_district,shipping_address_county,shipping_address_city,shipping_address_postal_code,shipping_address_address1,
                    shipping_address_address2,shipping_address_address3,shipping_address_type,is_loaded,purchase_order_number,is_business_order,
                    is_prime,earliest_delivery_date,latest_delivery_date,is_sold_by_ab,created_time,updated_time,ods_create_time
                from ymx_tmp.ods_amazon_order_original where company_code='${company_code}'
                union all 
                select 
                    aoo_id,amazon_order_id,seller_order_id,site,user_account,purchase_date_site,purchase_date_local,purchase_date,
                    last_update_date,order_status,fulfillment_channel,sales_channel,order_channel,ship_service_level,order_type,currency_code,
                    amount,sale_amount,payment_method,marketplace_id,buyer_email,buyer_name,earliest_ship_date,latest_ship_date,
                    shipment_service_level_category,shipped_amazon_tfm,tfm_shipment_status,cba_displayable_shipping_label,number_items_shipped,
                    number_items_unshipped,shipping_address_name,shipping_address_phone,shipping_address_country_code,shipping_address_state,
                    shipping_address_district,shipping_address_county,shipping_address_city,shipping_address_postal_code,shipping_address_address1,
                    shipping_address_address2,shipping_address_address3,shipping_address_type,is_loaded,purchase_order_number,is_business_order,
                    is_prime,earliest_delivery_date,latest_delivery_date,is_sold_by_ab,created_time,updated_time,ods_create_time
                from ymx.ods_amazon_order_original where company_code='${company_code}'
            ) t 
        ) tt
        where rn=1    
        "
    echo "--$DISK_SPACE 文件目录已经存在，执行数据写入操作$sql"
    hive -e "${sql}"
else
        echo '未获取到数据！！！'
fi

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] hive execute failed!"
     exit $?
fi