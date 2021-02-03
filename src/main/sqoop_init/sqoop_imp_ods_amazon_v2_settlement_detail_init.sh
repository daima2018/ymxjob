#!/bin/bash
#############################################################
#源表   ec_amazon_v2_settlement_detail
#名称   亚马逊结算报告V2版本详情
#目标表  ods_amazon_v2_settlement_detail
#############################################################

source /home/ecm/ymx/conf/common-conf.sh
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
ods_tbname=ods_amazon_v2_settlement_detail
tmp_tbname=ods_amazon_v2_settlement_detail

target_dir=${HDFS_BASE_DIR}/${tmp_dbname}.db/${tmp_tbname}/company_code=${company_code}

echo "--target_dir:${target_dir}"

db_connect=`eval echo  '$'"${company_code}_db_connect"`
db_username=`eval echo '$'"${company_code}_db_username"`
db_password=`eval echo '$'"${company_code}_db_password"`




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
    ras_id                         -- '自增长ID'      
    ,seller_id                     -- '卖家销售id'      
    ,user_account                  -- '店铺账号'         
    ,site                          -- '站点' 
    ,settlement_id                 -- ''          
    ,currency                      -- '币种'     
    ,transaction_type              -- ''             
    ,order_id                      -- '订单号'     
    ,merchant_order_id             -- ''              
    ,adjustment_id                 -- ''          
    ,shipment_id                   -- ''        
    ,marketplace_name              -- ''             
    ,amount_type                   -- '费用类型'        
    ,amount_description            -- '费用描述'               
    ,amount                        -- '费用金额'          
    ,fulfillment_id                -- '订单的配送方式'           
    ,posted_date                   -- '发起日期'        
    ,posted_date_time              -- '发起时间'             
    ,order_item_code               -- ''            
    ,merchant_order_item_id        -- ''                   
    ,sku                           -- 'SKU'
    ,quantity_purchased            -- '数量'            
    ,promotion_id                  -- '折扣描述'         
    ,merchant_adjustment_item_id   -- ''                        
    ,row_key                       -- '行唯一值唯一索引'    
    ,row_index                     -- '行唯一值'      
    ,'' as report_id                     -- ''      
    ,'' as settlement_index              -- '结算数据唯一值'             
    ,created_time                  -- ''         
    ,updated_time                  -- ''         
    ,now() as ods_create_time   -- '导入数据时间'
from ec_amazon_v2_settlement_detail   
where \$CONDITIONS" 

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] sqoop execute failed!"
     exit $?
fi

echo "--开始导入数据到ods表--"

DISK_SPACE=$(hadoop fs -du -h -s ${target_dir} | awk -F ' ' '{print int($1)}')
if [ $DISK_SPACE -gt 0 ];then
    sql="insert overwrite table ymx.ods_amazon_v2_settlement_detail partition(company_code='${company_code}') 
        select 
            ras_id                                   
            ,seller_id                           
            ,user_account                           
            ,site                           
            ,settlement_id                 
            ,currency                           
            ,transaction_type              
            ,order_id                           
            ,merchant_order_id             
            ,adjustment_id                 
            ,shipment_id                   
            ,marketplace_name              
            ,amount_type                           
            ,amount_description                           
            ,amount                                  
            ,fulfillment_id                           
            ,posted_date                           
            ,posted_date_time                           
            ,order_item_code               
            ,merchant_order_item_id        
            ,sku                           
            ,quantity_purchased                        
            ,promotion_id                           
            ,merchant_adjustment_item_id   
            ,row_key                           
            ,row_index                           
            ,report_id                     
            ,settlement_index                           
            ,created_time                  
            ,updated_time                  
            ,ods_create_time
        from ymx_tmp.ods_amazon_v2_settlement_detail where company_code='${company_code}'
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