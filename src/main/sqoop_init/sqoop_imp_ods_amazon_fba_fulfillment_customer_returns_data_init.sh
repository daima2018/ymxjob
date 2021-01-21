#!/bin/bash
#############################################################
#源表   ec_amazon_fba_fulfillment_customer_returns_data
#名称   亚马逊客户退货数据
#目标表 ods_amazon_fba_fulfillment_customer_returns_data
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
ods_tbname=ods_amazon_fba_fulfillment_customer_returns_data
tmp_tbname=ods_amazon_fba_fulfillment_customer_returns_data

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
     affcrd_id             -- ''      
    ,seller_id             -- 'seller_id'         
    ,user_account          -- '店铺账号'            
    ,sku                   -- '卖家sku'   
    ,asin                  -- 'asin'    
    ,fnsku                 -- 'amazon fba仓库sku'     
    ,order_id              -- 'amazon订单id'        
    ,return_date           -- '退回时间'           
    ,product_name          -- 'SKU名称'            
    ,quantity              -- '数量'     
    ,fulfillment_center_id -- '中心编号'                     
    ,detailed_disposition  -- '详细配置'                    
    ,reason                -- '原因'      
    ,status                -- '状态Reimbursed：已补偿  Unit returned to inventory退回库存'      
    ,license_plate_number  -- '执照号码'                    
    ,customer_comments     -- '客户评论'                 
    ,row_index             -- 'md5'         
    ,created_time          -- '创建时间'            
    ,updated_time          -- '更新时间'            
    ,now() as ods_create_time   -- '导入数据时间'
from ec_amazon_fba_fulfillment_customer_returns_data       
where  \$CONDITIONS" 

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] sqoop execute failed!"
     exit $?
fi

echo "--开始导入数据到ods表--"

DISK_SPACE=$(hadoop fs -du -h -s ${target_dir} | awk -F ' ' '{print int($1)}')
if [ $DISK_SPACE -gt 0 ];then
    sql="insert overwrite table ymx.ods_amazon_fba_fulfillment_customer_returns_data partition(company_code='${company_code}') 
        select 
            affcrd_id              
            ,seller_id                      
            ,user_account                     
            ,sku                      
            ,asin                      
            ,fnsku                      
            ,order_id                      
            ,return_date                      
            ,product_name                      
            ,quantity                   
            ,fulfillment_center_id                      
            ,detailed_disposition                      
            ,reason                      
            ,status                      
            ,license_plate_number                      
            ,customer_comments                      
            ,row_index                      
            ,created_time                      
            ,updated_time                      
            ,ods_create_time
        from ymx_tmp.ods_amazon_fba_fulfillment_customer_returns_data where company_code='${company_code}'
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