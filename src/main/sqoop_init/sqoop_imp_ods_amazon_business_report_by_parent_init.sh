#!/bin/bash
#############################################################
#源表   ec_amazon_business_report_by_parent
#名称   BusinessReport 流量 报告 父asin维度
#目标表 ods_amazon_business_report_by_parent
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
ods_tbname=ods_amazon_business_report_by_parent
tmp_tbname=ods_amazon_business_report_by_parent

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
     id                          -- ''              
    ,user_account                -- '店铺账号'           
    ,seller_id                   -- '卖家销售id'        
    ,site                        -- '站点'   
    ,parent_asin                 -- '父asin'          
    ,title                       -- '标题'    
    ,sessions                    -- '买家访问次数'    
    ,session_percentage          -- '买家访问次数百分比'                        
    ,page_views                  -- '页面浏览次数'      
    ,page_views_percentage       -- '页面浏览次数百分比'                           
    ,buy_box_percentage          -- '购买按钮赢得率'                        
    ,units_ordered               -- '已订购商品数量'         
    ,units_ordered_b2b           -- '订购数量 – B2B'             
    ,unit_session_percentage     -- '订单商品数量转化率'                             
    ,unit_session_percentage_b2b -- '订单商品数量转化率b2b'                                 
    ,currency                    -- '币种'       
    ,ordered_product_sales       -- '已订购商品销售额'                           
    ,ordered_product_sales_b2b   -- '已订购商品的销售额 – B2B'                               
    ,total_order_items           -- '订单商品种类数'             
    ,total_order_items_b2b       -- '订单商品总数 – B2B'                 
    ,generate_date               -- '爬取日期'          
    ,created_time                -- ''           
    ,updated_time                -- ''           
    ,now() as ods_create_time   -- '导入数据时间'
from ec_amazon_business_report_by_parent            
where  \$CONDITIONS" 

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] sqoop execute failed!"
     exit $?
fi

echo "--开始导入数据到ods表--"

DISK_SPACE=$(hadoop fs -du -h -s ${target_dir} | awk -F ' ' '{print int($1)}')
if [ $DISK_SPACE -gt 0 ];then
    sql="insert overwrite table ymx.ods_amazon_business_report_by_parent partition(company_code='${company_code}') 
        select 
             id                                  
            ,user_account                       
            ,seller_id                         
            ,site                        
            ,parent_asin                     
            ,title                       
            ,sessions                        
            ,session_percentage                               
            ,page_views                  
            ,page_views_percentage                               
            ,buy_box_percentage                           
            ,units_ordered                 
            ,units_ordered_b2b                
            ,unit_session_percentage                               
            ,unit_session_percentage_b2b                                  
            ,currency                    
            ,ordered_product_sales                          
            ,ordered_product_sales_b2b                                  
            ,total_order_items              
            ,total_order_items_b2b                 
            ,generate_date                       
            ,created_time                
            ,updated_time                
            ,ods_create_time   
        from ymx_tmp.ods_amazon_business_report_by_parent where company_code='${company_code}'
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