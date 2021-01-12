#!/bin/bash
#############################################################
#源表   ec_amazon_business_report_by_child
#名称   BusinessReport 流量 报告 子 asin维度
#目标表 ods_amazon_business_report_by_child
#############################################################

source /opt/jobs/conf/sqoop-job-conf.sh

#获取脚本参数
opts=$@
getparam(){
    arg=$1
    echo $opts |xargs -n1|cut -b 2- |awk -F '=' '{if($1=="'"$arg"'") print $2}'
}
#解析脚本参数
start_time=`getparam start_time`
end_time=`getparam end_time`
company_code=`getparam company_code`

ods_dbname=ymx
tmp_dbname=ymx_tmp
ods_tbname=ods_amazon_business_report_by_child
tmp_tbname=ods_amazon_business_report_by_child

target_dir=${HDFS_BASE_DIR}/${tmp_dbname}.db/${tmp_tbname}/company_code=${company_code}

echo "--target_dir:${target_dir}"

db_connect=`eval echo  '$'"${company_code}_db_connect"`
db_username=`eval echo '$'"${company_code}_db_username"`
db_password=`eval echo '$'"${company_code}_db_password"`

echo "--connect:${db_connect}"
echo "--db_username:${db_username}"
echo "--db_password:${db_password}"


/opt/module/sqoop/bin/sqoop import \
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
    ,company_code                -- '公司代码'         
    ,user_account                -- '店铺账号'         
    ,seller_id                   -- '卖家销售id'      
    ,site                        -- '站点'  
    ,parent_asin                 -- '父asin'        
    ,child_asin                  -- '子asin'       
    ,title                       -- '标题'  
    ,seller_sku                  -- '卖家 sku'       
    ,sessions                    -- '买家访问次数'  
    ,session_percentage          -- '买家访问次数百分比'                      
    ,page_views                  -- '页面浏览次数'    
    ,page_views_percentage       -- '页面浏览次数百分比'                         
    ,buy_box_percentage          -- '购买按钮赢得率'                      
    ,units_ordered               -- '已订购商品数量'       
    ,units_ordered_b2b           -- '订购数量 – B2B'           
    ,unit_session_percentage     -- '订单商品数量转化率'                           
    ,unit_session_percentage_b2b -- '商品转化率 – B2B'                               
    ,currency                    -- '币种'     
    ,ordered_product_sales       -- '已订购商品销售额'                         
    ,ordered_product_sales_b2b   -- '已订购商品的销售额 – B2B'                             
    ,total_order_items           -- '订单商品种类数'           
    ,total_order_items_b2b       -- '订单商品总数 – B2B'               
    ,generate_date               -- '爬取日期'          
    ,created_time                -- ''         
    ,updated_time                -- ''         
    ,now() as ods_create_time    -- '导入数据时间'
from ec_amazon_business_report_by_child            
where   \$CONDITIONS" 

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] sqoop execute failed!"
     exit $?
fi

echo "--开始导入数据到ods表--"

DISK_SPACE=$(hadoop fs -du -h -s ${target_dir} | awk -F ' ' '{print int($1)}')
if [ $DISK_SPACE -gt 0 ];then
    sql="insert overwrite table ${ods_dbname}.${ods_tbname} partition(company_code='${company_code}') 
        select 
             id                          
            ,company_code                
            ,user_account                
            ,seller_id                   
            ,site                        
            ,parent_asin                 
            ,child_asin                  
            ,title                       
            ,seller_sku                  
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
        from ${tmp_dbname}.${tmp_tbname} where company_code='${company_code}'"
    echo "--$DISK_SPACE 文件目录已经存在，执行数据写入操作$sql"
    /opt/module/hive-3.1.2/bin/hive -e "${sql}"
else
        echo '未获取到数据！！！'
fi

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] hive execute failed!"
     exit $?
fi