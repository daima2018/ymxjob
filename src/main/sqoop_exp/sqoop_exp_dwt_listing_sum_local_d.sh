#!/bin/bash
#############################################################
#源表   dwt_listing_sum_local_d
#名称   listing统计表(本地时间)
#############################################################

source /opt/jobs/conf/sqoop-job-conf.sh

#获取脚本参数
opts=$@
getparam(){
    arg=$1
    echo $opts |xargs -n1|cut -b 2- |awk -F '=' '{if($1=="'"$arg"'") print $2}'
}
#解析脚本参数
start_date=`getparam start_date`
end_date=`getparam end_date`

hive_dbname=ymx
tmp_dbname=ymx_tmp
hive_tbname=dwt_listing_sum_local_d
tmp_tbname=dwt_listing_sum_local_d   #不需要建表,只要路径就行

target_dir=${HDFS_BASE_DIR}/${tmp_dbname}.db/${tmp_tbname}

echo "--target_dir:${target_dir}"

db_connect=`eval echo  '$'"${company_code}_db_connect"`
db_username=`eval echo '$'"${company_code}_db_username"`
db_password=`eval echo '$'"${company_code}_db_password"`

echo "--connect:${db_connect}"
echo "--db_username:${db_username}"
echo "--db_password:${db_password}"

/opt/module/hive-3.1.2/bin/hive -e "
insert overwrite direcotry '${target_dir}'
select
    user_account
    ,site
    ,seller_sku
    ,asin
    ,qty
    ,sale_amount
    ,sale_amount_usd
    ,sale_amount_eur
    ,sale_amount_gbp
    ,sale_amount_jpy
    ,sale_amount_original
    ,created_time
    ,updated_time
    ,sale_order_num
    ,refund_amount
    ,refund_money
    ,refund_money_usd
    ,refund_money_eur
    ,refund_money_gbp
    ,refund_money_jpy
    ,refund_money_local
    ,key1
    ,return_amount
    ,asin_type
    ,ad_qty
    ,ad_sale_amount
    ,ad_sale_amount_usd
    ,ad_sale_amount_eur
    ,ad_sale_amount_gbp
    ,ad_sale_amount_jpy
    ,ad_sale_order_num
    ,ad_sale_amount_original
    ,cost
    ,cost_local
    ,cost_usd
    ,cost_eur
    ,cost_gbp
    ,cost_jpy
    ,clicks
    ,impressions
    ,sessions
    ,page_views
    ,buy_box_percentage
    ,session_percentage
    ,page_views_percentage
from '${hive_dbname}'.'${hive_tbname}' 
where company_code='${company_code}' 
    and stat_date>='${start_date}' and stat_date<'${end_date}'
"


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


from dwt_listing_sum_local_d where             
where   \$CONDITIONS" 

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] sqoop execute failed!"
     exit $?
fi

echo "--开始导入数据到ods表--"

DISK_SPACE=$(hadoop fs -du -h -s ${target_dir} | awk -F ' ' '{print int($1)}')
if [ $DISK_SPACE -gt 0 ];then
    sql="insert overwrite table ${ods_dbname}.${ods_tbname}  
        select 
            company_id             
            ,company_code                            
            ,company_short                            
            ,company_name                            
            ,company_auth                            
            ,company_type                         
            ,company_status                      
            ,company_level                         
            ,verified_status                         
            ,mobile                           
            ,company_name_cn                           
            ,company_inner_source                         
            ,company_source                         
            ,deploy_status         
            ,season                            
            ,db_code                          
            ,currency_local                           
            ,created_time                            
            ,updated_time                            
            ,ods_create_time
        from ${tmp_dbname}.${tmp_tbname} "
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