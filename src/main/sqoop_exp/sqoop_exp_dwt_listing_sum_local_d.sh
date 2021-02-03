#!/bin/bash
#############################################################
#源表   dwt_listing_sum_local_d
#名称   listing统计表(本地时间)
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

hive_dbname=ymx
tmp_dbname=ymx_tmp
hive_tbname=dwt_listing_sum_local_d
tmp_tbname=dwt_listing_sum_local_d   #不需要建表,只要路径就行

target_dir=${HDFS_BASE_DIR}/${tmp_dbname}.db/${tmp_tbname}/company_code=${company_code}

echo "--target_dir:${target_dir}"

db_connect=`eval echo  '$'"${company_code}_db_connect"`
db_username=`eval echo '$'"${company_code}_db_username"`
db_password=`eval echo '$'"${company_code}_db_password"`

#删除临时目录
DISK_SPACE=$(hadoop fs -du -h -s ${target_dir} | awk -F ' ' '{print int($1)}')
if [ $DISK_SPACE -gt 0 ];then
    echo "删除临时目录"
    hadoop fs -rmr ${target_dir}
else
    echo '未获取到数据！！！'
fi

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] execute failed!"
     exit $?
fi

columns=user_account,site,seller_sku,asin,qty,summary_date,sale_amount,sale_amount_usd,sale_amount_eur,sale_amount_gbp,sale_amount_jpy,sale_amount_original,created_time,updated_time,sale_order_num,refund_amount,refund_money,refund_money_usd,refund_money_eur,refund_money_gbp,refund_money_jpy,refund_money_local,key1,return_amount,asin_type,ad_qty,ad_sale_amount,ad_sale_amount_usd,ad_sale_amount_eur,ad_sale_amount_gbp,ad_sale_amount_jpy,ad_sale_order_num,ad_sale_amount_original,cost,cost_local,cost_usd,cost_eur,cost_gbp,cost_jpy,clicks,impressions,sessions,page_views,buy_box_percentage,session_percentage,page_views_percentage

hive -e "
insert overwrite directory '${target_dir}'
select
     user_account
    ,site
    ,seller_sku
    ,asin
    ,qty
    ,stat_date as summary_date
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
from ymx.dwt_listing_sum_local_d 
where company_code='${company_code}' 
    and stat_date>='${start_date}' and stat_date<'${end_date}'
    and length(asin)<21
"

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] hive execute failed!"
     exit $?
fi

echo "--导出之前先删除--"
sqoop eval                       \
--connect ${db_connect} \
--username ${db_username}  \
--password ${db_password} \
--query "delete from ec_amazon_listing_extend_statistics_local Where summary_date>='${start_date}' and summary_date<'${end_date}'"

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] sqoop eval execute failed!"
     exit $?
fi

echo "--开始导出数据--"

DISK_SPACE=$(hadoop fs -du -h -s ${target_dir} | awk -F ' ' '{print int($1)}')
if [ $DISK_SPACE -gt 0 ];then
    sqoop export             \
    --connect ${db_connect} \
    --username ${db_username} \
    --password ${db_password} \
    --table ec_amazon_listing_extend_statistics_local     \
    --num-mappers 4                          \
    --export-dir ${target_dir}   \
    --columns ${columns}     \
    --input-fields-terminated-by '\001'          \
    --input-lines-terminated-by   '\n'     \
    --input-null-string '\\N'                    \
    --input-null-non-string '\\N'               
else
        echo '未获取到数据！！！'
fi

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] sqoop execute failed!"
     exit $?
fi                           