#!/bin/bash
#############################################################
#源表   ec_amazon_parent_listing
#名称   父listing数据
#目标表 ods_amazon_parent_listing
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
ods_tbname=ods_amazon_parent_listing
tmp_tbname=ods_amazon_parent_listing

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
     id                         -- '主键'
    ,asin                       -- '父asin或者独立asin' 
    ,user_account               -- '店铺账号'        
    ,seller_sku                 -- 'seller_sku'      
    ,created_time               -- ''  
    ,updated_time               -- ''  
    ,is_parent                  -- '1是父asin0是独立asin' 
    ,top_time                   -- '置顶时间' 
    ,image_url                  -- '图片地址' 
    ,item_name                  -- 'sku名称'  
    ,md5_key                    -- 'user_account.asin.seller_sku的MD5值'    
    ,now() as ods_create_time   -- '导入数据时间'
from ec_amazon_parent_listing            
where ('${start_date}'<updated_time and updated_time<'${end_date}')
    and \$CONDITIONS" 

#created_time和updated_time的格式为yyyy-MM-dd HH:mm:ss

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] sqoop execute failed!"
     exit $?
fi

echo "--开始导入数据到ods表--"

DISK_SPACE=$(hadoop fs -du -h -s ${target_dir} | awk -F ' ' '{print int($1)}')
if [ $DISK_SPACE -gt 0 ];then
    sql="insert overwrite table ymx.ods_amazon_parent_listing partition(company_code='${company_code}') 
        select
            id,asin,user_account,seller_sku,created_time,updated_time,is_parent,top_time,image_url,item_name,md5_key,ods_create_time
        from (
            select 
                t.*
                ,row_number() over(partition by id order by updated_time desc) rn
            from(select 
                    id,asin,user_account,seller_sku,created_time,updated_time,is_parent,top_time,image_url,item_name,md5_key,ods_create_time
                from ymx_tmp.ods_amazon_parent_listing where company_code='${company_code}'
                union all 
                select 
                    id,asin,user_account,seller_sku,created_time,updated_time,is_parent,top_time,image_url,item_name,md5_key,ods_create_time
                from ymx.ods_amazon_parent_listing where company_code='${company_code}'
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