#!/bin/bash
#############################################################
#源表   ec_currency_rate_rule
#名称   汇率表
#目标表 ods_currency_rate_rule
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
ods_tbname=ods_currency_rate_rule
tmp_tbname=ods_currency_rate_rule

target_dir=${HDFS_BASE_DIR}/${tmp_dbname}.db/${tmp_tbname}

echo "--target_dir:${target_dir}"

db_connect=${center_db_connect}
db_username=${center_db_username}
db_password=${center_db_password}

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
     crr_id                 --  ''
    ,crr_date               -- '汇率时间时间'
    ,crr_local              -- '本位币'
    ,created_time           -- '创建时间'
    ,updated_time           -- '更新时间'
    ,now() as ods_create_time   -- '导入数据时间'
from ec_currency_rate_rule            
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
            crr_id                  
            ,crr_date               
            ,crr_local              
            ,created_time           
            ,updated_time           
            ,ods_create_time
        from ${tmp_dbname}.${tmp_tbname} "
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