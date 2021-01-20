#!/bin/bash
#############################################################
#源表   ec_currency_rate_rule_detail
#名称   汇率明细表
#目标表 ods_currency_rate_rule_detail
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

ods_dbname=ymx
tmp_dbname=ymx_tmp
ods_tbname=ods_currency_rate_rule_detail
tmp_tbname=ods_currency_rate_rule_detail

target_dir=${HDFS_BASE_DIR}/${tmp_dbname}.db/${tmp_tbname}

echo "--target_dir:${target_dir}"

db_connect=${center_db_connect}
db_username=${center_db_username}
db_password=${center_db_password}

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
     crrd_id                    -- ''
    ,crr_id                     -- '规则ID'
    ,crrd_currency_code         -- '币种简称'
    ,crrd_currency_rate         -- '系统汇率'
    ,created_time               -- '创建时间'
    ,updated_time               -- '更新时间'
    ,now() as ods_create_time   -- '导入数据时间'
from ec_currency_rate_rule_detail            
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
    sql="insert overwrite table ymx.ods_currency_rate_rule_detail  
        select 
             crrd_id                    
            ,crr_id                   
            ,crrd_currency_code       
            ,crrd_currency_rate       
            ,created_time             
            ,updated_time             
            ,ods_create_time
        from (
            select 
                t.*
                ,row_number() over(partition by crrd_id order by updated_time desc) rn
            from(select 
                    crrd_id,crr_id,crrd_currency_code,crrd_currency_rate,created_time,updated_time,ods_create_time
                from ymx_tmp.ods_currency_rate_rule_detail 
                union all 
                select 
                    crrd_id,crr_id,crrd_currency_code,crrd_currency_rate,created_time,updated_time,ods_create_time
                from ymx.ods_currency_rate_rule_detail 
            ) t 
        ) tt
        where rn=1
        "
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