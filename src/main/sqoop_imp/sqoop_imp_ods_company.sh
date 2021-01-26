#!/bin/bash
#############################################################
#源表   ec_company
#名称   客户主表
#目标表 ods_company
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
ods_tbname=ods_company
tmp_tbname=ods_company

target_dir=${HDFS_BASE_DIR}/${tmp_dbname}.db/${tmp_tbname}

echo "--target_dir:${target_dir}"

db_connect=${center_db_connect}
db_username=${center_db_username}
db_password=${center_db_password}




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
     company_id            --  ''            
    ,company_code          -- '客户代码'                  
    ,company_short         -- '简称'                   
    ,company_name          -- '客户名称'                  
    ,company_auth          -- '授权编码'                  
    ,company_type          -- '公司类型'               
    ,company_status        -- '客户状态 0=已注销 1=待激活 2=已停用 3=已激活'                 
    ,company_level         -- '客户等级 0=未绑定  关联sys_base_levell等级表'                
    ,verified_status       -- '认证状态：0未认证,1待审核.2未通过，3审核中，4认证失败，5认证通过'                  
    ,mobile                -- '联系方式'           
    ,company_name_cn       -- '公司名称-中文'                     
    ,company_inner_source  -- '注册来源 1 ERP. 2 用户中心'                       
    ,company_source        -- '注册来源 1 ERP. 2 用户中心'                 
    ,deploy_status         -- '部署状态 1初始化 2部署中3部署失败 4已部署'                
    ,season                -- '失败原因'            
    ,db_code               -- '数据库编码'           
    ,currency_local        -- '本位币设置，默认为RMB。'                   
    ,created_time          -- '建立时间'                  
    ,updated_time          -- '更新时间'                  
    ,now() as ods_create_time   -- '导入数据时间'
from ec_company            
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
    hive -e "${sql}"
else
        echo '未获取到数据！！！'
fi

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] hive execute failed!"
     exit $?
fi                           