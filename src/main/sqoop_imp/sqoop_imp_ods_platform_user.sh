#!/bin/bash
#############################################################
#源表   ec_platform_user
#名称   
#目标表 ods_platform_user
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
ods_tbname=ods_platform_user
tmp_tbname=ods_platform_user

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
     pu_id                    -- ''
    ,user_account             -- '平台账号'
    ,short_name               -- '简称'
    ,site                     -- '站点'
    ,seller_id                -- 'Amazon-销售ID/卖家ID'
    ,currency_code            -- '站点币种'
    ,status                   -- '状态，0禁用，1：启用 2:临时禁用'
    ,date_create              -- '添加时间'
    ,platform_user_name       -- '平台登录名称'
    ,final_value_fee_currency -- ''
    ,erp_user_id              -- '系统用户id'
    ,auth_type                -- '是否平台项目授权 1是 0否'
    ,account_tax_id           -- '店铺税号'
    ,is_create_product        -- 'FBA极速版是否自动创建产品'
    ,is_create_warehouse      -- 'FBA极速版是否自动创建仓库/运输方式等信息'
    ,is_account_mark          -- '是否开启账号标发，1是0否'
    ,shopowner                -- '店长'
    ,auth_status              --  '授权状态：0未授权，1已授权，2授权异常，3异常，4授权过期'
    ,ad_auth_status           --  '广告授权状态：0未授权，1已授权'
    ,product_ad_auth_status   --  '是否商品广告授权'
    ,brand_ad_auth_status     -- '是否品牌广告授权'
    ,display_ad_auth_status   -- '是否展示型广告授权'
    ,is_del                   --  '是否删除：0正常账号，1删除的账号'
    ,marketplace_id           -- '市场编号'
    ,access_key               -- '访问秘钥-ID'
    ,secret_key               -- '访问秘钥-Token'
    ,developer_number         -- '开发编号'
    ,mws_auth_token           -- 'mws授权token'
    ,merchant_id              -- '卖家ID'
    ,oauth_token              -- 'OAuth token'
    ,refresh_token            -- 'Refresh token'
    ,expires_in               -- 'token过期时间'
    ,oauth_expires_in         -- 'oauth的过期时间'
    ,oauth_client_id          -- 'oauth client id'
    ,oauth_client_secret      -- 'oauth client secret'
    ,platform                 --  'amazon'
    ,created_time             --  '创建时间'
    ,updated_time             --  '更新时间'
    ,pu_type                  --  ''     
    ,now() as ods_create_time   -- '导入数据时间'
from ec_platform_user            
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
             pu_id                   
            ,user_account            
            ,short_name              
            ,site                    
            ,seller_id               
            ,currency_code           
            ,status                   
            ,date_create             
            ,platform_user_name      
            ,final_value_fee_currency
            ,erp_user_id             
            ,auth_type             
            ,account_tax_id          
            ,is_create_product       
            ,is_create_warehouse     
            ,is_account_mark         
            ,shopowner               
            ,auth_status             
            ,ad_auth_status          
            ,product_ad_auth_status  
            ,brand_ad_auth_status    
            ,display_ad_auth_status  
            ,is_del                  
            ,marketplace_id          
            ,access_key           
            ,secret_key             
            ,developer_number        
            ,mws_auth_token          
            ,merchant_id             
            ,oauth_token              
            ,refresh_token          
            ,expires_in             
            ,oauth_expires_in       
            ,oauth_client_id         
            ,oauth_client_secret    
            ,platform               
            ,created_time           
            ,updated_time           
            ,pu_type                  
            ,ods_create_time   
        from ${tmp_dbname}.${tmp_tbname} where company_code='${company_code}'"
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