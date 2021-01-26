#!/bin/bash
#############################################################
#源表   product_ad_products_report_daily
#名称   Amazon广告产品报告日表
#目标表 ods_product_ad_products_report_daily
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
ods_tbname=ods_product_ad_products_report_daily
tmp_tbname=ods_product_ad_products_report_daily

target_dir=${HDFS_BASE_DIR}/${tmp_dbname}.db/${tmp_tbname}/company_code=${company_code}

echo "--target_dir:${target_dir}"

db_connect=`eval echo  '$'"${company_code}_ad_db_connect"`
db_username=`eval echo '$'"${company_code}_ad_db_username"`
db_password=`eval echo '$'"${company_code}_ad_db_password"`

echo "--connect:${db_connect}"
echo "--db_username:${db_username}"
echo "--db_password:${db_password}"


sqoop import \
--connect ${db_connect} \
--username ${db_username} \
--password ${db_password} \
--hive-overwrite \
--target-dir $target_dir \
--split-by paprdy_id -m 4  \
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
     paprdy_id                 -- ''                     
    ,user_account              -- '平台账号'                
    ,profile_id                -- 'profileId'              
    ,campaign_id               -- '活动campaignId'               
    ,campaign_name             -- '活动名称'                 
    ,ad_group_id               -- '广告组ID'               
    ,ad_group_name             -- '广告组名称'                 
    ,asin                      -- 'ASIN'        
    ,sku                       -- 'SKU'       
    ,item_name                 -- '产品名称'             
    ,aapr_status               -- '产品状态 enabled:启用,paused:暂停,archived:归档、删除'               
    ,ad_id                     -- '广告产品ID'         
    ,currency                  -- '币种'            
    ,impressions               -- '曝光度'            
    ,clicks                    -- '点击数'       
    ,cost                      -- '所有点击的总费用'               
    ,generated_date            -- '报告生成生成日期'                
    ,conversions1d             -- '点击广告1天内发生的归因转化事件数'              
    ,conversions1d_same_sku    -- '点击广告后1天内发生的归因转化事件的数量，其中已购买的SKU与广告中的SKU相同'                       
    ,units_ordered1d           -- '点击广告后1天内订购的归属订单数量'                
    ,units_ordered1d_same_sku  -- '点击的广告在1天内订购的归因单位数（购买的SKU与广告的SKU相同）'                         
    ,sales1d                   -- '点击广告后1天内发生的归因销售数量'                  
    ,sales1d_same_sku          -- '在购买的SKU与广告中的SKU相同的广告点击后的1天内发生的归因销售总值'                           
    ,conversion1d_rate         -- '1天内转化率：1天订单数/点击次数'                            
    ,ctr                       -- '点击率：点击数/曝光数'              
    ,cpc                       -- 'CPC ：广告费/点击数'              
    ,acos1d                    -- '1天内ACoS：广告费/1天销售额'                 
    ,product_amount            -- '该sku的销售额'                         
    ,create_id                 -- '创建人'          
    ,created_time              -- '添加时间'                
    ,update_id                 -- '修改人'          
    ,updated_time              -- '更新时间'                
    ,order_proportion          -- '广告订单占比：广告总订单量/店铺总订单量'                           
    ,sale_proportion           -- '广告销售额占比：广告总销售额/店铺销售额'                          
    ,cost_proportion           -- '广告花费占比：广告总花费/店铺销售额'                          
    ,cpa                       -- '单位订单平均广告花费：广告总花费/广告总订单量'              
    ,roas                      -- '广告产出投入比：广告总销售额/广告总花费'               
    ,conversions7d             -- '点击广告7天内发生的归因转化事件数'              
    ,conversions7d_same_sku    -- '点击广告后7天内发生的归因转化事件的数量，其中已购买的SKU与广告中的SKU相同'                       
    ,units_ordered7d           -- '点击广告后1天内订购的归属订单数量'                
    ,sales7d                   -- '点击广告后7天内发生的归因销售数量'                  
    ,sales7d_same_sku          -- '在购买的SKU与广告中的SKU相同的广告点击后的7天内发生的归因销售总值'                           
    ,conversion7d_rate         -- '7天内转化率：7天订单数/点击次数'                            
    ,acos7d                    -- '7天内ACoS： 广告费/7天销售额'                 
    ,units_ordered7d_same_sku  -- '点击广告后7天内订购的归因单位数（购买的SKU与广告的SKU相同）'                         
    ,order_amount              -- '时间段内店铺金额'                       
    ,order_total               -- '时间段内订单数量'            
    ,now() as ods_create_time   -- '导入数据时间'
from product_ad_products_report_daily       
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
    sql="insert overwrite table ymx.ods_product_ad_products_report_daily partition(company_code='${company_code}') 
        select 
             paprdy_id                 
            ,user_account                              
            ,profile_id                              
            ,campaign_id                              
            ,campaign_name                              
            ,ad_group_id                              
            ,ad_group_name                              
            ,asin                              
            ,sku                              
            ,item_name                              
            ,aapr_status                              
            ,ad_id                              
            ,currency                              
            ,impressions                           
            ,clicks                           
            ,cost                                       
            ,generated_date                            
            ,conversions1d                           
            ,conversions1d_same_sku                           
            ,units_ordered1d                           
            ,units_ordered1d_same_sku                           
            ,sales1d                      
            ,sales1d_same_sku             
            ,conversion1d_rate                                        
            ,ctr                                        
            ,cpc                                        
            ,acos1d                                        
            ,product_amount               
            ,create_id                           
            ,created_time                              
            ,update_id                           
            ,updated_time                              
            ,order_proportion                                        
            ,sale_proportion                                        
            ,cost_proportion                                        
            ,cpa                                        
            ,roas                                        
            ,conversions7d                           
            ,conversions7d_same_sku                           
            ,units_ordered7d                           
            ,sales7d                      
            ,sales7d_same_sku             
            ,conversion7d_rate                                        
            ,acos7d                                      
            ,units_ordered7d_same_sku                           
            ,order_amount                           
            ,order_total                           
            ,ods_create_time
        from (
            select 
                t.*
                ,row_number() over(partition by paprdy_id order by updated_time desc) rn
            from(select 
                    paprdy_id,user_account,profile_id,campaign_id,campaign_name,ad_group_id,ad_group_name,asin,sku,item_name,
                    aapr_status,ad_id,currency,impressions,clicks,cost,generated_date,conversions1d,conversions1d_same_sku,
                    units_ordered1d,units_ordered1d_same_sku,sales1d,sales1d_same_sku,conversion1d_rate,ctr,cpc,acos1d,product_amount,
                    create_id,created_time,update_id,updated_time,order_proportion,sale_proportion,cost_proportion,cpa,roas,
                    conversions7d,conversions7d_same_sku,units_ordered7d,sales7d,sales7d_same_sku,conversion7d_rate,acos7d,
                    units_ordered7d_same_sku,order_amount,order_total,ods_create_time
                from ymx_tmp.ods_product_ad_products_report_daily where company_code='${company_code}'
                union all 
                select 
                    paprdy_id,user_account,profile_id,campaign_id,campaign_name,ad_group_id,ad_group_name,asin,sku,item_name,
                    aapr_status,ad_id,currency,impressions,clicks,cost,generated_date,conversions1d,conversions1d_same_sku,
                    units_ordered1d,units_ordered1d_same_sku,sales1d,sales1d_same_sku,conversion1d_rate,ctr,cpc,acos1d,product_amount,
                    create_id,created_time,update_id,updated_time,order_proportion,sale_proportion,cost_proportion,cpa,roas,
                    conversions7d,conversions7d_same_sku,units_ordered7d,sales7d,sales7d_same_sku,conversion7d_rate,acos7d,
                    units_ordered7d_same_sku,order_amount,order_total,ods_create_time
                from ymx.ods_product_ad_products_report_daily where company_code='${company_code}'
            ) t 
        ) tt
        where rn=1 
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