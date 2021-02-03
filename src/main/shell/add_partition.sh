#!/bin/bash
#############################################################
#名称   给每个公司添加表分区
#############################################################

source /home/ecm/ymx/ymxjob/src/main/common/functions.sh

#获取脚本参数
opts=$@

#解析脚本参数
company_code=`getparam company_code "$opts"`

hive -e "
alter table ymx_tmp.ods_amazon_parent_listing add partition(company_code='${company_code}');
alter table ymx_tmp.ods_amazon_get_merchant_listings_data add partition(company_code='${company_code}');
alter table ymx_tmp.ods_amazon_order_original add partition(company_code='${company_code}');
alter table ymx_tmp.ods_amazon_order_detail add partition(company_code='${company_code}');
alter table ymx_tmp.ods_product_ad_products_report_daily add partition(company_code='${company_code}');
alter table ymx_tmp.ods_amazon_v2_settlement_detail add partition(company_code='${company_code}');
alter table ymx_tmp.ods_amazon_fba_fulfillment_customer_returns_data add partition(company_code='${company_code}');
alter table ymx_tmp.ods_amazon_business_report_by_child add partition(company_code='${company_code}');
alter table ymx_tmp.ods_amazon_business_report_by_parent add partition(company_code='${company_code}');
alter table ymx_tmp.ods_platform_user add partition(company_code='${company_code}');
"

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] hive execute failed!"
     exit $?
fi


