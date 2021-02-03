#!/bin/bash
#############################################################
#源表   ec_amazon_order_detail
#名称   amazon原始订单明细表
#目标表 ods_amazon_order_detail
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

ods_dbname=ymx
tmp_dbname=ymx_tmp
ods_tbname=ods_amazon_order_detail
tmp_tbname=ods_amazon_order_detail

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
     aod_id                            -- '自增长序列'                          
    ,aoo_id                            -- 'amazon订单原始表主键ID'                         
    ,user_account                      -- '店铺账号'                                    
    ,amazon_order_id                   -- '亚马逊所定义的订单编码，格式3-7-7'                                       
    ,asin                              -- '商品的亚马逊商品编码'                            
    ,seller_sku                        -- '销售SKU'                                  
    ,order_item_id                     -- '亚马逊所定义的订单商品编码'                                     
    ,title                             -- '商品的名称'                             
    ,quantity_ordered                  -- '订单中的商品数量。'                                    
    ,quantity_shipped                  -- '已配送的商品数量'                                    
    ,gift_message_text                 -- '由买方提供的礼物消息'                                         
    ,item_sale_amount                  -- 'item的销售额，自己计算的'                                        
    ,gift_wrap_level                   -- '买家指定的礼品包装等级'                                       
    ,item_price_currency_code          -- '商品的售价，币种'                                                
    ,item_price_amount                 -- '商品的售价-金额，请注意，订单商品涉及到商品及其数量。即，ItemPrice 的值等于 商品的售价 x 商品的订购数量。另外，ItemPrice 不包括 ShippingPrice 和 GiftWrapPrice。'                                         
    ,shipping_price_currency_code      -- '商品配送费用-币种'                                                    
    ,shipping_price_amount             -- '商品配送费用-金额'                                             
    ,gift_wrap_price_currency_code     -- '礼品包装费用-币种'                                                     
    ,gift_wrap_price_amount            -- '礼品包装费用-金额'                                              
    ,item_tax_currency_code            -- '商品价格所缴税费-币种'                                              
    ,item_tax_amount                   -- '商品价格所缴税费'                                       
    ,shipping_tax_currency_code        -- '商品配送费用所缴税费-币种'                                                  
    ,shipping_tax_amount               -- '商品配送费用所缴税费-金额'                                           
    ,gift_wrap_tax_currency_code       -- '礼品包装费用所缴税费-币种'                                                   
    ,gift_wrap_tax_amount              -- '礼品包装费用所缴税费-金额'                                            
    ,shipping_discount_currency_code   -- '商品配送费用所享折扣-币种'                                                       
    ,shipping_discount_amount          -- '商品配送费用所享折扣-金额'                                                
    ,promotion_discount_currency_code  -- '报价中的总促销折扣-币种'                                                        
    ,promotion_discount_amount         -- '报价中的总促销折扣-金额'                                                 
    ,cod_fee_currency_code             -- '货到付款服务收取的费用-币种'                                             
    ,cod_fee_amount                    -- '货到付款服务收取的费用-金额'                                      
    ,cod_fee_discount_currency_code    -- '货到付款费用的折扣-币种'                                                      
    ,cod_fee_discount_amount           -- '货到付款费用的折扣-金额'                                               
    ,invoice_requirement               -- '发票要求信息(Individual - 买家要求对订单中的每件商品单独开具发票。)'                                           
    ,invoice_buyer_selected_category   -- '买家在下订单时选择的发票类目信息'                                                       
    ,invoice_title                     -- '买家指定的发票抬头'                                     
    ,invoice_information               -- '发票要求信息(NotApplicable - 买家不要求开具发票。)'                                           
    ,condition_id                      -- '商品的状况(New,Used...)'                                    
    ,condition_subtype_id              -- '商品的子状况(New,Mint.....)'                                            
    ,condition_note                    -- '卖家描述的商品状况'                                      
    ,scheduled_delivery_start_date     --'订单预约送货上门的开始日期（目的地时区）'                                                      
    ,scheduled_delivery_end_date       --'订单预约送货上门的终止日期（目的地时区）'                                                    
    ,price_designation                 -- '商业价格'                                         
    ,payments_date                     -- '付款日期'                                     
    ,buyer_phone_number                -- '买家手机号'                                          
    ,delivery_instructions             -- '配送说明'                                             
    ,delivery_time_zone                -- '交货时区'                                          
    ,created_time                      --'创建时间'                                     
    ,updated_time                      --'更新时间'                                     
    ,now() as ods_create_time   -- '导入数据时间'
from ec_amazon_order_detail            
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
    sql="insert overwrite table ymx.ods_amazon_order_detail partition(company_code='${company_code}') 
        select 
            aod_id                             
            ,aoo_id                             
            ,user_account                      
            ,amazon_order_id                                                          
            ,asin                              
            ,seller_sku                        
            ,order_item_id                     
            ,title                             
            ,quantity_ordered                  
            ,quantity_shipped                  
            ,gift_message_text                 
            ,item_sale_amount                  
            ,gift_wrap_level                   
            ,item_price_currency_code          
            ,item_price_amount                  
            ,shipping_price_currency_code                                                          
            ,shipping_price_amount                                                          
            ,gift_wrap_price_currency_code                                                          
            ,gift_wrap_price_amount                                                          
            ,item_tax_currency_code                                                          
            ,item_tax_amount                   
            ,shipping_tax_currency_code                                                          
            ,shipping_tax_amount                                                          
            ,gift_wrap_tax_currency_code                                                          
            ,gift_wrap_tax_amount                                                          
            ,shipping_discount_currency_code                                                          
            ,shipping_discount_amount                                                          
            ,promotion_discount_currency_code                                                          
            ,promotion_discount_amount                                                          
            ,cod_fee_currency_code                                                          
            ,cod_fee_amount                                                          
            ,cod_fee_discount_currency_code                                                          
            ,cod_fee_discount_amount                                                          
            ,invoice_requirement                                                        
            ,invoice_buyer_selected_category   
            ,invoice_title                     
            ,invoice_information                                                         
            ,condition_id                                       
            ,condition_subtype_id                                         
            ,condition_note                    
            ,scheduled_delivery_start_date     
            ,scheduled_delivery_end_date       
            ,price_designation                 
            ,payments_date                     
            ,buyer_phone_number                
            ,delivery_instructions             
            ,delivery_time_zone                
            ,created_time                      
            ,updated_time                      
            ,ods_create_time

        from (
            select 
                t.*
                ,row_number() over(partition by aod_id order by updated_time desc) rn
            from(select 
                    aod_id,aoo_id,user_account,amazon_order_id,asin,seller_sku,order_item_id,title,quantity_ordered,quantity_shipped,
                    gift_message_text,item_sale_amount,gift_wrap_level,item_price_currency_code,item_price_amount,shipping_price_currency_code,
                    shipping_price_amount,gift_wrap_price_currency_code,gift_wrap_price_amount,item_tax_currency_code,item_tax_amount,
                    shipping_tax_currency_code,shipping_tax_amount,gift_wrap_tax_currency_code,gift_wrap_tax_amount,shipping_discount_currency_code,
                    shipping_discount_amount,promotion_discount_currency_code,promotion_discount_amount,cod_fee_currency_code,cod_fee_amount,
                    cod_fee_discount_currency_code,cod_fee_discount_amount,invoice_requirement,invoice_buyer_selected_category,invoice_title,
                    invoice_information,condition_id,condition_subtype_id,condition_note,scheduled_delivery_start_date,scheduled_delivery_end_date,
                    price_designation,payments_date,buyer_phone_number,delivery_instructions,delivery_time_zone,created_time,updated_time,
                    ods_create_time
                from ymx_tmp.ods_amazon_order_detail where company_code='${company_code}'
                union all 
                select 
                    aod_id,aoo_id,user_account,amazon_order_id,asin,seller_sku,order_item_id,title,quantity_ordered,quantity_shipped,
                    gift_message_text,item_sale_amount,gift_wrap_level,item_price_currency_code,item_price_amount,shipping_price_currency_code,
                    shipping_price_amount,gift_wrap_price_currency_code,gift_wrap_price_amount,item_tax_currency_code,item_tax_amount,
                    shipping_tax_currency_code,shipping_tax_amount,gift_wrap_tax_currency_code,gift_wrap_tax_amount,shipping_discount_currency_code,
                    shipping_discount_amount,promotion_discount_currency_code,promotion_discount_amount,cod_fee_currency_code,cod_fee_amount,
                    cod_fee_discount_currency_code,cod_fee_discount_amount,invoice_requirement,invoice_buyer_selected_category,invoice_title,
                    invoice_information,condition_id,condition_subtype_id,condition_note,scheduled_delivery_start_date,scheduled_delivery_end_date,
                    price_designation,payments_date,buyer_phone_number,delivery_instructions,delivery_time_zone,created_time,updated_time,
                    ods_create_time
                from ymx.ods_amazon_order_detail where company_code='${company_code}'
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