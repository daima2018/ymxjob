alter table ymx_tmp.ods_amazon_parent_listing add partition(company_code='A20090247');
alter table ymx_tmp.ods_amazon_get_merchant_listings_data add partition(company_code='A20090247');
alter table ymx_tmp.ods_amazon_order_original add partition(company_code='A20090247');
alter table ymx_tmp.ods_amazon_order_detail add partition(company_code='A20090247');
alter table ymx_tmp.ods_product_ad_products_report_daily add partition(company_code='A20090247');
alter table ymx_tmp.ods_amazon_v2_settlement_detail add partition(company_code='A20090247');
alter table ymx_tmp.ods_amazon_fba_fulfillment_customer_returns_data add partition(company_code='A20090247');
alter table ymx_tmp.ods_amazon_business_report_by_child add partition(company_code='A20090247');
alter table ymx_tmp.ods_amazon_business_report_by_parent add partition(company_code='A20090247');
alter table ymx_tmp.ods_platform_user add partition(company_code='A20090247');