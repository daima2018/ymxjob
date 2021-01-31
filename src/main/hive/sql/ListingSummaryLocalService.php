<?php
namespace App\Service\Summary;

use App\Model\Amazon\AmazonListingExtendSummaryLocalModel;
use App\Model\Amazon\AmazonOrderOriginalModel;
use App\Model\Amazon\AmazonV2SettlementDetailModel;
use App\Model\Amazon\BusinessReportByChildModel;
use App\Model\Amazon\BusinessReportByParentModel;
use App\Model\Amazon\FBAFulfillmentCustomerReturnsDataModel;
use App\Model\Amazon\MerchantListingsDataModel;
use App\Model\AmazonAd\ProductAdProductsReportDailyModel;
use App\Service\CacheData\CurrencyService;
use App\Service\CacheData\UserAccountService;
use App\Service\CommonService;
use App\Service\Functions;
use App\Service\Logs;
use App\Utility\Common;

class ListingSummaryLocalService {
    private $moduleFields = null;
    private $batchInsertKeys = [];
    private  $currencyLocal = null;
    private $currencyOriginal = [];

    public  function saveSummary($list=[]){
        $this->currencyLocal =  CurrencyService::getCurrencyLocal();

        Functions::cliEcho('消耗队列数据start : -- '. \md5(json_encode($list)) . ' currencyLocal ' . $this->currencyLocal ); 

        foreach ($list as $key=> $data){
            Functions::cliEcho('消耗队列数据:'.json_encode($data));

            //特殊处理
            if(!isset($data['company_code']) || ($data['company_code'] === 'A20080135')){
                continue;
            }

            $this->moduleFields = $this->getFields($data['fields']);
            $fields = $this->moduleFields;
            $md5Key = md5($data['user_account'] . $data['asin'] . $data['seller_sku']);
            $hadCache = ListingSummaryService::checkHadCached('local',$md5Key ,$data['date'],$data['fields']);
            if($hadCache){
                continue;
            }

            try{
                if((int)$data['is_parent'] === 1){
                    $summaryModel  = new MerchantListingsDataModel();
                    $sql = "SELECT asin1 AS asin,
                       seller_sku
                    FROM ec_amazon_get_merchant_listings_data
                    WHERE parent_asin != asin1 and parent_asin=? and user_account=? 
                    AND item_status='on_sale'
                 ";
                    $result = $summaryModel->query($sql,[$data['asin'],$data['user_account']]);
                    if($result){
                        $parentSummaryInfo = [];
                        foreach ($result as $k=> $v){
                            $param = [
                                'asin' => $v['asin'],
                                'user_account' => $data['user_account'],
                                'date' => $data['date'],
                                'seller_sku'=> $v['seller_sku'],
                                'site' => $data['site']
                            ];
                            $preSummary = [];
                            $summaryInfo = $this->getListingInfo($param,$preSummary);
                            $summaryInfo['asin_type'] = 1;
                            $this->save($summaryInfo, $param,$preSummary);
                            if(isset($summaryInfo['sale_amount'])){
                                $parentSummaryInfo['sale_amount'] = $parentSummaryInfo['sale_amount'] ?? 0;
                                $parentSummaryInfo['sale_amount'] += $summaryInfo['sale_amount'];
                            }
                            if(isset($summaryInfo['sale_money'])){
                                $parentSummaryInfo['sale_money'] = $parentSummaryInfo['sale_money'] ?? 0;
                                $parentSummaryInfo['sale_money'] += $summaryInfo['sale_money'];
                            }
                            if(isset($summaryInfo['sale_money_usd'])){
                                $parentSummaryInfo['sale_money_usd'] = $parentSummaryInfo['sale_money_usd'] ?? 0;
                                $parentSummaryInfo['sale_money_usd'] += $summaryInfo['sale_money_usd'];
                            }
                            if(isset($summaryInfo['sale_money_eur'])){
                                $parentSummaryInfo['sale_money_eur'] = $parentSummaryInfo['sale_money_eur'] ?? 0;
                                $parentSummaryInfo['sale_money_eur'] += $summaryInfo['sale_money_eur'];
                            }
                            if(isset($summaryInfo['sale_money_gbp'])){
                                $parentSummaryInfo['sale_money_gbp'] = $parentSummaryInfo['sale_money_gbp'] ?? 0;
                                $parentSummaryInfo['sale_money_gbp'] += $summaryInfo['sale_money_gbp'];
                            }
                            if(isset($summaryInfo['sale_money_jpy'])){
                                $parentSummaryInfo['sale_money_jpy'] = $parentSummaryInfo['sale_money_jpy'] ?? 0;
                                $parentSummaryInfo['sale_money_jpy'] += $summaryInfo['sale_money_jpy'];
                            }
                            if(isset($summaryInfo['sale_money_original'])){
                                $parentSummaryInfo['sale_money_original'] = $parentSummaryInfo['sale_money_original'] ?? 0;
                                $parentSummaryInfo['sale_money_original'] += $summaryInfo['sale_money_original'];
                            }
                            if(isset($summaryInfo['sale_order_num'])){
                                $parentSummaryInfo['sale_order_num'] = $parentSummaryInfo['sale_order_num'] ?? 0;
                                $parentSummaryInfo['sale_order_num'] += $summaryInfo['sale_order_num'];
                            }
                            //假设广告其中一个字段有，那么其他的应该也有
                            if(isset($summaryInfo['ad_sale_amount'])){
                                $parentSummaryInfo['ad_sale_amount'] = $parentSummaryInfo['ad_sale_amount'] ?? 0;
                                $parentSummaryInfo['ad_sale_amount'] += $summaryInfo['ad_sale_amount'];
                                $parentSummaryInfo['ad_sale_money'] = $parentSummaryInfo['ad_sale_money'] ?? 0;
                                $parentSummaryInfo['ad_sale_money'] += $summaryInfo['ad_sale_money'];
                                $parentSummaryInfo['ad_sale_money_usd'] = $parentSummaryInfo['ad_sale_money_usd'] ?? 0;
                                $parentSummaryInfo['ad_sale_money_usd'] += $summaryInfo['ad_sale_money_usd'];
                                $parentSummaryInfo['ad_sale_money_eur'] = $parentSummaryInfo['ad_sale_money_eur'] ?? 0;
                                $parentSummaryInfo['ad_sale_money_eur'] += $summaryInfo['ad_sale_money_eur'];
                                $parentSummaryInfo['ad_sale_money_gbp'] = $parentSummaryInfo['ad_sale_money_gbp'] ?? 0;
                                $parentSummaryInfo['ad_sale_money_gbp'] += $summaryInfo['ad_sale_money_gbp'];
                                $parentSummaryInfo['ad_sale_money_jpy'] = $parentSummaryInfo['ad_sale_money_jpy'] ?? 0;
                                $parentSummaryInfo['ad_sale_money_jpy'] += $summaryInfo['ad_sale_money_jpy'];
                                $parentSummaryInfo['ad_sale_money_original'] = $parentSummaryInfo['ad_sale_money_original'] ?? 0;
                                $parentSummaryInfo['ad_sale_money_original'] += $summaryInfo['ad_sale_money_original'];
                                $parentSummaryInfo['ad_sale_order_num'] = $parentSummaryInfo['ad_sale_order_num'] ?? 0;
                                $parentSummaryInfo['ad_sale_order_num'] += $summaryInfo['ad_sale_order_num'];
                                $parentSummaryInfo['impressions'] = $parentSummaryInfo['impressions'] ?? 0;
                                $parentSummaryInfo['impressions'] += $summaryInfo['impressions'];
                                $parentSummaryInfo['clicks'] = $parentSummaryInfo['clicks'] ?? 0;
                                $parentSummaryInfo['clicks'] += $summaryInfo['clicks'];
                                $parentSummaryInfo['cost'] = $parentSummaryInfo['cost'] ?? 0;
                                $parentSummaryInfo['cost'] += $summaryInfo['cost'];
                                $parentSummaryInfo['cost_local'] = $parentSummaryInfo['cost_local'] ?? 0;
                                $parentSummaryInfo['cost_local'] += $summaryInfo['cost_local'];
                                $parentSummaryInfo['cost_usd'] = $parentSummaryInfo['cost_usd'] ?? 0;
                                $parentSummaryInfo['cost_usd'] += $summaryInfo['cost_usd'];
                                $parentSummaryInfo['cost_eur'] = $parentSummaryInfo['cost_eur'] ?? 0;
                                $parentSummaryInfo['cost_eur'] += $summaryInfo['cost_eur'];
                                $parentSummaryInfo['cost_gbp'] = $parentSummaryInfo['cost_gbp'] ?? 0;
                                $parentSummaryInfo['cost_gbp'] += $summaryInfo['cost_gbp'];
                                $parentSummaryInfo['cost_jpy'] = $parentSummaryInfo['cost_jpy'] ?? 0;
                                $parentSummaryInfo['cost_jpy'] += $summaryInfo['cost_jpy'];
                            }

                            if(isset($summaryInfo['refund_amount'])){
                                $parentSummaryInfo['refund_amount'] = $parentSummaryInfo['refund_amount'] ?? 0;
                                $parentSummaryInfo['refund_amount'] += $summaryInfo['refund_amount'];
                            }
                            if(isset($summaryInfo['refund_money'])){
                                $parentSummaryInfo['refund_money'] = $parentSummaryInfo['refund_money'] ?? 0;
                                $parentSummaryInfo['refund_money'] += $summaryInfo['refund_money'];
                            }
                            if(isset($summaryInfo['refund_money_local'])){
                                $parentSummaryInfo['refund_money_local'] = $parentSummaryInfo['refund_money_local'] ?? 0;
                                $parentSummaryInfo['refund_money_local'] += $summaryInfo['refund_money_local'];
                            }
                            if(isset($summaryInfo['refund_money_usd'])){
                                $parentSummaryInfo['refund_money_usd'] = $parentSummaryInfo['refund_money_usd'] ?? 0;
                                $parentSummaryInfo['refund_money_usd'] += $summaryInfo['refund_money_usd'];
                            }
                            if(isset($summaryInfo['refund_money_eur'])){
                                $parentSummaryInfo['refund_money_eur'] = $parentSummaryInfo['refund_money_eur'] ?? 0;
                                $parentSummaryInfo['refund_money_eur'] += $summaryInfo['refund_money_eur'];
                            }
                            if(isset($summaryInfo['refund_money_gbp'])){
                                $parentSummaryInfo['refund_money_gbp'] = $parentSummaryInfo['refund_money_gbp'] ?? 0;
                                $parentSummaryInfo['refund_money_gbp'] += $summaryInfo['refund_money_gbp'];
                            }
                            if(isset($summaryInfo['refund_money_jpy'])){
                                $parentSummaryInfo['refund_money_jpy'] = $parentSummaryInfo['refund_money_jpy'] ?? 0;
                                $parentSummaryInfo['refund_money_jpy'] += $summaryInfo['refund_money_jpy'];
                            }
                            if(isset($summaryInfo['return_amount'])){
                                $parentSummaryInfo['return_amount'] = $parentSummaryInfo['return_amount'] ?? 0;
                                $parentSummaryInfo['return_amount'] += $summaryInfo['return_amount'];
                            }
                        }
                        //补充父asin维度的直接数据源数据，其他主要为通过子asin汇总计算出来
                        if(in_array('visit',$fields)){
                            $visitInfoDefault = ['sessions' => 0 ,'page_views' => 0 ,'buy_box_percentage'=>0,
                                'session_percentage' => 0 , 'page_views_percentage' => 0
                            ];
                            $visitInfo = $this->getParentVisit($param);
                            $visitInfo = empty($visitInfo)? $visitInfoDefault:$visitInfo;
                            $parentSummaryInfo['sessions'] = $visitInfo['sessions'];
                            $parentSummaryInfo['page_views'] = $visitInfo['page_views'];
                            $parentSummaryInfo['buy_box_percentage'] = $visitInfo['buy_box_percentage'];
                            $parentSummaryInfo['session_percentage'] = $visitInfo['session_percentage'];
                            $parentSummaryInfo['page_views_percentage'] = $visitInfo['page_views_percentage'];
                        }
                        //重置对象属性
                        $param = [
                            'asin' => $data['asin'],
                            'user_account' => $data['user_account'],
                            'date' => $data['date'],
                            'seller_sku' => '',
                            'site' => $data['site']
                        ];
                        $date = $data['date'];
                        $md5Key =md5( $data['user_account']. $data['asin']);
                        $model = new AmazonListingExtendSummaryLocalModel();
                        $sql = "SELECT lse_id,site,qty,summary_date,
                           sale_amount,sale_amount_usd,sale_amount_eur,sale_amount_gbp,sale_amount_jpy,
                           sale_amount_original,
                           sale_order_num,refund_amount,refund_money,refund_money_usd,
                           refund_money_eur,refund_money_gbp,refund_money_jpy,refund_money_local,key1,return_amount,asin_type
                                 FROM ec_amazon_listing_extend_summary_local 
                                 WHERE key1=?  AND summary_date=?";
                        $preOne =  $model->query($sql, [$md5Key,$date]);
                        $preSummary = isset($preOne[0]['lse_id'])? $preOne[0]:[];
                        $parentSummaryInfo['asin_type'] = 3;
                        $this->save($parentSummaryInfo, $param,$preSummary);
                    }
                }else{
                    $param = [
                        'asin' => $data['asin'],
                        'date' => $data['date'],
                        'site' => $data['site'],
                        'user_account' => $data['user_account'],
                        'seller_sku' => $data['seller_sku']
                    ];
                    $preSummary = [];
                    $summaryInfo = $this->getListingInfo($param,$preSummary);
                    $summaryInfo['asin_type'] = 2;
                    $this->save($summaryInfo, $param,$preSummary);
                }
                Functions::cliEcho('统计完成:'.json_encode($data));
            }catch (\Throwable $e){
                Functions::cliEcho($e->getMessage().$e->getFile().$e->getLine());
                throw $e;
            }
        }
        Functions::cliEcho('消耗队列数据 --zj : -- '. \md5(json_encode($list)));
        //当前进程统计完
        $batchNum = 50;//50一批：单进程独立asin不及50个
        if($this->batchInsertKeys){
            Functions::cliEcho('消耗队列数据 -- 插入: -- ' . count($this->batchInsertKeys) . ' -- ' . \md5(json_encode($list)));
            $data = [];
            foreach ($this->batchInsertKeys as $key => $value){
                $data[] = $value;
                if(count($data) === $batchNum){
                    //执行批量插入，批量插入完毕清除对应的key
                    $this->batchInsert($data);
                    $data = [];
                }
            }
            if($data){
                //执行批量插入，批量插入完毕清除对应的key
                $this->batchInsert($data);
            }
        }
        Functions::cliEcho('消耗队列数据end : -- '. \md5(json_encode($list)));
    }


    public function save($summaryInfo,$param,$preSummary=[]){
        $lockSecond = 5;
        $date = $param['date'] ?? '';
        $asin = $param['asin'] ?? '';
        $companyCode = $param['companyCode'] ?? '';
        $userAccount = $param['user_account'] ?? '';
        $sellerSku = $param['seller_sku'] ?? '';
        $site = $param['site'] ?? '';
        $fields = $this->moduleFields;
        $key1 = md5($userAccount.$asin.$sellerSku);
        $lockKey = 'lock_listing_summary_local_'.$key1.$date;
        $rand = rand(1,100000);
        $redis =  Common::getRedis();
        $redis->multi();
        $redis->setNX($lockKey, $rand);
        $redis->expire($lockKey, $lockSecond);
        $redisResult = $redis->exec();
        if(empty($redisResult[0]) || ($redisResult[0] !== 1)){
            return true;
        }
        $model = new AmazonListingExtendSummaryLocalModel();
        if(!$asin || !$date)
        {
            return true;
        }
        if(!$preSummary){
            $existKey = 'lock_listing_summary_local_exist_'.$key1.$date;
            if($redis->exists($existKey))
            {
                $preOne[0]['lse_id'] = $redis->get($existKey);
            }else{
                if(isset($summaryInfo['asin_type']) && ($summaryInfo['asin_type'] === 3)){
                    $sql = "SELECT lse_id FROM ec_amazon_listing_extend_summary_local 
                 WHERE key1=? AND summary_date=?";
                    $preOne =  $model->query($sql, [$key1, $date]);
                }else{
                    $sql = "SELECT lse_id FROM ec_amazon_listing_extend_summary_local 
                 WHERE key1=?  AND summary_date=?";
                    $preOne =  $model->query($sql, [$key1, $date]);
                }
                if(!empty($preOne[0]['lse_id'])){
                    //如果已存在，则缓存主键id 5天
                    $redis->setex($existKey, 3600*24*5,$preOne[0]['lse_id']);
                }
            }
        }else{
            $preOne[0] = $preSummary;
        }

        $desc = empty($preOne[0])?  '新增':'更新';
        Functions::cliEcho($desc.$asin);
        $data = [];
        if(!empty($preOne[0])){
            if(in_array('order',$fields)){
                $data['qty'] = $summaryInfo['sale_amount'];//总销量
                $data['sale_amount'] =  $summaryInfo['sale_money'];//总销售额
                $data['sale_amount_usd'] =  $summaryInfo['sale_money_usd'];//总销售额
                $data['sale_amount_eur'] =  $summaryInfo['sale_money_eur'];//总销售额
                $data['sale_amount_gbp'] =  $summaryInfo['sale_money_gbp'];//总销售额
                $data['sale_amount_jpy'] =  $summaryInfo['sale_money_jpy'];//总销售额
                $data['sale_amount_original'] = $summaryInfo['sale_money_original'];//销售额（站点原始金额）
                $data['sale_order_num'] =  $summaryInfo['sale_order_num'];//总订单数
            }
            if(in_array('ad_order',$fields)){
                $data['ad_qty'] = $summaryInfo['ad_sale_amount'];//广告销量
                $data['ad_sale_amount'] = $summaryInfo['ad_sale_money'];//广告销售额
                $data['ad_sale_amount_usd'] = $summaryInfo['ad_sale_money_usd'];
                $data['ad_sale_amount_eur'] = $summaryInfo['ad_sale_money_eur'];
                $data['ad_sale_amount_gbp'] = $summaryInfo['ad_sale_money_gbp'];
                $data['ad_sale_amount_jpy'] = $summaryInfo['ad_sale_money_jpy'];
                $data['ad_sale_amount_original'] = $summaryInfo['ad_sale_money_original'];
                $data['ad_sale_order_num'] = $summaryInfo['ad_sale_order_num'];
                $data['impressions'] = $summaryInfo['impressions'];
                $data['clicks'] = $summaryInfo['clicks'];
                $data['cost'] = $summaryInfo['cost'];
                $data['cost_local'] = $summaryInfo['cost_local'];
                $data['cost_usd'] = $summaryInfo['cost_usd'];
                $data['cost_eur'] = $summaryInfo['cost_eur'];
                $data['cost_gbp'] = $summaryInfo['cost_gbp'];
                $data['cost_jpy'] = $summaryInfo['cost_jpy'];
            }
            if(in_array('refund',$fields)){
                $data['refund_amount'] = $summaryInfo['refund_amount'];//退款数
                $data['refund_money'] = $summaryInfo['refund_money'];//退款总额
                $data['refund_money_usd'] = $summaryInfo['refund_money_usd'];//退款总额
                $data['refund_money_eur'] = $summaryInfo['refund_money_eur'];//退款总额
                $data['refund_money_gbp'] = $summaryInfo['refund_money_gbp'];//退款总额
                $data['refund_money_jpy'] = $summaryInfo['refund_money_jpy'];//退款总额
                $data['refund_money_local'] = $summaryInfo['refund_money_local'];//退款总额
            }
            if(in_array('return',$fields)){
                $data['return_amount'] = $summaryInfo['return_amount'];//退货数量
            }
            if(in_array('visit',$fields)){
                $data['sessions'] = $summaryInfo['sessions'];//访客次数
                $data['page_views'] = $summaryInfo['page_views'];//浏览次数
                $data['buy_box_percentage'] = $summaryInfo['buy_box_percentage'];
                $data['session_percentage'] = $summaryInfo['session_percentage'];
                $data['page_views_percentage'] = $summaryInfo['page_views_percentage'];
            }

            if(!$this->checkIsSame($data ,$preSummary )){
                $model->updateByWhere($data,['lse_id'=>$preOne[0]['lse_id']]);
            }
        }else{
            $asinType = isset($summaryInfo['asin_type'])?  $summaryInfo['asin_type'] : 0;
            $data = [
                'qty' => $summaryInfo['sale_amount'] ?? 0,//总销量
                'sale_amount' =>  $summaryInfo['sale_money'] ?? 0,//总销售额
                'sale_amount_usd' =>  $summaryInfo['sale_money_usd'] ?? 0,
                'sale_amount_eur' =>  $summaryInfo['sale_money_eur'] ?? 0,
                'sale_amount_gbp' =>  $summaryInfo['sale_money_gbp'] ?? 0,
                'sale_amount_jpy' =>  $summaryInfo['sale_money_jpy'] ?? 0,
                'sale_amount_original' => $summaryInfo['sale_money_original'] ?? 0,//销售额（站点原始金额）
                'sale_order_num' =>  $summaryInfo['sale_order_num'] ?? 0,//总订单数
                'ad_qty' =>  $summaryInfo['ad_sale_amount'] ?? 0,//总广告销量
                'ad_sale_amount' => $summaryInfo['ad_sale_money'] ?? 0,//总广告销售额
                'ad_sale_amount_usd' => $summaryInfo['ad_sale_money_usd'] ?? 0,
                'ad_sale_amount_eur' => $summaryInfo['ad_sale_money_eur'] ?? 0,
                'ad_sale_amount_gbp' => $summaryInfo['ad_sale_money_gbp'] ?? 0,
                'ad_sale_amount_jpy' => $summaryInfo['ad_sale_money_jpy'] ?? 0,
                'ad_sale_order_num' =>$summaryInfo['ad_sale_order_num'] ?? 0,//总广告订单数
                'ad_sale_amount_original' => $summaryInfo['ad_sale_money_original'] ?? 0,//广告销售额（站点原始金额）
                'cost' =>  $summaryInfo['cost'] ?? 0,
                'cost_local' =>  $summaryInfo['cost_local'] ?? 0,
                'cost_usd' =>  $summaryInfo['cost_usd'] ?? 0,
                'cost_eur' =>  $summaryInfo['cost_eur'] ?? 0,
                'cost_gbp' =>  $summaryInfo['cost_gbp'] ?? 0,
                'cost_jpy' =>  $summaryInfo['cost_jpy'] ?? 0,
                'clicks' =>  $summaryInfo['clicks'] ?? 0,
                'impressions' =>  $summaryInfo['impressions'] ?? 0,
                'sessions' => $summaryInfo['sessions'] ?? 0,//访客次数
                'page_views' => $summaryInfo['page_views'] ?? 0,//浏览次数
                'buy_box_percentage' =>  $summaryInfo['buy_box_percentage'] ?? 0,
                'session_percentage' =>  $summaryInfo['session_percentage'] ?? 0,
                'page_views_percentage' =>  $summaryInfo['page_views_percentage'] ?? 0,
                'refund_amount' => $summaryInfo['refund_amount'] ?? 0,//退货数
                'refund_money' => $summaryInfo['refund_money'] ?? 0,//退款总额
                'refund_money_local' => $summaryInfo['refund_money_local'] ?? 0,
                'refund_money_usd' => $summaryInfo['refund_money_usd'] ?? 0,
                'refund_money_eur' => $summaryInfo['refund_money_eur'] ?? 0,
                'refund_money_gbp' => $summaryInfo['refund_money_gbp'] ?? 0,
                'refund_money_jpy' => $summaryInfo['refund_money_jpy'] ?? 0,
                'site' => $site,
                'return_amount' => $summaryInfo['return_amount'] ?? 0,//退货数量
                'user_account' => $userAccount,
                'seller_sku' => $sellerSku,
                'asin' => $asin,
                'summary_date' =>  $date,
                'asin_type' => $asinType,
                'key1' => $key1,//user_account+asin+seller_sku 的MD5值
               ];
            //插入改为批量插入,设置成功，才会记录为待插入数据
            $batchInsertKey = 'amazon:[summary_local_listing_batch_insert]:['.$companyCode.']:'.$key1.$date;
            $ret = $redis->set($batchInsertKey, json_encode($data), ['NX', 'EX' => 60*15]);//有效期为秒
            if($ret){
                $this->batchInsertKeys[] = $batchInsertKey;
            }
//            $model->add($data);
        }
        $lockValue = $redis->get($lockKey);
        if(strval($lockValue) === strval($rand)){
            $redis->del($lockKey);
        }
    }

    //获取汇总数据
    //目前金额相关的需获取销量信息才能计算出对应汇率
    public  function getListingInfo($param,&$preSummary){
        $date = $param['date'] ?? '';
        $asin = $param['asin'] ?? '';
        $userAccount = $param['user_account'] ?? '';
        $sellerSku = $param['seller_sku'] ?? '';
        $md5Key =md5( $userAccount.$asin.$sellerSku);
        $model = new AmazonListingExtendSummaryLocalModel();
        $sql = "SELECT lse_id,user_account,site,qty,summary_date,
       sale_amount,sale_amount_usd,sale_amount_eur,sale_amount_gbp,sale_amount_jpy,
       sale_amount_original,
       sale_order_num,refund_amount,refund_money,refund_money_usd,
       refund_money_eur,refund_money_gbp,refund_money_jpy,refund_money_local,key1,return_amount,asin_type,
       ad_qty,ad_sale_amount,ad_sale_amount_usd,ad_sale_amount_eur,ad_sale_amount_gbp,
       ad_sale_amount_jpy,ad_sale_order_num,ad_sale_amount_original,cost,
       cost_local,cost_usd,cost_eur,cost_gbp,cost_jpy,clicks,impressions,sessions,
       page_views,buy_box_percentage,session_percentage,page_views_percentage
             FROM ec_amazon_listing_extend_summary_local 
             WHERE key1=?  AND summary_date=?";
        $preOne =  $model->query($sql, [$md5Key,$date]);
        $preSummary = isset($preOne[0]['lse_id'])? $preOne[0]:[];
        $fields = $this->moduleFields;
        $date = $param['date'] ?? '';

        if (!isset($this->currencyOriginal[$userAccount])) {
            $this->currencyOriginal[$userAccount] = UserAccountService::getCurrencyByAccount($userAccount);
        }
        $currencyOriginal = $this->currencyOriginal[$userAccount];

        $currencyLocal = $this->currencyLocal;
        //订单数据
        $orderInfo = [];
        if(in_array('order' , $fields)){
            $orderInfo = $this->getOrder($param);
            $orderInfo['sale_money_currency'] = $currencyLocal;
            $moneyResult = $this->getMoneyFromSiteCurrency($currencyOriginal,$orderInfo['sale_money_original'],$date);
            $orderInfo['sale_money'] = $moneyResult['local'];
            $orderInfo['sale_money_usd'] = $moneyResult['usd'];
            $orderInfo['sale_money_eur'] = $moneyResult['eur'];
            $orderInfo['sale_money_gbp'] = $moneyResult['gbp'];
            $orderInfo['sale_money_jpy'] = $moneyResult['jpy'];
        }
        $adOrderInfo = [];
        if(in_array('ad_order' , $fields)){
            $adOrderInfo = $this->getAdOrder($param);
            $moneyResult = $this->getMoneyFromSiteCurrency($currencyOriginal,$adOrderInfo['ad_sale_money_original'],$date);
            $adOrderInfo['ad_sale_money'] = $moneyResult['local'];
            $adOrderInfo['ad_sale_money_usd'] = $moneyResult['usd'];
            $adOrderInfo['ad_sale_money_eur'] = $moneyResult['eur'];
            $adOrderInfo['ad_sale_money_gbp'] = $moneyResult['gbp'];
            $adOrderInfo['ad_sale_money_jpy'] = $moneyResult['jpy'];

            $moneyResult = $this->getMoneyFromSiteCurrency($currencyOriginal,$adOrderInfo['cost'],$date);
            $adOrderInfo['cost_local'] = $moneyResult['local'];
            $adOrderInfo['cost_usd'] = $moneyResult['usd'];
            $adOrderInfo['cost_eur'] = $moneyResult['eur'];
            $adOrderInfo['cost_gbp'] = $moneyResult['gbp'];
            $adOrderInfo['cost_jpy'] = $moneyResult['jpy'];
        }
        $refundInfo = [];
        if(in_array('refund',$fields)){
            $refundInfo = $this->getAmazonRefundOrder($param);
            $moneyResult = $this->getMoneyFromSiteCurrency($currencyOriginal,$refundInfo['refund_money'],$date);
            $refundInfo['refund_money_local'] = $moneyResult['local'];
            $refundInfo['refund_money_usd'] = $moneyResult['usd'];
            $refundInfo['refund_money_eur'] = $moneyResult['eur'];
            $refundInfo['refund_money_gbp'] = $moneyResult['gbp'];
            $refundInfo['refund_money_jpy'] = $moneyResult['jpy'];
        }
        $returnInfo = [];
        if(in_array('return',$fields)){
            $returnInfo = $this->getReturnOrder($param);
        }
        $visitInfo = [];
        if(in_array('visit',$fields)){
            $visitInfoDefault = ['sessions' => 0 ,'page_views' => 0 ,'buy_box_percentage'=>0,
                'session_percentage' => 0 , 'page_views_percentage' => 0
            ];
            $visitInfo = $this->getVisit($param);
            $visitInfo = empty($visitInfo)?  $visitInfoDefault:$visitInfo;
        }
        $data = array_merge($orderInfo,$adOrderInfo , $refundInfo,$returnInfo,$visitInfo);
        return $data;
    }
    public function getOrder($param){
        $date = $param['date'] ?? '';
        $asin = $param['asin'] ?? '';
        $userAccount = $param['user_account'] ?? '';
        $sellerSku = $param['seller_sku'] ?? '';
        $orderOriginalModel = new AmazonOrderOriginalModel();
        $dateStart = date('Y-m-d 00:00:00',strtotime($date));
        $dateEnd = date('Y-m-d 23:59:59',strtotime($date));
        //订单量，销售量，销售额
        $sql = "SELECT /*+ INL_JOIN(aoo, aod) */ 
          COUNT(DISTINCT aod.amazon_order_id) AS sale_order_num,
          IFNULL(SUM(aod.quantity_ordered), 0) AS sale_amount,
          IFNULL(SUM(aod.item_sale_amount), 0) AS sale_money,
          IFNULL(SUM(aod.item_sale_amount), 0) AS sale_money_original
          FROM
          ec_amazon_order_original aoo  use index(idxx_user_date_local)
          JOIN ec_amazon_order_detail aod ON aoo.aoo_id = aod.aoo_id
        WHERE
          aoo.user_account = ?
          AND aod.asin = ?
          AND aod.seller_sku = ?
          AND aoo.purchase_date_local >= ?
          AND aoo.purchase_date_local <= ?
          AND aoo.order_status != 'Canceled';";
        $result = $orderOriginalModel->query($sql, [$userAccount,$asin,$sellerSku,$dateStart,$dateEnd]);
        $result = empty($result[0])?  []:$result[0];
        return $result;
    }

    //广告先按站点时间冗余统计
    public function getAdOrder($param){
        $date = $param['date'] ?? '';
        $asin = $param['asin'] ?? '';
        $userAccount = $param['user_account'] ?? '';
        $sellerSku = $param['seller_sku'] ?? '';
        $model = new ProductAdProductsReportDailyModel();
        $sql = "SELECT 	IFNULL(SUM(conversions7d_same_sku),0) AS ad_sale_order_num,
                        IFNULL(SUM(units_ordered7d_same_sku),0) AS ad_sale_amount,
                        IFNULL(SUM(sales7d_same_sku), 0) AS ad_sale_money_original,
                        IFNULL(SUM(impressions), 0) AS impressions,
                        IFNULL(SUM(clicks), 0) AS clicks,
                        IFNULL(SUM(cost), 0) AS cost
                       FROM product_ad_products_report_daily
                       WHERE generated_date=? AND asin=? AND sku=? AND user_account=?";
        $result = $model->query($sql, [$date,$asin, $sellerSku,$userAccount]);
        $result = empty($result[0])?  []:$result[0];
        return $result;
    }

    public function getAmazonRefundOrder($param){
        $date = $param['date'] ?? '';
        $asin = $param['asin'] ?? '';
        $userAccount = $param['user_account'] ?? '';
        $sellerSku = $param['seller_sku'] ?? '';
        $site = $param['site'] ?? '';
        //获取listing站点信息//CommonService::siteDateConversionZone();
        $localStart = date('Y-m-d 00:00:00',strtotime($date));
        $start = CommonService::siteDateConversionZone($localStart,'CN','UTC');
        $localEnd = date('Y-m-d 23:59:59',strtotime($date));
        $end = CommonService::siteDateConversionZone($localEnd,'CN','UTC');
        //AmazonV2SettlementDetailModel
        $model = new AmazonV2SettlementDetailModel();
        $sql = "SELECT IFNULL(SUM(quantity_purchased),0) AS refund_amount,
                IFNULL(SUM(amount),0) AS refund_money 
                from ec_amazon_v2_settlement_detail 
                WHERE transaction_type='Refund' AND sku=? AND posted_date_time>=? AND posted_date_time<=? AND user_account=? ";
        $result  = $model->query($sql, [$sellerSku, $start,$end,$userAccount]);
        $result = empty($result[0])? []:$result[0];
        return $result;
    }
    public function getReturnOrder($param){
        $date = $param['date'] ?? '';
        $asin = $param['asin'] ?? '';
        $userAccount = $param['user_account'] ?? '';
        $sellerSku = $param['seller_sku'] ?? '';
        $localStart = date('Y-m-d 00:00:00',strtotime($date));
        $start = CommonService::siteDateConversionZone($localStart,'CN','UTC','Y-m-d\TH:i:s+00:00');
        $localEnd = date('Y-m-d 23:59:59',strtotime($date));
        $end = CommonService::siteDateConversionZone($localEnd,'CN','UTC','Y-m-d\TH:i:s+00:00');
        $model = new FBAFulfillmentCustomerReturnsDataModel();
        $sql = "SELECT IFNULL(sum(quantity),0) as return_amount FROM ec_amazon_fba_fulfillment_customer_returns_data
                WHERE user_account=? AND sku=? AND asin=? 
                AND return_date>=? AND return_date<=?";
        $result  = $model->query($sql, [$userAccount, $sellerSku,$asin,$start,$end]);
        $result = empty($result[0])? []:$result[0];
        return $result;
    }

    public function getVisit($param){
        $date = $param['date'] ?? '';
        $asin = $param['asin'] ?? '';
        $userAccount = $param['user_account'] ?? '';
        $sellerSku = $param['seller_sku'] ?? '';

        $model = new BusinessReportByChildModel();
        $sql = "SELECT IFNULL(sessions,0) as sessions,
                     IFNULL(page_views,0) as page_views ,
                     IFNULL(buy_box_percentage,0) as buy_box_percentage,
                     IFNULL(session_percentage,0) as session_percentage,
                     IFNULL(page_views_percentage,0) as page_views_percentage
                from ec_amazon_business_report_by_child
                WHERE generate_date=? AND child_asin=? AND seller_sku=? AND user_account=? 
                      limit 1";
        $result  = $model->query($sql, [$date,$asin,$sellerSku,$userAccount]);
        $result = empty($result[0])? []:$result[0];
        return $result;
    }
    public function getParentVisit($param){
        $date = $param['date'] ?? '';
        $asin = $param['asin'] ?? '';
        $userAccount = $param['user_account'] ?? '';

        $model = new BusinessReportByParentModel();
        $sql = "SELECT IFNULL(sessions,0) as sessions,
                     IFNULL(page_views,0) as page_views ,
                     IFNULL(buy_box_percentage,0) as buy_box_percentage,
                     IFNULL(session_percentage,0) as session_percentage,
                     IFNULL(page_views_percentage,0) as page_views_percentage
                from ec_amazon_business_report_by_parent
                WHERE generate_date=?  AND parent_asin=? AND user_account=? 
                      limit 1";
        $result  = $model->query($sql, [$date,$asin,$userAccount]);

        $result = empty($result[0])? []:$result[0];
        return $result;
    }
    /***
     * @param $key
     * @return array|mixed
     * //获得需要统计的模块
     */
    private function getFields($key){
        $msg = [
            '*' => ['order','refund','return'],
            'a' => ['order','refund','return','ad_order','visit'],
            'b' => ['order'],
            'c' => ['ad_order'],
            'd' => ['return'],
            'e' => ['refund'],
        ];
        return isset($msg[$key])? $msg[$key]:[];
    }
    public function batchInsert($keys){
        $redis =  Common::getRedis();
        $insertData  = [];
        //拼接数据
        foreach ($keys as $k => $v){
            $dataStr = $redis->get($v);
            $data = json_decode($dataStr,true);
            if($data){
                $insertData[] = $data;
            }
        }
        //批量插入
        if($insertData){
            $model = new AmazonListingExtendSummaryLocalModel();
            $model->addMulti($insertData);
        }
        //清除缓存键
        foreach ($keys as $k => $v){
            $redis->del($v);
        }
    }
    public function getMoneyFromSiteCurrency($currency,$money,$date){
        // $currencyLocal = CurrencyService::getCurrencyLocal();
        $currencyLocal = $this->currencyLocal;
        $data = [
            'local' => 0,
            'usd' => 0,
            'eur' => 0,
            'gbp' =>  0,
            'jpy' => 0,
        ];
        if($currency && is_numeric($money) && ($money <> 0) && $date){
            $newRates = CurrencyService::getRates($date, $currency);//汇率设置使用北京时间
            if($currency === $currencyLocal){
                $localMoney = $money;
            }else{
                $rate = isset($newRates[$currencyLocal]) ? $newRates[$currencyLocal] : 0;
                if($rate == 0){
                    try {
                        $rate = CurrencyService::getRate($date, $currency, $currencyLocal);
                    }catch (\Exception $e){
                        Logs::info('saveListingLocalSummary_err',['currency'=>$currency,
                            'currencyLocal'=>$currencyLocal,'date'=>$date],'未获取到本位币种');
                        Functions::cliEcho('未获取到本位币种：'.$currency.$currencyLocal.$date.'的汇率。');
                    }
                }
                $localMoney = $rate > 0 ? bcdiv($money, $rate, 2):0;
            }


            if($currency === 'USD'){
                $usdMoney = $money;
            }else{
                $usdRate = isset($newRates['USD']) ? $newRates['USD'] : 0;
                if($usdRate == 0){
                    try {
                        $usdRate = CurrencyService::getRate($date, $currency, 'USD');
                    }catch (\Exception $e){
                        Logs::info('saveListingLocalSummary_err',['currency'=>$currency,
                            'currencyLocal'=>$currencyLocal,'date'=>$date],'未获取到USD币种汇率');
                        Functions::cliEcho('未获取到本位币种：'.$currency.$currencyLocal.$date.'的汇率。');
                    }
                }
                $usdMoney = $usdRate>0? bcdiv($money,$usdRate,2):0;
            }

            if($currency === 'EUR'){
                $eurMoney = $money;
            }else{
                $eurRate = isset($newRates['EUR']) ? $newRates['EUR'] : 0;
                if($eurRate == 0){
                    Functions::cliEcho('未获取到币种：EUR'.$date.'的汇率。');
                }
                $eurMoney = $eurRate>0? bcdiv($money,$eurRate,2):0;
            }

            if($currency === 'GBP'){
                $gbpMoney = $money;
            }else{
                $gbpRate = isset($newRates['GBP']) ? $newRates['GBP'] : 0;
                if($gbpRate == 0){
                    Functions::cliEcho('未获取到币种：GBP'.$date.'的汇率。');
                }
                $gbpMoney = $gbpRate>0? bcdiv($money,$gbpRate,2):0;
            }

            if($currency === 'JPY'){
                $jpyMoney = $money;
            }else{
                $jpyRate = isset($newRates['JPY']) ? $newRates['JPY'] : 0;
                if($jpyRate == 0){
                    Functions::cliEcho('未获取到币种：JPY'.$date.'的汇率。');
                }
                $jpyMoney = $jpyRate>0? bcdiv($money,$jpyRate,2):0;
            }

            $data['local'] = bcadd($localMoney,0,2);
            $data['usd']  = $usdMoney;
            $data['eur']  = $eurMoney;
            $data['gbp']  = $gbpMoney;
            $data['jpy']  = $jpyMoney;
        }
        return $data;
    }

    //对比待更新数据是否一致,一致则不做更新操作
    public function checkIsSame($currentData , $preData){
        $result  = true;
        if($preData){
            foreach ($currentData as $k => $v){
                if(isset($preData[$k])){
                    $preCol = '';
                    if($preData[$k]){
                        if(is_numeric($preData[$k])){
                            $preCol = round($preData[$k]*10000);
                        }else{
                            $preCol = strval($preData[$k]);
                        }
                    }
                    $currentCol = '';
                    if($currentData[$k]){
                        if(is_numeric($currentData[$k])){
                            $currentCol = round($currentData[$k]*10000);
                        }else{
                            $currentCol = strval($currentData[$k]);
                        }
                    }
                    if(($preCol !== $currentCol) && ($currentCol || $preCol) ){
                        $result = false;
                        break;
                    }
                }
            }
        }else{
            $result  = false;
        }

        return $result;
    }
}