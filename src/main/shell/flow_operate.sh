#!/bin/bash
#############################################################
#名称   循环修改flow脚本
#############################################################

source /d/projects/ymxjob/src/main/common/functions.sh

#获取脚本参数
opts=$@

#解析脚本参数
start_date=`getparam start_date "$opts"`
end_date=`getparam end_date "$opts"`
script_name=`getparam script_name "$opts"`

#需要修改的公司
companys="A20040002 A20030001 A20060010 A20060019 A20060020 A20060021 A20060022 A20060024 A20060009 A20060011 A20060012 A20050008 A20040003 A20040004 A20050005 A20050006 A20060015 A20060016 A20060017 A20060018 A20060026 A20060027 A20070035 A20070036 A20070037 A20070038 A20050007 A20060013 A20060014 A20060023 A20060025 A20060028 A20060029 A20070030 A20070031 A20070032 A20070033 A20070034 A20070039 A20070040 A20070041 A20070042 A20070043 A20070044 A20070045 A20070046 A20070047 A20070048 A20080049 A20080050 A20080051 A20080052 A20080053 A20080054 A20080055 A20080056 A20080057 A20080058 A20080059 A20080060"

echo "循环开始时间:"`date "+%Y-%m-%d %H:%M:%S"`

for i in $companys
do
    echo "------公司:$i"
    #修改调度时间
    curl -k -d ajax=scheduleCronFlow -d projectName=$i -d flow=$i --data-urlencode cronExpression="0 40 14,16,18 ? * *" -b "azkaban.browser.session.id=79664c24-eeff-4483-9216-4233f6b5f73d" http://datamg1:8085/schedule
done

echo "循环结束时间:"`date "+%Y-%m-%d %H:%M:%S"`

#如果执行失败就退出
if [ $? -ne 0 ];then
     echo "[ERROR] execute failed!"
     exit $?
fi