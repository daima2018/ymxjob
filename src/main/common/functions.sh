#!/bin/bash
#############################################################
#函数封装
#############################################################

#获取脚本参数
getparam(){
    arg=$1
    echo $2 |xargs -n1|cut -b 2- |awk -F '=' '{if($1=="'"$arg"'") print $2}'
}

#时间特殊处理
getdate(){
    arg=$1
    if [ "${arg:0:1}" = "T" ]    #如果是T开头,特殊处理
    then
        if [ "${arg:1}" = "" ]   #去掉T
          then arg=`date "+%Y-%m-%d"`
        else arg=`date -d "${arg:1} days" "+%Y-%m-%d"`
        fi
    fi
    echo $arg
}