#!/bin/bash
#############################################################
#名称   解析账号脚本
#############################################################

getpassword(){
    arg=$1
    result=`php -r "echo openssl_decrypt($arg, 'des-ecb', 'AMAZON');"`
    echo $result
}

src_name=/home/ecm/ymx/conf/account.txt
tmp_name=/home/ecm/ymx/conf/tmp-conf.sh
target_name=/home/ecm/ymx/conf/sqoop-job-conf.sh

#清空文件内容,重新加载账号信息
echo '#!/bin/bash' > $tmp_name
echo "" >> $tmp_name

for line in `cat $src_name`
do
    a=`echo $line|awk -F "#=#" '{print $1}'`
    b=`echo $line|awk -F "#=#" '{print $2}'`
    if [ "${a:0-8:8}" = "password" ] 
        then 
            echo $a="\""`getpassword $b|sed 's/\$/\\\\\$/g'`"\"" >> $tmp_name
            echo "" >> $tmp_name
    else echo $a=$b >> $tmp_name   
    fi    
done

#内容覆盖
cp $tmp_name $target_name
xsync $target_name