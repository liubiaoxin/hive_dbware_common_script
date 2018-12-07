#!/bin/sh

#装载业务数据到hive
function  load_bussiness_data_to_hive(){
	#business_type
	business_type=$1
	echo "load_bussiness_data_to_hive:business_type=$business_type"
	
	#schema
	schema=$2
	echo "load_bussiness_data_to_hive:schema=$schema"
	
	#table_name
	table_name=$3
	echo "load_bussiness_data_to_hive:table_name=$table_name"
	
	#s_date
	s_date=$4
	echo "load_bussiness_data_to_hive:s_date=$s_date"

	#删除已存在的表分区、目录
	echo "hive -e \"alter table ${schema}.$table_name drop IF EXISTS partition(dt='${s_date}');\""
	hive -e "alter table ${schema}.$table_name drop IF EXISTS partition(dt='${s_date}');"
	hdfs dfs -rm -r  /qding/db/${schema}/$table_name/dt=$s_date
	
	#创建分区目录
	hdfs dfs -mkdir -p   /qding/db/${schema}/$table_name/dt=$s_date
	echo "===================================mkdir"

	#采集业务库数据
	nameservice=`get_ha_active_ip`
	echo "nameservice========$nameservice"
	echo "/data/datax/bin/datax.py    -p \"-Ddt=$s_date -Dnameservice=$nameservice\"  /data/qding_etl/bash/${schema}/$business_type/json/$table_name.json
	"
	/data/datax/bin/datax.py    -p "-Ddt=$s_date -Dnameservice=$nameservice"  /data/qding_etl/bash/${schema}/$business_type/json/$table_name.json

	 
	#加载分区
	echo "hive -e \"alter table ${schema}.$table_name add partition (dt='$s_date') location '/qding/db/${schema}/$table_name/dt=$s_date';\""
	hive -e "alter table ${schema}.$table_name add partition (dt='$s_date') location '/qding/db/${schema}/$table_name/dt=$s_date';"
	
	#刷新impala元数据
	#impala-shell -i 10.37.5.115 -q "INVALIDATE METADATA; REFRESH ${schema}.$table_name;" 
	impala-shell -i BJ-HOST-115 -q "REFRESH ${schema}.$table_name;"
}

#通过sql insert方式加载hive表数据到不同shema层
function  load_hive_data_to_schema(){
	#business_type
	business_type=$1
	echo "load_hive_data_to_schema:business_type=$business_type"
	
	#schema
	schema=$2
	echo "load_hive_data_to_schema:schema=$schema"
	
	#table_name
	table_name=$3
	echo "load_hive_data_to_schema:table_name=$table_name"
	
	#s_date
	s_date=$4
	echo "load_hive_data_to_schema:s_date=$s_date"
	
	#m_date
	m_date=$5
	echo "load_hive_data_to_schema:m_date=$m_date"

	#删除已存在的表分区、目录
	#echo "hive -e \"alter table ${schema}.$table_name drop IF EXISTS partition(dt='${s_date}');\""
	#hive -e "alter table ${schema}.$table_name drop IF EXISTS partition(dt='${s_date}');"
	#hdfs dfs -rm -r  /qding/db/${schema}/$table_name/dt=$s_date

	#替换s_date成具体数据日期
	sed "s/s_date/${s_date}/g" /data/qding_etl/sql/${schema}/${business_type}/${table_name}.sql >/data/qding_etl/sql/${schema}/${business_type}/${table_name}.sql.tmp
	sed -i "s/m_date/${m_date}/g"  /data/qding_etl/sql/${schema}/${business_type}/${table_name}.sql.tmp

	#执行hive sql文件
	echo "hive  -f  /data/qding_etl/sql/${schema}/${business_type}/${table_name}.sql.tmp"
	hive  -f  /data/qding_etl/sql/${schema}/${business_type}/${table_name}.sql.tmp
	
	#刷新impala元数据
	#impala-shell -i 10.37.5.115 -q "INVALIDATE METADATA; REFRESH ${schema}.$table_name;" 
	impala-shell -i BJ-HOST-115 -q "REFRESH ${schema}.$table_name;"


}

#获取hadoop的active node
function  get_ha_active_ip(){
  namenode_list=("namenode957-10.50.8.113:8020" "namenode953-10.50.8.112:8020")

  for line  in ${namenode_list[@]}; do
    array=(${line//-/ })
    namenode="${array[0]}"

    status=`sudo -u root sudo -u hdfs hdfs haadmin -getServiceState  $namenode`
    if [ "$status" = "active" ]; then
       active_ip="${array[1]}"
       echo  "$active_ip"
    fi

 done
}


#load_bds_data_to_mds  "qding_luopan" "qding_mds" "mds_s_mdm_bd_region" "2018-06-24"

#抽取ads层数到业务库
function  load_ads_data_to_db(){
	#business_type
	business_type=$1
	echo "load_ads_data_to_db:business_type=$business_type"
	
	#schema
	schema=$2
	echo "load_ads_data_to_db:schema=$schema"
	
	#table_name
	table_name=$3
	echo "load_ads_data_to_db:table_name=$table_name"
	
	#s_date
	s_date=$4
	echo "load_ads_data_to_db:s_date=$s_date"
	
	#datax_home path
	datax_home=/data/datax/
	
	#json file path
	basepath=/data/qding_etl/bash/$schema/$business_type/json

	#active_namenode
	active_namenode=$(get_ha_active_ip)
	echo "active_namenode=======$active_namenode"

	run_job=$basepath/$table_name.json
	echo "run_job=======${run_job}"
	
	#data_path
	dt_path=/qding/db/$schema/$table_name/dt=$s_date
	echo "data_path=======$dt_path"
	
	#judge path  exist
	hadoop  fs -test -e  $dt_path
	if [ $? -eq 0 ] ;then
		echo "path  exist:$dt_path"
		file_num=`hdfs dfs -count -q $dt_path | awk '{print $6}'`
		echo "file_num=======$file_num"
		#judge file  num 
		if [ $file_num -gt 0 ];then
			echo "cd $datax_home;./bin/datax.py ${run_job} -p\"-Ddt='${s_date}' -Dnameservice='$active_namenode'\""
			cd $datax_home;./bin/datax.py ${run_job} -p"-Ddt='${s_date}' -Dnameservice='$active_namenode'" 
		else
			echo "Skip! Directory{$dt_path} file_num is $file_num,less then  1,don't need load data to mysql!!!"
		fi
	else
			echo "Error! Directory{$dt_path} is not exist，don't need load data to mysql!!!"
	fi
}
