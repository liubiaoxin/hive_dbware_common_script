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

#抽取bds层数据到mds
function  load_bds_data_to_mds(){
	#business_type
	business_type=$1
	echo "load_bds_data_to_mds:business_type=$business_type"
	
	#schema
	schema=$2
	echo "load_bds_data_to_mds:schema=$schema"
	
	#table_name
	table_name=$3
	echo "load_bds_data_to_mds:table_name=$table_name"
	
	#s_date
	s_date=$4
	echo "load_bds_data_to_mds:s_date=$s_date"
	
	#m_date
	m_date=$5
	echo "load_bds_data_to_mds:m_date=$m_date"
	
	#partition_name
	partition_name=$6
	partition_name=${partition_name:='dt'}
	echo "load_bds_data_to_mds:partition_name=$partition_name"
	

	#删除已存在的表分区、目录
	#echo "hive -e \"alter table ${schema}.$table_name drop IF EXISTS partition(dt='${s_date}');\""
	#hive -e "alter table ${schema}.$table_name drop IF EXISTS partition(dt='${s_date}');"
	#hdfs dfs -rm -r  /qding/db/${schema}/$table_name/dt=$s_date

	#替换s_date成具体数据日期
	temp_sql=/data/qding_etl/sql/${schema}/${business_type}/${table_name}.sql.tmp
	sed "s/s_date/${s_date}/g" /data/qding_etl/sql/${schema}/${business_type}/${table_name}.sql >$temp_sql
	
	sed -i "s/m_date/${m_date}/g"  $temp_sql
	
	#使用tez引擎
	sed "1 iset hive.exec.compress.output=true;" -i $temp_sql
	sed "1 iset mapred.output.compress=true;" -i $temp_sql
	sed "1 iset mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;" -i $temp_sql	
	
	sed "1 iset hive.vectorized.execution.enabled = false;" -i $temp_sql
	sed "1 iset hive.execution.engine = tez;" -i $temp_sql
	sed "1 iset tez.queue.name=root.bigdata;" -i $temp_sql
	
	#处理包含union的sql，导致tez产生子目录问题，执行完sql之后，在overwrite一下
	grep -i "union"  $temp_sql
	if [ $? -ne 0 ]; then
	     echo "$temp_sql do not contain 'union',skip!!!!"
	else
	
	     echo "$temp_sql contain 'union',special deal with sql!!!"
		 
		 partition_name=$(echo `hive -e "show create table  ${schema}.${table_name};"  | xargs` | awk '{match($0,/(.*PARTITIONED BY.*\(.*`(.*)`.*string.*\).*)/,b);print b[2]}');
		 echo "${schema}.${table_name} partition_name======$partition_name"

		 if [ "$partition_name" = "year_month" ]; then
		    s_date=${s_date:0:7}
		 fi
		 
		 partition_sql_str=""
		 where_sql_str=""
		 fields_sql_str="*"
		 if [ "$partition_name" = "" ]; then
               echo "partition_name  is null!!!!"
		 else 
		    partition_sql_str="PARTITION(${partition_name}='${s_date}')"
			where_sql_str="where ${partition_name}='${s_date}'"
			fields_sql_str="\`(${partition_name})\?\+\.\+\`"
         fi
		 
		 insert_sql="INSERT OVERWRITE TABLE ${schema}.${table_name} $partition_sql_str select $fields_sql_str from ${schema}.${table_name} $where_sql_str;"
		 echo "insert_sql===================$insert_sql"

	     sed "$ a;" -i $temp_sql
		 sed "$ aset hive.support.quoted.identifiers=None;" -i $temp_sql
		 sed "$ a$insert_sql" -i $temp_sql
		  
	fi

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
	
	#partition_name
	partition_name=$(echo `hive -e "show create table  ${schema}.${table_name};"  | xargs` | awk '{match($0,/(.*PARTITIONED BY.*\(.*`(.*)`.*string.*\).*)/,b);print b[2]}');
	echo "load_ads_data_to_db:${schema}.${table_name} partition_name======$partition_name"
	
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
	dt_path=/qding/db/$schema/$table_name/$partition_name=$s_date
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
