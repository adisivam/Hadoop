#!/bin/bash

##############################################################################
#Author         : Kesav Adithya
#Program        : customer_load.sh
#Desc           : Import data from SMDW Customer Dimension to HDFS Hive table
#Version        : 1.0
##############################################################################

set -x

CURRDIR=`pwd`
TIMESTAMP=`date +'%y%m%d_%H%M%S'`
STRT_TIME=`date +'%Y-%m-%d %H:%M:%S'`
logfile=${CURRDIR}/"DDS_CUSTOMER_IMPORT_SQOOP_${TIMESTAMP}.log"
SRC_DB='DDSVD'
SRCTABLE="CUSTOMER_DL"
TGTTABLE="DDS_CUSTOMER_RAW"
TGTVW="DDS_CUSTOMER_VW"
FNLTABLE="DDS_CUSTOMER"
HIVEDB="MASTER_DATA"
ERR_DSC=""
AUDIT_TABLE="MASTER_DATA_AUDIT"
JOB_NAME=$(basename $0)
DATA_PROV_NAME="SMDW"
DATA_SET="CUSTOMER"

echo "--------------------------------------------------------------" | tee -a $logfile
echo "  Start Executing of Script to Import Customer Data to HDFS" | tee -a $logfile
echo "--------------------------------------------------------------" | tee -a $logfile

echo -e "\nVariables Assignment" | tee -a $logfile
echo "Log File Name: ${logfile}" | tee -a $logfile
echo "Teradata Source Table Name: ${SRCTABLE}" | tee -a $logfile
echo "Hive Target Table Name: ${TGTTABLE}" | tee -a $logfile
echo "Hive Final Table Name: ${FNLTABLE}" | tee -a $logfile
echo -e "View Query:\n${VWQRY}"

echo -e "\n----- Step 1: Hive Database and Table Check -----" | tee -a $logfile
echo "Starting Hive database validation ..." | tee -a $logfile

if [ -z "$HIVEDB" ]; then
		echo "The Hive Database is not provided. The default database will be used" | tee -a $logfile
		echo "Proceeding with next steps ..." | tee -a $logfile
else
		hive -e "use ${HIVEDB}"
		if [ $? != 0 ]; then
				echo "The hive database ${HIVEDB} doesn't exist. The database will be created" | tee -a $logfile
				hive -e "create database ${HIVEDB}"
		else
				echo "The hive database ${HIVEDB} exists. Proceeding with next steps ..." | tee -a $logfile
		fi
fi

if [ -z "$HIVEDB" ]; then
		TGTTABLE=${TGTTABLE}
		TGTVW=${TGTVW}
		FNLTABLE=${FNLTABLE}
		AUDIT_TABLE=${AUDIT_TABLE}
else
        TGTTABLE=${HIVEDB}.${TGTTABLE}
		TGTVW=${HIVEDB}.${TGTVW}
		FNLTABLE=${HIVEDB}.${FNLTABLE}
		AUDIT_TABLE=${HIVEDB}.${AUDIT_TABLE}
fi

PROCESS_ID=`hive -e "select case when max(process_id) is null then 0 else max(process_id)+1 end as process_id  from ${AUDIT_TABLE}"`
PROCESS_DESC="CUSTOMER SQOOP IMPORT"

VWQRY="CREATE VIEW ${TGTVW} AS SELECT A.* FROM ${TGTTABLE} A INNER JOIN (SELECT CUSTOMER_ID, MAX(MODIFIED_DATE) AS MOD_DATE FROM ${TGTTABLE} GROUP BY CUSTOMER_ID) B ON A.CUSTOMER_ID = B.CUSTOMER_ID AND A.MODIFIED_DATE = B.MOD_DATE"

hive -e "SELECT * FROM ${TGTTABLE} LIMIT 1"

if [ $? != 0 ]; then
        echo "${TGTTABLE} table doesn't exists in Hive. This may be first run and modified date should be provided as parameter" | tee -a $logfile
        if [ "$1" != "" ]; then
                max_modified_date=$1
        else
                ERR_DSC="Error: Modified date should be provided as parameter for first run. Please provide the same if this is first run..." 
				echo "${ERR_DSC}" | tee -a $logfile
				END_TIME=`date +'%Y-%m-%d %H:%M:%S'`
				STATUS="FAILED"
				hive -e "insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',0,0,0,'${STATUS}','${ERR_DSC}')"
				exit 1
        fi
else
        echo "${TGTTABLE} exists in Hive. Proceeding with below steps..." | tee -a $logfile
fi

echo -e "\n----- Step 2: Last Maximum Modified Date Calculation ------" | tee -a $logfile
echo "Calculating Max Modified Date ..." | tee -a $logfile

if [ "$1" != "" ]; then
   max_modified_date=$1
else
   max_modified_date=`hive -e "select cast(max(modified_date) as timestamp) from ${TGTTABLE}"`
fi

echo -e  "Last Maximum Modified Date: ${max_modified_date}" | tee -a $logfile
echo "Last Max Modified Date is found" | tee -a $logfile

echo -e "\n----- Step 3: Teradata Validation ------" | tee -a $logfile
echo "Getting count of records from Teradata ..." | tee -a $logfile

teraqry=`sqoop eval -Dhadoop.security.credential.provider.path=jceks://hdfs/keystore/teradatad.password.jceks --connect "jdbc:teradata://10.72.23.136/DATABASE=DDSVD" --username DDSVAWSSQHUMDLREADD --password-alias teradatad.password.alias --query "select count(*) from ${SRCTABLE} where MODIFIED_DATE>'${max_modified_date}'"`
teracnt=`echo $teraqry | cut -d'|' -f4`
teracnt=`echo $teracnt | tr -s " "`

if [ "${teracnt}" -eq 0 ]; then
        ERR_DSC="Message: No new records found in Teradata table $SRCTABLE since last modified date"
		echo "${ERR_DSC}" | tee -a $logfile
        END_TIME=`date +'%Y-%m-%d %H:%M:%S'`
		STATUS="SUCCEEDED"
		
		srctablecnt=`sqoop eval -Dhadoop.security.credential.provider.path=jceks://hdfs/keystore/teradatad.password.jceks --connect "jdbc:teradata://10.72.23.136/DATABASE=DDSVD" --username DDSVAWSSQHUMDLREADD --password-alias teradatad.password.alias --query "select count(*) from ${SRCTABLE}"`
		srctablecnt=`echo $srctablecnt | cut -d'|' -f4`
		srctablecnt=`echo $srctablecnt | tr -s " "`

		hive  -e "SELECT * FROM ${FNLTABLE} LIMIT 1"
		if [ $? != 0 ]; then
				echo "${FNLTABLE} doesn't exist in Hive. The count will be populated as 0 in audit table" | tee -a $logfile
		else
				echo "${FNLTABLE} exists in Hive. Calculating the count of records..." | tee -a $logfile
				tgttablecnt=`hive -e "select count(*) from ${FNLTABLE}"`
		fi
		hive -e "insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',${teracnt},${srctablecnt},${tgttablecnt},'${STATUS}','${ERR_DSC}')"
		exit 0
else
		echo "Total number of incremental records to be inserted: $teracnt" | tee -a $logfile
fi

echo -e "\n----- Step 4: Sqoop Command Execution -----" | tee -a $logfile
echo "Starting Sqoop Command Execution ..." | tee -a $logfile

sqoop import -Dhadoop.security.credential.provider.path=jceks://hdfs/keystore/teradatad.password.jceks --connect "jdbc:teradata://10.72.23.136/DATABASE=DDSVD" --username DDSVAWSSQHUMDLREADD --password-alias teradatad.password.alias --table ${SRCTABLE} --where "MODIFIED_DATE>'${max_modified_date}'" -m 1 --split-by customer_id --hive-import --hive-table ${TGTTABLE}  --compress --map-column-hive CUSTOMER_ID=STRING

if [ $? == 0 ]; then
		echo -e "\nImport has been completed successfully" | tee -a $logfile
		echo "Total Number of source records read from ${SRCTABLE}: $teracnt" | tee -a $logfile
		tgtcnt=`hive -e "select count(*) from ${TGTTABLE} where modified_date>'${max_modified_date}'"`
		echo "Total Number of target records inserted in ${TGTTABLE}: $tgtcnt" | tee -a $logfile
else 
		ERR_DSC="Sqoop Command Failed. Please see sqoop log for more details"
		echo "${ERR_DSC}" | tee -a $logfile
		END_TIME=`date +'%Y-%m-%d %H:%M:%S'`
		STATUS="FAILED"
		hive -e "insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',0,${teratotcnt},${teracnt},'${STATUS}','{ERR_DSC}')"
		exit 1
fi

echo -e "\n----- Step 5: Validation Checks and audit table population" | tee -a $logfile
echo "Starting validation checks ..." | tee -a $logfile
tgtvwcnt=`hive -e "select count(*) from $TGTVW"`
if [ $? == 0 ]; then
		echo "Total Number of records in ${SRCTABLE} in Teradata: ${srctablecnt}" | tee -a $logfile
		echo "Total Number of records in ${TGTVW} in Hive: ${tgtvwcnt}" | tee -a $logfile
else
		echo "The view ${TGTVW} doesn't exist in Hive. Creating the same..." | tee -a $logfile
		hive -e "${VWQRY}"
		tgtvwcnt=`hive -e "select count(*) from $TGTVW"`
		echo "Total Number of records in ${SRCTABLE} in Teradata: ${srctablecnt}" | tee -a $logfile
		echo "Total Number of records in ${TGTVW} in Hive: ${tgtvwcnt}" | tee -a $logfile
fi

if [ "${srctablecnt}" == "${tgtvwcnt}" ]; then
	echo -e "\nThe count of records matches between ${SRCTABLE} in Teradata and ${TGTVW} in Hive" | tee -a $logfile
else
	echo -e "\nThe count of records doesn't match between ${SRCTABLE} in Teradata and ${TGTVW} in Hive" | tee -a $logfile
	echo "Please check for the log for more details. Skipping Final table creation process..." | tee -a $logfile
	ERR_DSC="Error: The count of records in Teradata and Hive is not matching"
	END_TIME=`date +'%Y-%m-%d %H:%M:%S'`
	hive -e "insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',${teracnt},${srctablecnt},${tgtvwcnt},'${STATUS}','{ERR_DSC}')"
	exit 1
fi 

echo -e "\n----- Step 5: Final Table Creation Process " | tee -a $logfile
echo "Dropping ${FNLTABLE} in Hive ..." | tee -a $logfile
hive -e "drop table ${FNLTABLE}"
if [ $? == 0 ]; then
		echo "The table ${FNLTABLE} has been dropped" | tee -a $logfile
		echo "Creating table ${FNLTABLE} with new dataset" | tee -a $logfile
		hive -e "create table ${FNLTABLE} as select * from ${TGTVW}"
		echo "The table ${FNLTABLE} has been re-created successfully" | tee -a $logfile
		tgtvwcnt=`hive -e "select count(*) from $FNLTABLE"`
else
		ERR_DSC="Error: The table ${FNLTABLE} doesn't get created. Please check the log for more details" 
		echo "$ERR_DSC" | tee -a $logfile
		END_TIME=`date +'%Y-%m-%d %H:%M:%S'`
		STATUS="FAILED"
		hive -e "insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',${teracnt},${srctablecnt},${tgtvwcnt},'${STATUS}','')"
		exit 1
fi

echo -e "\n----- Step 6: Audit Table Insertion" | tee -a $logfile

END_TIME=`date +'%Y-%m-%d %H:%M:%S'`
STATUS="SUCCEEDED"
hive -e "insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',${teracnt},${srctablecnt},${tgtvwcnt},'${STATUS}','')"

echo -e "\n--------------------------------------------------------------" | tee -a $logfile
echo "  Data from $SRCTABLE has been imported successfully to $FNLTABLE" | tee -a $logfile
echo "  Script Execution has been completed Successfully" | tee -a $logfile
echo "--------------------------------------------------------------" | tee -a $logfile
