#!/bin/bash

##############################################################################
#Author         : Kesav Adithya
#Program        : customer_load.sh
#Desc           : Import data from SMDW Customer Dimension to HDFS Hive table
#Version        : 1.0
##############################################################################

set +x

CURRDIR=`pwd`
PARMFILE=/home/venkikx/sqoop_scripts/SQOOP_IMPORT_CUSTOMER.prm
TIMESTAMP=`date +'%y%m%d_%H%M%S'`
STRT_TIME=`date +'%Y-%m-%d %H:%M:%S'`
SRC_DB=`cat $PARMFILE | grep "SRC_DB" | cut -d "=" -f 2`
SRCTABLE=`cat $PARMFILE | grep "SRCTABLE" | cut -d "=" -f 2`
TGTTABLE=`cat $PARMFILE | grep "TGTTABLE=" | cut -d "=" -f 2`
TGTVW=`cat $PARMFILE | grep "TGTVW=" | cut -d "=" -f 2`
FNLTABLE=`cat $PARMFILE | grep "FNLTABLE=" | cut -d "=" -f 2`
HIVEDB=`cat $PARMFILE | grep "HIVEDB" | cut -d "=" -f 2`
ERR_DSC=""
AUDIT_TABLE=`cat $PARMFILE | grep "AUDIT_TABLE" | cut -d "=" -f 2`
JOB_NAME=$(basename $0)
DATA_PROV_NAME=`cat $PARMFILE | grep "DATA_PROV_NAME" | cut -d "=" -f 2`
DATA_SET=`cat $PARMFILE | grep "DATA_SET" | cut -d "=" -f 2`

echo "--------------------------------------------------------------"
echo "  Start Executing of Script to Import Customer Data to HDFS"
echo "--------------------------------------------------------------"

echo -e "\nDisplaying Variables Assignment"
echo "Teradata Database: ${SRC_DB}"
echo "Teradata Source Table Name: ${SRCTABLE}"
echo "Hive Target Table Name: ${TGTTABLE}"
echo "Hive Final Table Name: ${FNLTABLE}"
echo "Hive View Name: ${TGTVW}"
echo "Hive Database: ${HIVEDB}"
echo "Audit table Name: ${AUDIT_TABLE}"
echo "Data source provider name: ${DATA_PROV_NAME}"
echo "Data set: ${DATA_SET}"

echo -e "\n----- Step 1: Hive Database and Table Check -----"
echo "INFO: Starting Hive database validation ..."

if [ -z "$HIVEDB" ]; then
                echo "INFO: The Hive Database is not provided. The default database will be used"
                echo "INFO: Proceeding with next steps ..."
else
                echo "INFO: Executing hive command: hive -e \"use ${HIVEDB}\""
                                hive -e "use ${HIVEDB}"
                if [ $? != 0 ]; then
                                echo "INFO: The hive database ${HIVEDB} doesn't exist. The database will be created"
                                hive -e "create database ${HIVEDB}"
                else
                                echo "INFO: The hive database ${HIVEDB} exists. Proceeding with next steps ..."
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

echo -e "\nINFO: Calculating process id for audit table insertion"
echo "INFO: Executing hive command: hive -e \"select case when max(process_id) is null then 1 else max(process_id)+1 end as process_id  from ${AUDIT_TABLE}\""
PROCESS_ID=`hive -e "select case when max(process_id) is null then 0 else max(process_id)+1 end as process_id  from ${AUDIT_TABLE}"`
PROCESS_DESC=`cat $PARMFILE | grep "PROCESS_DESC" | cut -d "=" -f 2`

VWQRY=`cat $PARMFILE | grep "VWQRY" | cut -d "=" -f 2-`
VWQRY=`eval echo $VWQRY`
echo -e "View Query: ${VWQRY}"

echo -e "\nINFO: Checking Hive table..."
echo "INFO: Executing hive command: hive -e \"SELECT * FROM ${TGTTABLE} LIMIT 1\""
hive -e "SELECT * FROM ${TGTTABLE} LIMIT 1"
if [ $? != 0 ]; then
        if [ "$1" != "" ]; then
                max_modified_date=$1
                echo "INFO: ${TGTTABLE} table doesn't exists in Hive. The modified date passed as parameter is considered"
        else
                ERR_DSC="Error: ${TGTTABLE} table doesn't exists in Hive. The modified date should be provided as parameter"
                echo "${ERR_DSC}"
                END_TIME=`date +'%Y-%m-%d %H:%M:%S'`
                STATUS="FAILED"
                echo "INFO: Inserting an entry into audit table..."
				echo "INFO: Executing hive command: hive -e \"insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',0,0,0,'${STATUS}','${ERR_DSC}')\""
                hive -e "insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',0,0,0,'${STATUS}','${ERR_DSC}')"
                exit 1
        fi
else
        echo "INFO: ${TGTTABLE} exists in Hive. Validating the count of records..."
        TGTTABCNT=`hive  -e "SELECT COUNT(*) FROM ${TGTTABLE}"`
        if [ "$TGTTABCNT" == 0 ]; then
				if [ "$1" != "" ]; then
						max_modified_date=$1
                        echo "INFO: The ${TGTTABLE} doesn't have any record. The modified date passed as parameter is considered"
                else
						ERR_DSC="Error: The ${TGTTABLE} doesnt have records. Modified date should be passed as parameter"
                        echo "${ERR_DSC}"
                        END_TIME=`date +'%Y-%m-%d %H:%M:%S'`
                        STATUS="FAILED"
                        echo "INFO: Inserting an entry into audit table..."
                        echo "INFO: Executing hive command: hive -e \"insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',0,0,0,'${STATUS}','${ERR_DSC}')\""
						hive -e "insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',0,0,0,'${STATUS}','${ERR_DSC}')"
                        exit 1
                fi
        else
				echo "INFO: The ${TGTTABLE} exists and has records."
        fi
fi

echo -e "\n----- Step 2: Last Maximum Modified Date Calculation ------"
echo "INFO: Modified Date passed as argument takes the higher precedence"

if [ "$1" != "" ]; then
   max_modified_date=$1
else
        MAX_MOD_DATE_QRY=`cat $PARMFILE | grep "MAX_MOD_DATE_QRY" | cut -d "=" -f 2-`
        MAX_MOD_DATE_QRY=`eval echo $MAX_MOD_DATE_QRY`
        echo "INFO: The query for max modified date calculation: ${MAX_MOD_DATE_QRY}"
        max_modified_date=`hive -e "${MAX_MOD_DATE_QRY}"`
fi

echo -e  "INFO: Last Maximum Modified Date to be used for incremental pull: ${max_modified_date}"

echo -e "\n----- Step 3: Teradata Validation ------"
echo "INFO: Getting count of records from Teradata ..."

teraqry=`sqoop eval -Dhadoop.security.credential.provider.path=jceks://hdfs/keystore/teradatad.password.jceks --connect "jdbc:teradata://10.72.23.136/DATABASE=DDSVD" --username DDSVAWSSQHUMDLREADD --password-alias teradatad.password.alias --query "select count(*) from ${SRCTABLE} where MODIFIED_DATE>'${max_modified_date}'"`
teracnt=`echo $teraqry | cut -d'|' -f4`
teracnt=`echo $teracnt | tr -s " "`
srctablecnt=`sqoop eval -Dhadoop.security.credential.provider.path=jceks://hdfs/keystore/teradatad.password.jceks --connect "jdbc:teradata://10.72.23.136/DATABASE=DDSVD" --username DDSVAWSSQHUMDLREADD --password-alias teradatad.password.alias --query "select count(*) from ${SRCTABLE}"`
srctablecnt=`echo $srctablecnt | cut -d'|' -f4`
srctablecnt=`echo $srctablecnt | tr -s " "`

if [ "${teracnt}" -eq 0 ]; then
        ERR_DSC="INFO: No new records found in Teradata table $SRCTABLE since last modified date"
        echo "${ERR_DSC}"
        END_TIME=`date +'%Y-%m-%d %H:%M:%S'`
        STATUS="SUCCEEDED"
        echo "Executing: hive  -e \"SELECT * FROM ${FNLTABLE} LIMIT 1\""
        hive  -e "SELECT * FROM ${FNLTABLE} LIMIT 1"
        if [ $? != 0 ]; then
                echo "INFO: ${FNLTABLE} doesn't exist in Hive. The count will be populated as 0 in audit table"
        else
				echo "INFO: ${FNLTABLE} exists in Hive. Calculating the count of records..."
				tgttablecnt=`hive -e "select count(*) from ${FNLTABLE}"`
        fi
        echo "INFO: Inserting an entry into audit table..."
		echo "INFO: Executing hive command: hive -e \"insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',${teracnt},${srctablecnt},${tgttablecnt},'${STATUS}','${ERR_DSC}')\""
        hive -e "insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',${teracnt},${srctablecnt},${tgttablecnt},'${STATUS}','${ERR_DSC}')"
        exit 0
else
        echo "INFO: Total number of incremental records to be inserted: $teracnt"
fi

echo -e "\n----- Step 4: Sqoop Command Execution -----"
echo "INFO: Starting Sqoop Command Execution ..."
echo "Sqoop Command:sqoop import -Dhadoop.security.credential.provider.path=jceks://hdfs/keystore/teradatad.password.jceks --connect \"jdbc:teradata://10.72.23.136/DATABASE=DDSVD\" --username DDSVAWSSQHUMDLREADD --password-alias teradatad.password.alias --table ${SRCTABLE} --where \"MODIFIED_DATE>'${max_modified_date}'\" -m 1 --split-by customer_id --hive-import --hive-table ${TGTTABLE}  --compress --map-column-hive CUSTOMER_ID=STRING"
sqoop import -Dhadoop.security.credential.provider.path=jceks://hdfs/keystore/teradatad.password.jceks --connect "jdbc:teradata://10.72.23.136/DATABASE=DDSVD" --username DDSVAWSSQHUMDLREADD --password-alias teradatad.password.alias --table ${SRCTABLE} --where "MODIFIED_DATE>'${max_modified_date}'" -m 1 --split-by customer_id --hive-import --hive-table ${TGTTABLE}  --compress --map-column-hive CUSTOMER_ID=STRING

if [ $? == 0 ]; then
        echo -e "\nINFO: Import has been completed successfully"
        echo "INFO: Total Number of source records read from ${SRCTABLE}: $teracnt"
        tgtcnt=`hive -e "select count(*) from ${TGTTABLE} where cast(modified_date as timestamp)>'${max_modified_date}'"`
        echo "INFO: Total Number of target records inserted in ${TGTTABLE}: $tgtcnt"
else
        ERR_DSC="ERROR: Sqoop Command Failed. Please see sqoop log for more details"
        echo "${ERR_DSC}"
        END_TIME=`date +'%Y-%m-%d %H:%M:%S'`
        STATUS="FAILED"
        echo -e "\nINFO: Inserting record into audit table..."
        echo "INFO: Executing hive Command: hive -e \"insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',0,${srctablecnt},0,'${STATUS}','')\""
		hive -e "insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',0,${srctablecnt},0,'${STATUS}','${ERR_DSC}')"
        exit 1
fi

echo -e "\n----- Step 5: Hive Table Validations -----"
echo "INFO: Starting validation checks ..."
tgtvwcnt=`hive -e "select count(*) from $TGTVW"`
if [ $? == 0 ]; then
        echo "INFO: Total Number of records in ${SRCTABLE} in Teradata: ${srctablecnt}"
        echo "INFO: Total Number of records in ${TGTVW} in Hive: ${tgtvwcnt}"
else
        echo "INFO: The view ${TGTVW} doesn't exist in Hive. Creating the same..."
        hive -e "${VWQRY}"
        echo "INFO: Executing view query: ${VWQRY}"
        tgtvwcnt=`hive -e "select count(*) from $TGTVW"`
        echo "INFO: Total Number of records in ${SRCTABLE} in Teradata: ${srctablecnt}"
        echo "INFO: Total Number of records in ${TGTVW} in Hive: ${tgtvwcnt}"
fi

if [ "${srctablecnt}" == "${tgtvwcnt}" ]; then
        echo -e "\nINFO: The count of records matches between ${SRCTABLE} in Teradata and ${TGTVW} in Hive"
else
        echo -e "\nINFO: The count of records doesn't match between ${SRCTABLE} in Teradata and ${TGTVW} in Hive"
        echo "INFO: Please check for the log for more details. Skipping Final table creation process..."
        ERR_DSC="ERROR: The count of records in Teradata and Hive is not matching"
        END_TIME=`date +'%Y-%m-%d %H:%M:%S'`
        echo "INFO: Inserting an entry into audit table..."
        echo "INFO: Executing hive Command: hive -e \"insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',${teracnt},${srctablecnt},${tgtvwcnt},'${STATUS}','')\""
		hive -e "insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',${teracnt},${srctablecnt},${tgtvwcnt},'${STATUS}','{ERR_DSC}')"
        exit 1
fi

echo -e "\n----- Step 5: Hive Final Table Creation -----"
echo "INFO: Dropping ${FNLTABLE} in Hive ..."
hive -e "drop table IF EXISTS ${FNLTABLE}"
if [ $? == 0 ]; then
        echo "INFO: The table ${FNLTABLE} has been dropped"
        echo "INFO: Creating table ${FNLTABLE} with new dataset"
        FNLTABQRY=`cat $PARMFILE | grep "FNLTABQRY" | cut -d "=" -f 2-`
        FNLTABQRY=`eval echo "${FNLTABQRY}"`
        echo "INFO: The final creation query: $FNLTABQRY"
        hive -e "${FNLTABQRY}"
                if [ $? == 0 ]; then
                                echo "INFO: The table ${FNLTABLE} has been re-created successfully"
                                echo "INFO: Calculating count of records in ${FNLTABLE}"
                                tgtvwcnt=`hive -e "select count(*) from $FNLTABLE"`
                else
                                ERR_DSC="ERROR: The script ${FNLTABQRY} got failed, and table ${FNLTABLE} is not created"
                                echo "${ERR_DSC}"
                                END_TIME=`date +'%Y-%m-%d %H:%M:%S'`
                                STATUS="FAILED"
                                echo "INFO: Inserting an entry into audit table..."
                                echo "INFO: Executing hive Command: hive -e \"insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',${teracnt},${srctablecnt},0,'${STATUS}','')\""
								hive -e "insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',${teracnt},${srctablecnt},0,'${STATUS}',${ERR_DSC})"
                fi
else
        ERR_DSC="ERROR: The table ${FNLTABLE} doesn't get created. Please check the log for more details"
        echo "$ERR_DSC"
        END_TIME=`date +'%Y-%m-%d %H:%M:%S'`
        STATUS="FAILED"
        echo "INFO: Inserting an entry into audit table..."
        echo "INFO: Executing hive Command: hive -e \"insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',${teracnt},${srctablecnt},${tgtvwcnt},'${STATUS}','')\""
		hive -e "insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',${teracnt},${srctablecnt},${tgtvwcnt},'${STATUS}','')"
        exit 1
fi

echo -e "\n----- Step 6: Audit Table Insertion -----"

END_TIME=`date +'%Y-%m-%d %H:%M:%S'`
STATUS="SUCCEEDED"
echo "INFO: Inserting an entry into audit table..."
echo "INFO: Executing hive Command: hive -e \"insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',${teracnt},${srctablecnt},${tgtvwcnt},'${STATUS}','')\""
hive -e "insert into ${AUDIT_TABLE} values (${PROCESS_ID},'${PROCESS_DESC}','${JOB_NAME}','${DATA_PROV_NAME}','${DATA_SET}','${SRC_DB}','${HIVEDB}','${SRCTABLE}','${TGTTABLE}','${STRT_TIME}','${END_TIME}',${teracnt},${srctablecnt},${tgtvwcnt},'${STATUS}','')"
echo "INFO: An entry has been inserted into audit table" 


echo -e "\n--------------------------------------------------------------------------------------"
echo "INFO: Data from $SRCTABLE has been imported successfully to $FNLTABLE"
echo "INFO: Script Execution has been completed Successfully"
echo "-------------------------------------------------------------------------------------------"
