#!/usr/bin/perl -w


#use strict;
use warnings;
use diagnostics;
use POSIX qw/strftime/;

#change directory
chdir("/home/hdfsbe/test_quality/");

#set queue name for hadoop query

my $queueName = "BDML_SINGAPORE";

chomp($queueName);

#for(my $n=0;$n<=1;$n++){

#obtain desired date

my $epoch = time(); # - $n * 24 * 60 * 60;

my $selected_Date = $epoch - 3 * 24 * 60 * 60;
my $selected_Date_Upper = $epoch - 2 * 24 * 60 * 60;
my $selected_Date_Lower = $selected_Date;

my $selected_Date_1 = strftime ("%Y/%m/%d", localtime($selected_Date));
chomp($selected_Date_1);
my $selected_Date_2 = strftime ("%Y-%m-%d", localtime($selected_Date));
chomp($selected_Date_2);


my $selected_Date_Upper_1 = strftime ("%Y-%m-%d", localtime($selected_Date_Upper));
chomp($selected_Date_Upper_1);
my $selected_Date_Lower_1 = strftime ("%Y-%m-%d", localtime($selected_Date_Lower));
chomp($selected_Date_Lower_1);


#create temp folder for storage
my $temp_dir_1 = "/eng/mti/ww/be/msb/test_quality/Temp/auto_variance_daily";
chomp($temp_dir_1);
`hadoop fs -rm -r $temp_dir_1`;
`hadoop fs -mkdir $temp_dir_1`;


my $temp_dir_2 = "/eng/mti/ww/be/msb/test_quality/Temp/auto_totallot_daily";
chomp($temp_dir_2);
`hadoop fs -rm -r $temp_dir_2`;
`hadoop fs -mkdir $temp_dir_2`;


#query of variance data

`beeline -u "jdbc:hive2://fshdpprod01-hive.imfs.micron.com:10010/default;principal=hive/fslhdppname2.imfs.micron.com\@HADOOP.MICRON.COM" -e "INSERT OVERWRITE DIRECTORY '$temp_dir_1'
SELECT LOT_ID, DESIGN_ID, PACKAGE_TYPE, STEP_TEMPERATURE, LEAD_COUNT, PACKAGE_HEIGHT, PACKAGE_LENGTH, PACKAGE_WIDTH, PRODUCT_FAMILY, reason, action, STEP_ID, quantity, date_time_local, TESTER_ID, LOT_QTY, partition_date FROM
	(SELECT LOT_ID, DESIGN_ID, PACKAGE_TYPE, STEP_TEMPERATURE, LEAD_COUNT, PACKAGE_HEIGHT, PACKAGE_LENGTH, PACKAGE_WIDTH, PRODUCT_FAMILY, reason, action, STEP_ID, quantity, step_job_oid FROM
		(SELECT a.ma_id AS LOT_ID,
         		MAX(CASE WHEN a.attribute_id = 'DESIGN ID' THEN a.attribute_value ELSE NULL END) AS DESIGN_ID,
         		MAX(CASE WHEN a.attribute_id = 'PACKAGE TYPE' THEN a.attribute_value ELSE NULL END) AS PACKAGE_TYPE,
         		MAX(CASE WHEN a.attribute_id = 'STEP TEMPERATURE' THEN a.attribute_value ELSE NULL END) AS STEP_TEMPERATURE,
         		MAX(CASE WHEN a.attribute_id = 'LEAD COUNT' THEN a.attribute_value ELSE NULL END) AS LEAD_COUNT,
         		MAX(CASE WHEN a.attribute_id = 'PACKAGE HEIGHT' THEN a.attribute_value ELSE NULL END) AS PACKAGE_HEIGHT,
         		MAX(CASE WHEN a.attribute_id = 'PACKAGE LENGTH' THEN a.attribute_value ELSE NULL END) AS PACKAGE_LENGTH,
         		MAX(CASE WHEN a.attribute_id = 'PACKAGE WIDTH' THEN a.attribute_value ELSE NULL END) AS PACKAGE_WIDTH,
         		MAX(CASE WHEN a.attribute_id = 'PRODUCT FAMILY' THEN a.attribute_value ELSE NULL END) AS PRODUCT_FAMILY
	 	 FROM 
			(SELECT ma_id, attribute_id, attribute_value
		 	 FROM prod_mti_ww_be_idl.mam_tx
		 	 WHERE ma_type='LOT' AND system_name='MAMTSTSI' AND facility_id='TEST-MSA' 
		 	 AND attribute_id IN ('DESIGN ID','PACKAGE TYPE','STEP TEMPERATURE','LEAD COUNT','PACKAGE HEIGHT','PACKAGE LENGTH','PACKAGE WIDTH','PRODUCT FAMILY') AND partition_date between '$selected_Date_Lower_1' and '$selected_Date_Upper_1'
		 	 GROUP BY ma_id, attribute_id, attribute_value) a 
		 GROUP BY a.ma_id) b1
		JOIN
		(SELECT ma_id AS LOT_ID, reason, action, location AS STEP_ID, quantity, step_job_oid
	 	 FROM prod_mti_ww_be_idl.mam_tx
	 	 WHERE ma_type='LOT' AND system_name='MAMTSTSI' AND facility_id='TEST-MSA' AND reason IN ('VARIANCE') AND partition_date between '$selected_Date_Lower_1' and '$selected_Date_Upper_1'
	 	 GROUP BY ma_id, reason, action, location, quantity, step_job_oid) b2
		ON b1.LOT_ID = b2.LOT_ID
		GROUP BY b1.LOT_ID, b1.DESIGN_ID, b1.PACKAGE_TYPE, b1.STEP_TEMPERATURE, b1.LEAD_COUNT, b1.PACKAGE_HEIGHT, b1.PACKAGE_LENGTH, b1.PACKAGE_WIDTH, b1.PRODUCT_FAMILY, b2.reason, b2.action, b2.STEP_ID, b2.quantity, b2.step_job_oid) e1
	JOIN
	(SELECT LOT_ID, STEP_ID, TESTER_ID, LOT_QTY, date_time_local, partition_date, step_job_oid FROM        
		(SELECT ma_id AS LOT_ID, attribute_prior_value AS LOT_QTY, job_oid, date_time_local, partition_date 
	 	 FROM prod_mti_ww_be_idl.mam_tx 
	 	 WHERE ma_type='LOT' AND system_name='MAMTSTSI' AND facility_id='TEST-MSA' AND attribute_id='CURRENT QTY' AND attribute_prior_value IS NOT NULL AND partition_date between '$selected_Date_Lower_1' and '$selected_Date_Upper_1'
	 	 GROUP BY ma_id, attribute_prior_value, job_oid, date_time_local, partition_date) c1
		JOIN
		(SELECT LOT_ID, STEP_ID, TESTER_ID, step_job_oid FROM
			(SELECT ma_id AS LOT_ID, job_oid, resource AS TESTER_ID, MIN(unix_timestamp(date_time_local, 'yyyy-MM-dd HH:mm:ss.SS+0800')) AS time_machine_start
		 	 FROM prod_mti_ww_be_idl.mam_tx
		 	 WHERE ma_type='LOT' AND system_name='MAMTSTSI' AND facility_id='TEST-MSA' AND role='TESTER MACHINE' AND event_action='STARTED' AND partition_date between '$selected_Date_Lower_1' and '$selected_Date_Upper_1'
		 	 GROUP BY ma_id, job_oid, resource) d1
			JOIN
			(SELECT ma_id AS LOT_ID, location AS STEP_ID, step_job_oid, MAX(unix_timestamp(date_time_local, 'yyyy-MM-dd HH:mm:ss.SS+0800')) AS time_record_start 
		 	 FROM prod_mti_ww_be_idl.mam_tx 
		 	 WHERE ma_type='LOT' AND system_name='MAMTSTSI' AND facility_id='TEST-MSA' AND event_type='GenealogyChanged' AND partition_date between '$selected_Date_Lower_1' and '$selected_Date_Upper_1'
		 	 GROUP BY ma_id, location, step_job_oid) d2
			ON d1.LOT_ID = d2.LOT_ID AND d1.job_oid = d2.step_job_oid
			WHERE d1.time_machine_start < d2.time_record_start
		 	GROUP BY d1.LOT_ID, d2.STEP_ID, d1.TESTER_ID, d2.step_job_oid) c2
		ON c1.LOT_ID = c2.LOT_ID AND c1.job_oid = c2.step_job_oid
	 	GROUP BY c1.LOT_ID, c2.STEP_ID, c2.TESTER_ID, c1.LOT_QTY, c1.date_time_local, c1.partition_date, c2.step_job_oid) e2
	ON e1.LOT_ID = e2.LOT_ID AND e1.step_job_oid = e2.step_job_oid
	GROUP BY e1.LOT_ID, e1.DESIGN_ID, e1.PACKAGE_TYPE, e1.STEP_TEMPERATURE, e1.LEAD_COUNT, e1.PACKAGE_HEIGHT, e1.PACKAGE_LENGTH, e1.PACKAGE_WIDTH, e1.PRODUCT_FAMILY, e1.reason, e1.action, e1.STEP_ID, e1.quantity, e2.date_time_local, e2.TESTER_ID, e2.LOT_QTY, e2.partition_date;"`;



#QUERY OF TOTAL LOTS

`beeline -u "jdbc:hive2://fshdpprod01-hive.imfs.micron.com:10010/default;principal=hive/fslhdppname2.imfs.micron.com\@HADOOP.MICRON.COM" -e "INSERT OVERWRITE DIRECTORY '$temp_dir_2'
SELECT LOT_ID, DESIGN_ID, PACKAGE_TYPE, STEP_TEMPERATURE, LEAD_COUNT, PACKAGE_HEIGHT, PACKAGE_LENGTH, PACKAGE_WIDTH, PRODUCT_FAMILY, STEP_ID, TESTER_ID, LOT_QTY, date_time_local, partition_date FROM
	(SELECT a.ma_id AS LOT_ID,
         	MAX(CASE WHEN a.attribute_id = 'DESIGN ID' THEN a.attribute_value ELSE NULL END) AS DESIGN_ID,
         	MAX(CASE WHEN a.attribute_id = 'PACKAGE TYPE' THEN a.attribute_value ELSE NULL END) AS PACKAGE_TYPE,
         	MAX(CASE WHEN a.attribute_id = 'STEP TEMPERATURE' THEN a.attribute_value ELSE NULL END) AS STEP_TEMPERATURE,
         	MAX(CASE WHEN a.attribute_id = 'LEAD COUNT' THEN a.attribute_value ELSE NULL END) AS LEAD_COUNT,
         	MAX(CASE WHEN a.attribute_id = 'PACKAGE HEIGHT' THEN a.attribute_value ELSE NULL END) AS PACKAGE_HEIGHT,
         	MAX(CASE WHEN a.attribute_id = 'PACKAGE LENGTH' THEN a.attribute_value ELSE NULL END) AS PACKAGE_LENGTH,
         	MAX(CASE WHEN a.attribute_id = 'PACKAGE WIDTH' THEN a.attribute_value ELSE NULL END) AS PACKAGE_WIDTH,
         	MAX(CASE WHEN a.attribute_id = 'PRODUCT FAMILY' THEN a.attribute_value ELSE NULL END) AS PRODUCT_FAMILY
	 FROM 
		(SELECT ma_id, attribute_id, attribute_value
		 FROM prod_mti_ww_be_idl.mam_tx
		 WHERE ma_type='LOT' AND system_name='MAMTSTSI' AND facility_id='TEST-MSA' 
		 AND attribute_id IN ('DESIGN ID','PACKAGE TYPE','STEP TEMPERATURE','LEAD COUNT','PACKAGE HEIGHT','PACKAGE LENGTH','PACKAGE WIDTH','PRODUCT FAMILY') AND partition_date between '$selected_Date_Lower_1' and '$selected_Date_Upper_1'
		 GROUP BY ma_id, attribute_id, attribute_value) a 
	GROUP BY a.ma_id) b1
	JOIN
	(SELECT LOT_ID, STEP_ID, TESTER_ID, LOT_QTY, date_time_local, partition_date FROM        
		(SELECT ma_id AS LOT_ID, attribute_prior_value AS LOT_QTY, job_oid, date_time_local, partition_date 
	 	 FROM prod_mti_ww_be_idl.mam_tx 
	 	 WHERE ma_type='LOT' AND system_name='MAMTSTSI' AND facility_id='TEST-MSA' AND attribute_id='CURRENT QTY' AND attribute_prior_value IS NOT NULL AND partition_date between '$selected_Date_Lower_1' and '$selected_Date_Upper_1'
	 	 GROUP BY ma_id, attribute_prior_value, job_oid, date_time_local, partition_date) c1
		JOIN
		(SELECT LOT_ID, STEP_ID, TESTER_ID, step_job_oid FROM
			(SELECT ma_id AS LOT_ID, job_oid, resource AS TESTER_ID, MIN(unix_timestamp(date_time_local, 'yyyy-MM-dd HH:mm:ss.SS+0800')) AS time_machine_start
		 	 FROM prod_mti_ww_be_idl.mam_tx
		 	 WHERE ma_type='LOT' AND system_name='MAMTSTSI' AND facility_id='TEST-MSA' AND role='TESTER MACHINE' AND event_action='STARTED' AND partition_date between '$selected_Date_Lower_1' and '$selected_Date_Upper_1'
		 	 GROUP BY ma_id, job_oid, resource) d1
			JOIN
			(SELECT ma_id AS LOT_ID, location AS STEP_ID, step_job_oid, MAX(unix_timestamp(date_time_local, 'yyyy-MM-dd HH:mm:ss.SS+0800')) AS time_record_start 
		 	 FROM prod_mti_ww_be_idl.mam_tx 
		 	 WHERE ma_type='LOT' AND system_name='MAMTSTSI' AND facility_id='TEST-MSA' AND event_type='GenealogyChanged' AND partition_date between '$selected_Date_Lower_1' and '$selected_Date_Upper_1'
		 	 GROUP BY ma_id, location, step_job_oid) d2
			ON d1.LOT_ID = d2.LOT_ID AND d1.job_oid = d2.step_job_oid
			WHERE d1.time_machine_start < d2.time_record_start
		 	GROUP BY d1.LOT_ID, d2.STEP_ID, d1.TESTER_ID, d2.step_job_oid) c2
		ON c1.LOT_ID = c2.LOT_ID AND c1.job_oid = c2.step_job_oid
	 	GROUP BY c1.LOT_ID, c2.STEP_ID, c2.TESTER_ID, c1.LOT_QTY, c1.date_time_local, c1.partition_date) b2
	ON b1.LOT_ID = b2.LOT_ID
	GROUP BY b1.LOT_ID, b1.DESIGN_ID, b1.PACKAGE_TYPE, b1.STEP_TEMPERATURE, b1.LEAD_COUNT, b1.PACKAGE_HEIGHT, b1.PACKAGE_LENGTH, b1.PACKAGE_WIDTH, b1.PRODUCT_FAMILY, b2.STEP_ID, b2.TESTER_ID, b2.LOT_QTY, b2.date_time_local, b2.partition_date;"`;



#remove temp folder for report
my $dir_report_variance = "/eng/mti/ww/be/msb/test_quality/auto_variance/$selected_Date_2";
chomp($dir_report_variance);
`hadoop fs -rm -r $dir_report_variance`;


my $dir_report_totallot = "/eng/mti/ww/be/msb/test_quality/auto_totallot/$selected_Date_2";
chomp($dir_report_totallot);
`hadoop fs -rm -r $dir_report_totallot`;


#use pig to generate ww and store into hive table

`pig -e "REGISTER '/home/hdfsbe/test_quality/ww_convert.py' USING jython AS ww_convert;

A = LOAD '$temp_dir_1/*' USING PigStorage('\\u0001') AS 
(LOT_ID:chararray, 
 DESIGN_ID:chararray, 
 PACKAGE_TYPE:chararray, 
 STEP_TEMPERATURE:chararray, 
 LEAD_COUNT:chararray,
 PACKAGE_HEIGHT:chararray, 
 PACKAGE_LENGTH:chararray, 
 PACKAGE_WIDTH:chararray, 
 PRODUCT_FAMILY:chararray,
 reason:chararray, 
 action:chararray, 
 STEP_ID:chararray,
 quantity:chararray,
 date_time_local:chararray,
 TESTER_ID:chararray,
 LOT_QTY:chararray,
 partition_date:chararray
);

A_OP = FOREACH A GENERATE
LOT_ID,
STEP_ID, 
DESIGN_ID, 
PACKAGE_TYPE, 
STEP_TEMPERATURE, 
LEAD_COUNT,
PACKAGE_HEIGHT, 
PACKAGE_LENGTH, 
PACKAGE_WIDTH, 
PRODUCT_FAMILY,
TESTER_ID,
LOT_QTY,
reason, 
action, 
quantity,
date_time_local,
ww_convert.get_ww(date_time_local) AS ww:chararray,
partition_date;


B = LOAD '$temp_dir_2/*' USING PigStorage('\\u0001') AS 
(LOT_ID:chararray, 
 DESIGN_ID:chararray, 
 PACKAGE_TYPE:chararray, 
 STEP_TEMPERATURE:chararray, 
 LEAD_COUNT:chararray,
 PACKAGE_HEIGHT:chararray, 
 PACKAGE_LENGTH:chararray, 
 PACKAGE_WIDTH:chararray, 
 PRODUCT_FAMILY:chararray,
 STEP_ID:chararray,
 TESTER_ID:chararray, 
 LOT_QTY:chararray,
 date_time_local:chararray,
 partition_date:chararray
);

B_OP = FOREACH B GENERATE
LOT_ID,
STEP_ID, 
DESIGN_ID, 
PACKAGE_TYPE, 
-- STEP_TEMPERATURE, 
-- LEAD_COUNT,
-- PACKAGE_HEIGHT, 
-- PACKAGE_LENGTH, 
-- PACKAGE_WIDTH, 
PRODUCT_FAMILY,
TESTER_ID,
LOT_QTY,
date_time_local,
ww_convert.get_ww(date_time_local) AS ww:chararray,
partition_date;

A_OP_DIST = DISTINCT A_OP;
B_OP_DIST = DISTINCT B_OP;

STORE A_OP_DIST INTO '$dir_report_variance' USING PigStorage(',');
STORE B_OP_DIST INTO '$dir_report_totallot' USING PigStorage(',');"`;


#}
