#!/usr/bin/perl


#use strict;
use warnings;
use diagnostics;
use POSIX qw/strftime/;

#change directory
chdir("/home/xiaoxiang/automation/");

#set queue name for hadoop query

my $queueName = "BDML_SINGAPORE";

chomp($queueName);

#for(my $n=31;$n<=360;$n++){

#obtain desired date

my $epoch = time() - 53 * 24 * 60 * 60;

my $selected_Date = $epoch - 7 * 24 * 60 * 60;
my $selected_Date_Upper = $epoch;
my $selected_Date_Lower = $epoch - 15 * 24 * 60 * 60;

my $selected_Date_1 = strftime ("%Y/%m/%d", localtime($selected_Date));
chomp($selected_Date_1);
my $selected_Date_2 = strftime ("%Y-%m-%d", localtime($selected_Date));
chomp($selected_Date_2);
#$selected_Date_1 =~ s/0*(\d+)/$1/g;


my $selected_Date_Upper_1 = strftime ("%Y-%m-%d", localtime($selected_Date_Upper));
chomp($selected_Date_Upper_1);
my $selected_Date_Lower_1 = strftime ("%Y-%m-%d", localtime($selected_Date_Lower));
chomp($selected_Date_Lower_1);


#create temp folder for storage
#my $temp_dir_1 = "/user/xiaoxiang/Automation_Variance_Temp";
my $temp_dir_1 = "/eng/mti/ww/be/msb/test_quality/auto_variance_daily_Temp";
chomp($temp_dir_1);
`hadoop fs -rm -r $temp_dir_1`;
`hadoop fs -mkdir $temp_dir_1`;

#my $temp_dir_2 = "/user/xiaoxiang/Automation_All_Temp";
my $temp_dir_2 = "/eng/mti/ww/be/msb/test_quality/auto_totallot_daily_Temp";
chomp($temp_dir_2);
`hadoop fs -rm -r $temp_dir_2`;
`hadoop fs -mkdir $temp_dir_2`;


#query of variance data

`beeline -u "jdbc:hive2://fshdpprod01-hive.imfs.micron.com:10010/default;principal=hive/fslhdppname2.imfs.micron.com\@HADOOP.MICRON.COM" -e "INSERT OVERWRITE DIRECTORY '$temp_dir_1' 
SELECT LOT_ID, DESIGN_ID, PACKAGE_TYPE, STEP_TEMPERATURE, LEAD_COUNT, PACKAGE_HEIGHT, PACKAGE_LENGTH, PACKAGE_WIDTH, PRODUCT_FAMILY, LOT_QTY, reason, action, location, quantity, date_time_local, partition_date FROM
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
     (SELECT ma_id, ma_type, facility_id, attribute_id, attribute_value
      FROM prod_mti_ww_be_idl.mam_tx
      WHERE ma_type='LOT' AND system_name='MAMTSTSI' AND facility_id='TEST-MSA' AND partition_date between '$selected_Date_Lower_1' and '$selected_Date_Upper_1' AND attribute_id IN ('DESIGN ID', 
                                                                          'PACKAGE TYPE', 
                                                                          'STEP TEMPERATURE', 
                                                                          'LEAD COUNT', 
                                                                          'PACKAGE HEIGHT', 
                                                                          'PACKAGE LENGTH', 
                                                                          'PACKAGE WIDTH', 
                                                                          'PRODUCT FAMILY') 
      GROUP BY ma_id, ma_type, facility_id, attribute_id, attribute_value) a 
   GROUP BY a.ma_id) b
JOIN 
   (SELECT ma_id AS LOT_ID, reason, action, location, quantity, date_time_local 
    FROM prod_mti_ww_be_idl.mam_tx
    WHERE ma_type='LOT' AND system_name='MAMTSTSI' AND facility_id='TEST-MSA' AND partition_date between '$selected_Date_Lower_1' and '$selected_Date_Upper_1' AND reason IN ('VARIANCE')
    GROUP BY ma_id, ma_type, facility_id, reason, action, location, quantity, date_time_local) c
ON b.LOT_ID = c.LOT_ID         
JOIN       
    (SELECT LOT_ID, location, attribute_prior_value AS LOT_QTY, partition_date FROM
         (SELECT ma_id AS LOT_ID, attribute_prior_value, job_oid, partition_date 
          FROM prod_mti_ww_be_idl.mam_tx 
          WHERE ma_type='LOT' AND system_name='MAMTSTSI' AND facility_id='TEST-MSA' AND attribute_id='CURRENT QTY' 
          AND partition_date between '$selected_Date_Lower_1' and '$selected_Date_Upper_1' AND attribute_prior_value IS NOT NULL
          GROUP BY ma_id, attribute_prior_value, job_oid, partition_date) c
          JOIN
          (SELECT ma_id AS LOT_ID, location, step_job_oid
           FROM prod_mti_ww_be_idl.mam_tx 
           WHERE ma_type='LOT' AND system_name='MAMTSTSI' AND facility_id='TEST-MSA' AND partition_date between '$selected_Date_Lower_1' and '$selected_Date_Upper_1' AND event_type='GenealogyChanged'
           GROUP BY ma_id, location, step_job_oid) d
          ON c.LOT_ID = d.LOT_ID AND c.job_oid = d.step_job_oid
          GROUP BY c.LOT_ID, d.location, c.attribute_prior_value, c.partition_date) e
ON c.LOT_ID = e.LOT_ID AND c.location = e.location
GROUP BY b.LOT_ID, DESIGN_ID, PACKAGE_TYPE, STEP_TEMPERATURE, LEAD_COUNT, 
         PACKAGE_HEIGHT, PACKAGE_LENGTH, PACKAGE_WIDTH, PRODUCT_FAMILY, e.LOT_QTY,
         reason, action, c.location, quantity, date_time_local, partition_date;"`;


#QUERY OF TOTAL LOTS

`beeline -u "jdbc:hive2://fshdpprod01-hive.imfs.micron.com:10010/default;principal=hive/fslhdppname2.imfs.micron.com\@HADOOP.MICRON.COM" -e "INSERT OVERWRITE DIRECTORY '$temp_dir_2' 
SELECT LOT_ID, DESIGN_ID, PACKAGE_TYPE, STEP_TEMPERATURE, LEAD_COUNT, PACKAGE_HEIGHT, PACKAGE_LENGTH, PACKAGE_WIDTH, PRODUCT_FAMILY, location, LOT_QTY, date_time_local, partition_date FROM
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
      WHERE ma_type='LOT' AND system_name='MAMTSTSI' AND facility_id='TEST-MSA' AND partition_date between '$selected_Date_Lower_1' and '$selected_Date_Upper_1' 
      AND attribute_id IN ('DESIGN ID', 'PACKAGE TYPE', 'STEP TEMPERATURE', 'LEAD COUNT', 'PACKAGE HEIGHT', 'PACKAGE LENGTH', 'PACKAGE WIDTH', 'PRODUCT FAMILY') 
      GROUP BY ma_id, attribute_id, attribute_value) a 
   GROUP BY a.ma_id) b
JOIN       
    (SELECT LOT_ID, location, attribute_prior_value AS LOT_QTY, date_time_local, partition_date FROM
         (SELECT ma_id AS LOT_ID, attribute_prior_value, date_time_local, job_oid, partition_date 
          FROM prod_mti_ww_be_idl.mam_tx 
          WHERE ma_type='LOT' AND system_name='MAMTSTSI' AND facility_id='TEST-MSA' AND attribute_id='CURRENT QTY' 
          AND partition_date between '$selected_Date_Lower_1' and '$selected_Date_Upper_1' AND attribute_prior_value IS NOT NULL
          GROUP BY ma_id, attribute_prior_value, date_time_local, job_oid, partition_date) c
          JOIN
          (SELECT ma_id AS LOT_ID, location, step_job_oid
           FROM prod_mti_ww_be_idl.mam_tx 
           WHERE ma_type='LOT' AND system_name='MAMTSTSI' AND facility_id='TEST-MSA' AND partition_date between '$selected_Date_Lower_1' and '$selected_Date_Upper_1' AND event_type='GenealogyChanged'
           GROUP BY ma_id, location, step_job_oid) d
          ON c.LOT_ID = d.LOT_ID AND c.job_oid = d.step_job_oid
          GROUP BY c.LOT_ID, d.location, c.attribute_prior_value, c.date_time_local, c.partition_date) e
ON b.LOT_ID = e.LOT_ID
GROUP BY b.LOT_ID, DESIGN_ID, PACKAGE_TYPE, STEP_TEMPERATURE, LEAD_COUNT, 
         PACKAGE_HEIGHT, PACKAGE_LENGTH, PACKAGE_WIDTH, PRODUCT_FAMILY, 
         location, LOT_QTY, date_time_local, partition_date;"`;
         

#remove temp folder for report
#my $dir_report_variance = "/user/xiaoxiang/Automation_Variance_Temp_report";
my $dir_report_variance = "/eng/mti/ww/be/msb/test_quality/auto_variance/$selected_Date_2";
chomp($dir_report_variance);
`hadoop fs -rm -r $dir_report_variance`;

#my $dir_report_totallot = "/user/xiaoxiang/Automation_All_Temp_report";
my $dir_report_totallot = "/eng/mti/ww/be/msb/test_quality/auto_totallot/$selected_Date_2";
chomp($dir_report_totallot);
`hadoop fs -rm -r $dir_report_totallot`;

#MERGE AND GENERATE REPORT

#my $output_filepath_all = "/home/xiaoxiang/automation/" . "$selected_Date_2\_all.csv"; #report/all/" . "$selected_Date_2\_all.csv";
#my $output_filepath_variance = "/home/xiaoxiang/automation/" . "$selected_Date_2\_variance.csv"; #report/variance/" . "$selected_Date_2\_variance.csv";


`pig -t ColumnMapKeyPrune -e "REGISTER /home/xiaoxiang/tte/tte-piggybank*.jar;

REGISTER '/home/xiaoxiang/tte/ww_convert.py' USING jython AS ww_convert;

A = load '/prod/mti/ww/test/tte/*/$selected_Date_1/*.parquet' USING parquet.pig.ParquetLoader
('summaryID:bytearray
, summaryType:chararray
, derivedLotID:chararray
, serialNo:chararray
, design:chararray
, step:bytearray
, facility:chararray
, testFacility:chararray
, version:chararray
, standardFlow:chararray
, ownerCompany:chararray
, machineID:chararray
, startEtime:chararray
, endEtime:chararray
, cycleData:{array:(cycle:int,timestamp:long,data:chararray)}
, attributes:map[chararray]'
);

B = FOREACH A GENERATE attributes#'LOT' AS LOT_ID_RAW:chararray,
attributes#'STEP_ID' AS STEP_ID_RAW:chararray,
-- attributes#'HANDLER_ID' AS HANDLER_ID:chararray,
attributes#'TESTER_ID' AS TESTER_ID:chararray,
-- attributes#'VERSION' AS VERSION:chararray,
attributes#'SPEED' AS SPEED:chararray,
-- attributes#'HANDLER_VENDOR' AS HANDLER_VENDOR:chararray,
attributes#'TEMPERATURE' AS TEMPERATURE:chararray;

B_withU = FILTER B BY LOT_ID_RAW MATCHES '.*U\$';

B_withU_seperated = FOREACH B_withU GENERATE *, 
FLATTEN(REGEX_EXTRACT_ALL(LOT_ID_RAW, '(.*)(U\$)')) AS (LOT_ID:chararray, U:chararray), 
REPLACE(STEP_ID_RAW,'_',' ') AS STEP_ID:chararray;

B_OP_1 = FOREACH B_withU_seperated GENERATE 
LOT_ID, 
STEP_ID, 
-- HANDLER_ID, 
TESTER_ID, 
-- VERSION, 
SPEED, 
-- HANDLER_VENDOR,
TEMPERATURE;

B_NO_NULL = FILTER B BY TESTER_ID IS NOT NULL;

B_NO_NULL_OP = FOREACH B_NO_NULL GENERATE
LOT_ID_RAW AS LOT_ID, 
REPLACE(STEP_ID_RAW,'_',' ') AS STEP_ID, 
-- HANDLER_ID, 
TESTER_ID, 
-- VERSION, 
SPEED, 
-- HANDLER_VENDOR,
TEMPERATURE;

B_OP = UNION B_OP_1, B_NO_NULL_OP;

C1 = LOAD '$temp_dir_2/*' USING PigStorage('\\u0001') AS 
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
 LOT_QTY:chararray,
 date_time_local:chararray,
 partition_date:chararray
);

C2 = LOAD '$temp_dir_1/*' USING PigStorage('\\u0001') AS 
(LOT_ID:chararray, 
 DESIGN_ID:chararray, 
 PACKAGE_TYPE:chararray, 
 STEP_TEMPERATURE:chararray, 
 LEAD_COUNT:chararray,
 PACKAGE_HEIGHT:chararray, 
 PACKAGE_LENGTH:chararray, 
 PACKAGE_WIDTH:chararray, 
 PRODUCT_FAMILY:chararray,
 LOT_QTY:chararray,
 reason:chararray, 
 action:chararray, 
 STEP_ID:chararray,
 quantity:chararray,
 date_time_local:chararray,
 partition_date:chararray
);



D1 = JOIN C1 BY (LOT_ID, STEP_ID),
         B_OP BY (LOT_ID, STEP_ID);
                 
D2 = JOIN C2 BY (LOT_ID, STEP_ID),
         B_OP BY (LOT_ID, STEP_ID);


D1_OP = FOREACH D1 GENERATE 
FLATTEN(C1::LOT_ID) AS LOT_ID:chararray,
FLATTEN(C1::STEP_ID) AS STEP_ID:chararray,
FLATTEN(C1::PACKAGE_TYPE) AS PACKAGE_TYPE:chararray,
FLATTEN(C1::DESIGN_ID) AS DESIGN_ID:chararray, 
-- FLATTEN(C1::STEP_TEMPERATURE) AS STEP_TEMPERATURE:chararray, 
-- FLATTEN(C1::LEAD_COUNT) AS LEAD_COUNT:chararray,
-- FLATTEN(C1::PACKAGE_HEIGHT) AS PACKAGE_HEIGHT:chararray, 
-- FLATTEN(C1::PACKAGE_LENGTH) AS PACKAGE_LENGTH:chararray, 
-- FLATTEN(C1::PACKAGE_WIDTH) AS PACKAGE_WIDTH:chararray, 
FLATTEN(C1::PRODUCT_FAMILY) AS PRODUCT_FAMILY:chararray,
FLATTEN(C1::LOT_QTY) AS LOT_QTY:chararray,
FLATTEN(C1::date_time_local) AS date_time_local:chararray,
-- FLATTEN(B_OP::HANDLER_ID) AS HANDLER_ID:chararray,
-- FLATTEN(B_OP::VERSION) AS VERSION:chararray,
-- FLATTEN(B_OP::SPEED) AS SPEED:chararray,
-- FLATTEN(B_OP::TEMPERATURE) AS TEMPERATURE:chararray,
-- FLATTEN(B_OP::HANDLER_VENDOR) AS HANDLER_VENDOR:chararray;
UPPER(B_OP::TESTER_ID) AS TESTER_ID:chararray,
ww_convert.get_ww(C1::date_time_local) AS ww:chararray,
FLATTEN(C1::partition_date) AS partition_date:chararray;


D2_OP = FOREACH D2 GENERATE 
FLATTEN(C2::LOT_ID) AS LOT_ID:chararray,
FLATTEN(C2::STEP_ID) AS STEP_ID:chararray,
FLATTEN(C2::PACKAGE_TYPE) AS PACKAGE_TYPE:chararray,
FLATTEN(C2::DESIGN_ID) AS DESIGN_ID:chararray, 
-- FLATTEN(C2::STEP_TEMPERATURE) AS STEP_TEMPERATURE:chararray, 
FLATTEN(C2::LEAD_COUNT) AS LEAD_COUNT:chararray,
FLATTEN(C2::PACKAGE_HEIGHT) AS PACKAGE_HEIGHT:chararray, 
FLATTEN(C2::PACKAGE_LENGTH) AS PACKAGE_LENGTH:chararray, 
FLATTEN(C2::PACKAGE_WIDTH) AS PACKAGE_WIDTH:chararray, 
FLATTEN(C2::PRODUCT_FAMILY) AS PRODUCT_FAMILY:chararray,
FLATTEN(C2::LOT_QTY) AS LOT_QTY:chararray,
FLATTEN(C2::reason) AS reason:chararray, 
FLATTEN(C2::action) AS action:chararray, 
FLATTEN(C2::quantity) AS quantity:chararray,
FLATTEN(C2::date_time_local) AS date_time_local:chararray,
-- B_OP::HANDLER_ID AS HANDLER_ID:chararray,
-- FLATTEN(B_OP::VERSION) AS VERSION:chararray,
FLATTEN(B_OP::SPEED) AS SPEED:chararray,
FLATTEN(B_OP::TEMPERATURE) AS TEMPERATURE:chararray,
-- B_OP::HANDLER_VENDOR AS HANDLER_VENDOR:chararray,
UPPER(B_OP::TESTER_ID) AS TESTER_ID:chararray,
ww_convert.get_ww(C2::date_time_local) AS ww:chararray,
FLATTEN(C2::partition_date) AS partition_date:chararray;


D1_OP_DIST = DISTINCT D1_OP;
D2_OP_DIST = DISTINCT D2_OP;


STORE D1_OP_DIST INTO '$dir_report_totallot' USING PigStorage(',');
STORE D2_OP_DIST INTO '$dir_report_variance' USING PigStorage(',');"`;


#`hadoop fs -text $dir_report_totallot/* > $output_filepath_all`;
#`hadoop fs -text $dir_report_variance/* > $output_filepath_variance`;


#}
