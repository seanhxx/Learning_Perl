#!/usr/bin/perl -w


#use strict;
use warnings;
use diagnostics;
use POSIX;
use Time::Local;

#change directory
chdir("/home/hdfsbe/test_quality/");

#generate ww range string

my $timestamp_end_week = time();
my $timestamp_start_week = $timestamp_end_week - 51 * 7 * 24 * 60 * 60;
my $ww_end = Convert_weekNumber($timestamp_end_week);
my $ww_start = Convert_weekNumber($timestamp_start_week);
my $ww_range = Generate_wwStr($ww_start,$ww_end);

sub Convert_weekNumber{
	my $timestamp = $_[0];
	my $timestamp_reference = timelocal("00","00","11","29","11","2016");
	my $timestamp_diff = $timestamp - $timestamp_reference;
	my $week_diff = floor($timestamp_diff / (7 * 24 * 60 * 60));
	my $week_num = $week_diff % 52 + 1;
	my $year_num = 2017 + floor($week_diff / 52);
	my $ww_output = $year_num . sprintf("%02d", $week_num);
	return $ww_output;
}

sub Generate_wwStr{
	my $start_ww = $_[0];
	my $start_ww_year = substr($start_ww,0,4);
	my $start_ww_no = substr($start_ww,4,2);

	my $end_ww = $_[1];
	my $end_ww_year = substr($end_ww,0,4);
	my $end_ww_no = substr($end_ww,4,2);

	my $str_ww_range="\'" . $start_ww_year . $start_ww_no . "\'" . ",";

	while($start_ww_year ne $end_ww_year or $start_ww_no ne $end_ww_no){
		if ($start_ww_no eq "52"){		
			$start_ww_no="01";
			$start_ww_year++;
			$str_ww_range=$str_ww_range . "\'" . $start_ww_year . $start_ww_no . "\'" . ",";
		}else{
			$start_ww_no++;
			$str_ww_range=$str_ww_range . "\'" . $start_ww_year . $start_ww_no . "\'" . ",";
		}
	}

	$str_ww_range =~ s/,$//;
	return $str_ww_range;
}


#create temp dir for pulling history data

my $temp_dir_variance = "/eng/mti/ww/be/msb/test_quality/Temp/variance_history_data";
chomp($temp_dir_variance);
`hadoop fs -rm -r $temp_dir_variance`;
`hadoop fs -mkdir $temp_dir_variance`;

my $temp_dir_totallot = "/eng/mti/ww/be/msb/test_quality/Temp/totallot_history_data";
chomp($temp_dir_totallot);
`hadoop fs -rm -r $temp_dir_totallot`;
`hadoop fs -mkdir $temp_dir_totallot`;


#hive code

`beeline -u "jdbc:hive2://fshdpprod01-hive.imfs.micron.com:10010/default;principal=hive/fslhdppname2.imfs.micron.com\@HADOOP.MICRON.COM" -e "INSERT OVERWRITE DIRECTORY '$temp_dir_variance' 
SELECT DISTINCT * FROM eng_mti_ww_be_msb_test_quality.auto_variance WHERE ww IN ($ww_range)"`;

`beeline -u "jdbc:hive2://fshdpprod01-hive.imfs.micron.com:10010/default;principal=hive/fslhdppname2.imfs.micron.com\@HADOOP.MICRON.COM" -e "INSERT OVERWRITE DIRECTORY '$temp_dir_totallot' 
SELECT DISTINCT * FROM eng_mti_ww_be_msb_test_quality.auto_totallot WHERE ww IN ($ww_range)"`;

my $htable_path_variance = "/eng/mti/ww/be/msb/test_quality/report_variance";
`hadoop fs -rm -r $htable_path_variance/*`;

my $temp_dir_variance_report = $htable_path_variance . "/variance_$ww_start-$ww_end";
chomp($temp_dir_variance_report);


my $htable_path_totallot = "/eng/mti/ww/be/msb/test_quality/report_totallot";
`hadoop fs -rm -r $htable_path_totallot/*`;

my $temp_dir_totallot_report = $htable_path_totallot . "/totallot_$ww_start-$ww_end";
chomp($temp_dir_totallot_report);


#totallot

`pig -e "A = LOAD '$temp_dir_totallot/*' USING PigStorage('\\u0001') AS 
(totallot_LOT_ID:chararray,
 totallot_STEP_ID:chararray,
 totallot_DESIGN_ID:chararray, 
 totallot_PACKAGE_TYPE:chararray, 
 totallot_PRODUCT_FAMILY:chararray,
 totallot_TESTER_ID:chararray,
 totallot_LOT_QTY:chararray,
 totallot_date_time_local:chararray,
 totallot_ww:chararray,
 totallot_partition_date:chararray
);

A_DIST = DISTINCT A;

STORE A_DIST INTO '$temp_dir_totallot_report' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'WINDOWS', 'WRITE_OUTPUT_HEADER');"`;

#variance

`pig -e "B = LOAD '$temp_dir_variance/*' USING PigStorage('\\u0001') AS 
(variance_LOT_ID:chararray, 
 variance_STEP_ID:chararray,
 variance_DESIGN_ID:chararray, 
 variance_PACKAGE_TYPE:chararray, 
 variance_STEP_TEMPERATURE:chararray,
 variance_LEAD_COUNT:chararray,
 variance_PACKAGE_HEIGHT:chararray, 
 variance_PACKAGE_LENGTH:chararray, 
 variance_PACKAGE_WIDTH:chararray, 
 variance_PRODUCT_FAMILY:chararray,
 variance_TESTER_ID:chararray,
 variance_LOT_QTY:chararray,
 variance_reason:chararray, 
 variance_action:chararray, 
 variance_quantity:chararray,
 variance_date_time_local:chararray,
 variance_ww:chararray,
 variance_partition_date:chararray
);

B_OP = FOREACH B GENERATE
variance_LOT_ID, 
variance_STEP_ID,
variance_DESIGN_ID, 
variance_PACKAGE_TYPE, 
-- variance_STEP_TEMPERATURE,
variance_LEAD_COUNT,
variance_PACKAGE_HEIGHT, 
variance_PACKAGE_LENGTH, 
variance_PACKAGE_WIDTH, 
variance_PRODUCT_FAMILY,
variance_TESTER_ID,
variance_LOT_QTY,
variance_reason, 
variance_action, 
variance_quantity,
variance_date_time_local,
variance_ww,
variance_partition_date;

B_OP_DIST = DISTINCT B_OP;

STORE B_OP_DIST INTO '$temp_dir_variance_report' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'WINDOWS', 'WRITE_OUTPUT_HEADER');"`;

my $path = "/home/hdfsbe/test_quality/report/";
my $output_filepath_totallot = $path . "totallot_$ww_start-$ww_end.csv"; 
my $output_filepath_variance = $path . "variance_$ww_start-$ww_end.csv"; 

#`hadoop fs -text $temp_dir_totallot_report/* > $output_filepath_totallot`;
#`hadoop fs -text $temp_dir_variance_report/* > $output_filepath_variance`;
