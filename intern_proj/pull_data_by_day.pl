#!/usr/bin/perl


#use strict;
use warnings;
use diagnostics;
use POSIX qw/strftime/;

#change directory
chdir("/home/hdfsbe/test_quality/");

#set queue name for hadoop query

my $queueName = "BDML_SINGAPORE";

chomp($queueName);

#for(my $n=31;$n<=360;$n++){

#obtain desired date

my $epoch = time(); # - 4 * 24 * 60 * 60;

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

my $output_filepath_totallot = "/home/home/hdfsbe/test_quality/report/" . "$selected_Date_2\_totallot.csv"; 
my $output_filepath_variance = "/home/home/hdfsbe/test_quality/report/" . "$selected_Date_2\_variance.csv"; 

my $dir_report_variance = "/eng/mti/ww/be/msb/test_quality/auto_variance/$selected_Date_2";
my $dir_report_totallot = "/eng/mti/ww/be/msb/test_quality/auto_totallot/$selected_Date_2";

`hadoop fs -text $dir_report_totallot/* > $output_filepath_totallot`;
`hadoop fs -text $dir_report_variance/* > $output_filepath_variance`;

#}