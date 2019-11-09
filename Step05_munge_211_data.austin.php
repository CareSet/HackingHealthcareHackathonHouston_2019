<?php
	//this allows us to call the run_sql_loop function later on
	//it also loads our SQL connection...
	require_once('util/run_sql_loop.function.php');

	//get the name of the database and the file from the argument list..
	if(!isset($argv[3])){
		echo "usage Step05_munge_211_data.php {database} {tablename} {file_to_import.csv}\n";
		exit();
	}else{
		$database = $argv[1];
		$tablename = $argv[2];
		$filename = $argv[3];
	}
	
	if(!file_exists($filename)){
		echo "We could not find $filename\n";
		exit();
	}

	$filename = realpath($filename); //to be specific	

	$sql = [];

//lets build a basic need table

	$sql["drop the need table"] = "DROP TABLE IF EXISTS $database.$tablename"."_need";

	$sql["create need table"] = "
CREATE TABLE $database.$tablename"."_need (
  `id` int(11) NOT NULL,
  `need_taxonomy` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL,
  `need_name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
";

	$sql["primary key need table"] = "
ALTER TABLE $database.$tablename"."_need
  ADD PRIMARY KEY (`id`);
";

	$sql["auto increment need table"] = "
ALTER TABLE $database.$tablename"."_need
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
";
	$sql["drop need link table"] = "DROP TABLE IF EXISTS $database.$tablename"."_to_need";

	$sql["create need link table"] = "
CREATE TABLE $database.$tablename"."_to_need (
  `call_id` int(11) NOT NULL,
  `need_id` int(11) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
";

	$sql["index need link table"] = "
ALTER TABLE $database.$tablename"."_to_need
  ADD PRIMARY KEY (`call_id`,`need_id`);
";


//and a basic agency table

	$sql["drop the the agency table"] = "DROP TABLE IF EXISTS $database.$tablename"."_agency";

	$sql["create the agency table"] = "
CREATE TABLE $database.$tablename"."_agency (
  `id` int(11) NOT NULL,
  `agency_identifier` int(11) NOT NULL,
  `agency_name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

";

	$sql["primary key agency table"] = "
ALTER TABLE $database.$tablename"."_agency
  ADD PRIMARY KEY (`id`);
";

	$sql["auto increment agency table"] = "
ALTER TABLE $database.$tablename"."_agency
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
";

	$sql["drop agency link table"] = "DROP TABLE IF EXISTS $database.$tablename"."_to_agency";

	$sql["create agency link table"] = "
CREATE TABLE $database.$tablename"."_to_agency (
  `call_id` int(11) NOT NULL,
  `agency_id` int(11) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
";

	$sql["index agency link table"] = "
ALTER TABLE $database.$tablename"."_to_agency
  ADD PRIMARY KEY (`call_id`,`agency_id`);
";



//and a basic referral table

	$sql["drop the the referral table"] = "DROP TABLE IF EXISTS $database.$tablename"."_referral";

	$sql["create the referral table"] = "
CREATE TABLE $database.$tablename"."_referral (
  `id` int(11) NOT NULL,
  `referral_identifier` int(11) NOT NULL,
  `referral_name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

";

	$sql["primary key referral table"] = "
ALTER TABLE $database.$tablename"."_referral
  ADD PRIMARY KEY (`id`);
";

	$sql["auto increment referral table"] = "
ALTER TABLE $database.$tablename"."_referral
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
";
	$sql["drop referral link table"] = "DROP TABLE IF EXISTS $database.$tablename"."_to_referral";

	$sql["create referral link table"] = "
CREATE TABLE $database.$tablename"."_to_referral (
  `call_id` int(11) NOT NULL,
  `referral_id` int(11) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
";

	$sql["index referral link table"] = "
ALTER TABLE $database.$tablename"."_to_referral
  ADD PRIMARY KEY (`call_id`,`referral_id`);
";


//then the main data table..


	$sql["dropping the table"] = "DROP TABLE IF EXISTS $database.$tablename\n";

	$sql["creating the table structure"] = "
CREATE TABLE $database.$tablename (
  `report_number` int(11) DEFAULT NULL,
  `date_of_call_start_text` varchar(25) DEFAULT NULL,
  `date_of_call_end_text` varchar(25) DEFAULT NULL,
  `length_of_call_minutes` int(11) DEFAULT NULL,
  `age` varchar(7) DEFAULT NULL,
  `gender` varchar(55) DEFAULT NULL,
  `military_status` varchar(55) DEFAULT NULL,
  `military_branch_veteran` varchar(55) DEFAULT NULL,
  `military_branch_active_duty` varchar(55) DEFAULT NULL,
  `preferred_language` varchar(255) DEFAULT NULL,
  `other_language` varchar(255) DEFAULT NULL,
  `zip_code` varchar(50) DEFAULT NULL,
  `city` varchar(255) DEFAULT NULL,
  `county` varchar(255) DEFAULT NULL,
  `need_name` varchar(1000) DEFAULT NULL,
  `need_taxonomy_code` varchar(1000) DEFAULT NULL,
  `met_or_reason_unmet` varchar(1000) DEFAULT NULL,
  `parent_agency_name` varchar(1000) DEFAULT NULL,
  `parent_agency_identifier` varchar(1000) DEFAULT NULL,
  `referral_name` varchar(1000) DEFAULT NULL,
  `referral_identifier` varchar(1000) DEFAULT NULL,
  `benefits_information_call` varchar(1000) DEFAULT NULL,
  `benefits_referral_call` varchar(1000) DEFAULT NULL,
  `benefits_transfer_call` varchar(1000) DEFAULT NULL,
  `first_time_user` varchar(1000) DEFAULT NULL,
  `referral_source_first_time_user` varchar(1000) DEFAULT NULL,
  `other_referral_source_first_time_user` varchar(1000) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

";

	//loops every every SQL command one at a time... 

	$sql["Load data statement to move the CSV data into the table"] = "
LOAD DATA INFILE '$filename' 
INTO TABLE $database.$tablename 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '\"'
LINES TERMINATED BY '\\n'
IGNORE 1 ROWS
";

	$sql["create uninque id per row for further data operations"] = "
ALTER TABLE $database.$tablename  ADD `id` INT(11) NOT NULL AUTO_INCREMENT  FIRST,  ADD   PRIMARY KEY  (`id`);
";

	$sql["add time cols"] = "
ALTER TABLE $database.$tablename  
	ADD `year` INT(11) NOT NULL  AFTER `report_number`,  
	ADD `week_number` INT(11) NOT NULL  AFTER `year`,  
	ADD `week_sunday` VARCHAR(15) NOT NULL  AFTER `week_number`,
	ADD `call_time_block` VARCHAR(25) NOT NULL  AFTER `week_sunday`,  
	ADD `call_order` INT(11) NOT NULL  AFTER `call_time_block`,  
	ADD `is_weekday` TINYINT(1) NOT NULL  AFTER `call_order`,
	ADD `age_bucket` VARCHAR(25) NOT NULL  AFTER `age`;
";

	

	run_sql_loop($sql,true);
