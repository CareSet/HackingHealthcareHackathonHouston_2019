<?php
	//this allows us to call the run_sql_loop function later on
	//it also loads our SQL connection...
	require_once('util/run_sql_loop.function.php');

	//get the name of the database and the file from the argument list..
	if(!isset($argv[3])){
		echo "usage Step10_munge_211_data.php {database} {tablename} {file_to_import.csv}\n";
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

	$sql["drop the need table"] = "DROP TABLE IF EXISTS $databae.$tablename"."_need";

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


//and a basic agency table

	$sql["drop the the agency table"] = "DROP TABLE IF EXISTS $database.$tablename"."_agency";

	$sql["create the agency table"] = "
CREATE TABLE $database.$tablename"."_agency (
  `id` int(11) NOT NULL,
  `agency_id` int(11) NOT NULL,
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


//and a basic referral table

	$sql["drop the the referral table"] = "DROP TABLE IF EXISTS $database.$tablename"."_referral";

	$sql["create the referral table"] = "
CREATE TABLE $database.$tablename"."_referral (
  `id` int(11) NOT NULL,
  `referral_id` int(11) NOT NULL,
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
  `zip_code` varchar(6) DEFAULT NULL,
  `city` varchar(255) DEFAULT NULL,
  `county` varchar(255) DEFAULT NULL,
  `need_name` varchar(1000) DEFAULT NULL,
  `need_taxonomy_code` varchar(1000) DEFAULT NULL,
  `met_or_reason_unmet` varchar(1000) DEFAULT NULL,
  `parent_agency_name` varchar(1000) DEFAULT NULL,
  `parent_agency_id` varchar(1000) DEFAULT NULL,
  `referral_name` varchar(1000) DEFAULT NULL,
  `referral_id` varchar(1000) DEFAULT NULL,
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


//if we get here, then we have created the database and made space for our new data structures... 	
//now we need to loop over the data and create our new column information with lots and lots of update statment
//lets start out by emptying sql

	$sql = [];


	//before we start the loop lets do an hour lookup table for our time block strings..
	$time_block_map = [
		0 => '0-3',
		1 => '0-3',
		2 => '0-3',
		3 => '0-3',
		4 => '4-7',
		5 => '4-7',
		6 => '4-7',
		7 => '4-7',
		8 => '8-11',
		9 => '8-11',
		10 => '8-11',
		11 => '8-11',
		12  => '12-15',
		13  => '12-15',
		14  => '12-15',
		15  => '12-15',
		16  => '16-19',
		17  => '16-19',
		18  => '16-19',
		19  => '16-19',
		20  => '20-24',
		21  => '20-24',
		22  => '20-24',
		23  => '20-24',
		24  => '20-24',
		];


	//run the sql..
	$select_sql = "SELECT * FROM $database.$tablename";
	$result = f_mysql_query($select_sql); //fyi f_mysql_query is just a wrapper for mysqli_query

	
	//prepare for the first looping of records...
	//where we are going to put data
	$need_data = [];
	$referral_data = [];
	$agency_data = [];
	$day_count = 1;
	$last_date_part = 'something';
	$row_count = 0;
	//this value limits the row import to a few thousand...
	$row_limit = 1000;
	//this value keeps the script running for all of the rows..
//	$row_limit = 999999999999999999999999999999999999999999;

	//run the first loop, which will calculate the data and update the rows as we go..
	while($row = mysqli_fetch_assoc($result)){
		$row_count++;
		//var_export($row);
		//exit();
		extract($row); //saves so much time..

		$age = $row['age'];
		$id = $row['id'];


		//a series of explode statements will allow us to get the specific values of the date and time string...
		//the going in format is '1/1/19 12:12'
		list($date_part,$time_part) = explode(' ',$date_of_call_start_text);
		list($month, $day, $two_digit_year) = explode('/',$date_part);
		list($hour,$second) = explode(':',$time_part);


		if($last_date_part == $date_part){
			$day_count++;
		}else{
			$day_count = 1;
		}

		//now lets calculate the time block.. 
		$time_block = $time_block_map[$hour];

		$this_year = "20$two_digit_year";

		$row_date_str = "$this_year-$month-$day";
		$timestamp = strtotime($row_date_str);
		$week_num = idate('W',$timestamp);

		$week_data = getStartAndEndDate($week_num,$this_year);
		$sunday_date = $week_data['week_start']; //this should be compatible with Google Correlate!!

		if(isWeekday($timestamp)){
			$is_weekday = 1;
		}else{
			$is_weekday = 0;
		}

		if(is_numeric($age)){
			$age_above = ceil($age/5) * 5;
			$age_below = floor($age/5) * 5;
			$age_range = "$age_below-$age_above";	
		}else{
			$age_range = 'unknown';
		}
		$update_sql = "
UPDATE $database.$tablename SET 
	year = '$this_year',
	week_number = '$week_num',
	week_sunday = '$sunday_date',
	call_time_block = '$time_block',
	is_weekday = '$is_weekday',
	call_order = '$day_count',
	age_bucket = '$age_range'
WHERE id = '$id'
; 

";
		f_mysql_query($update_sql);		
		echo '.';

		//yhaaa!! now we have a solid row of data built... but we also need to ensure that we are putting all of the other data in correctly..
		//and we are doing to be lazy... looping over the data twice to get this answer...
		//on this loop, we will just break out the data!!
		
		//the need field has a name and a taxonomy, in different fields..
		//and is seperated by an asterix

		$need_name_array = explode('*',$need_name);
		$need_taxonomy_array = explode('*',$need_taxonomy_code);
	
		if(count($need_name_array) != count($need_taxonomy_array)){
			echo "$need_name and $need_taxonomy_code are broken!!\n";
			var_export($need_name_array);
			var_export($need_taxonomy_array);
			exit();
		}

		foreach($need_name_array as $i => $need_name){
			$need_taxonomy = $need_taxonomy_array[$i];

			$need_data["$need_name-$need_taxonomy"] = [
					'need_name' => $need_name,
					'need_taxonomy' => $need_taxonomy
			];
		}

		$agency_name_array = explode(';',$parent_agency_name);	
		$agency_id_array = explode(';',$parent_agency_id);

		foreach($agency_id_array as $i => $agency_id){

			$agency_name = $agency_name_array[$i];
			$agency_data["$agency_name-$agency_id"] = [
					'agency_name' => $agency_name,
					'agency_id' => $agency_id,
				];
		}


		$referral_name_array = explode(';',$referral_name);	
		$referral_id_array = explode(';',$referral_id);
		
		foreach($referral_id_array as $i => $referral_id){
			$referral_name = $referral_name_array[$i];
			$referral_data["$referral_name-$referral_id"] = [
					'referral_name' => $referral_name,
					'referral_id' => $referral_id,
				];
		}

		if($row_count > $row_limit){
			break;
		}

	}

	//now we are going to create the database tables of the supporting data sets... 

	foreach($referral_data as $referral_row){
		extract($referral_row);

		if(strlen($referral_id) == 0){
			$referral_id = 0;
		}

		if(strlen($referral_name) > 0){
			$referral_name = f_mysql_real_escape_string($referral_name);
			$insert_sql = "
INSERT INTO $database.$tablename"."_referral (`id`, `referral_id`, `referral_name`) VALUES (NULL, '$referral_id', '$referral_name');
";

			f_mysql_query($insert_sql);
			echo 'r';
		}

	}

	foreach($agency_data as $agency_row){
		extract($agency_row);

		if(strlen("$agency_id") == 0){
			$agency_id = 0;
		}

		if(strlen($agency_name) > 0){
			$agency_name = f_mysql_real_escape_string($agency_name);
			$insert_sql = "
INSERT INTO $database.$tablename"."_agency (`id`, `agency_id`, `agency_name`) VALUES (NULL, '$agency_id', '$agency_name');
";

			f_mysql_query($insert_sql);
			echo 'a';
		}

	}


	foreach($need_data as $need_row){
		extract($need_row);


		if(strlen($need_name) > 0){
			$need_name = f_mysql_real_escape_string($need_name);
			$insert_sql = "
INSERT INTO $database.$tablename"."_need (`id`, `need_taxonomy`, `need_name`) VALUES (NULL, '$need_taxonomy', '$need_name');
";

			f_mysql_query($insert_sql);
			echo 'n';
		}

	}





function isWeekday($timestamp){
	return (date('N', $timestamp < 6));
}


function getStartAndEndDate($week, $year) {
  $dto = new DateTime();
  $ret['week_start'] = $dto->setISODate($year, $week)->format('Y-m-d');
  $ret['week_end'] = $dto->modify('+6 days')->format('Y-m-d');
  return $ret;
}
