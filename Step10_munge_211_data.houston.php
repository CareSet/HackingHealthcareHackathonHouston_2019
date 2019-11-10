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
//	$row_limit = 1000;
	//this value keeps the script running for all of the rows..
	$row_limit = 999999999999999999999999999999999999999999;

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

		$last_date_part = $date_part;

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
			if($age_above == $age_below){
				$age_above = $age_above + 5;
			}
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
//			exit();
		}

		foreach($need_name_array as $i => $need_name){
			$need_name = trim($need_name);
			if(isset($need_taxonomy_array[$i])){
				$need_taxonomy = trim($need_taxonomy_array[$i]);
			}else{
				$need_taxonomy = 'unknown';
			}
			
			$need_data["$need_name-$need_taxonomy"] = [
					'need_name' => $need_name,
					'need_taxonomy' => $need_taxonomy
			];
		}

		$agency_name_array = explode(';',$parent_agency_name);	
		$agency_id_array = explode(';',$parent_agency_identifier);

		foreach($agency_id_array as $i => $agency_identifier){
			$agency_identifier = trim($agency_identifier);
			$agency_name = trim($agency_name_array[$i]);
			$agency_data["$agency_name-$agency_identifier"] = [
					'agency_name' => $agency_name,
					'agency_identifier' => $agency_identifier,
				];
		}


		$referral_name_array = explode(';',$referral_name);	
		$referral_id_array = explode(';',$referral_identifier);
		
		foreach($referral_id_array as $i => $referral_identifier){
			$referral_identifier = trim($referral_identifier);
			$referral_name = trim($referral_name_array[$i]);
			$referral_data["$referral_name-$referral_identifier"] = [
					'referral_name' => $referral_name,
					'referral_identifier' => $referral_identifier,
				];
		}

		if($row_count > $row_limit){
			break;
		}

	}

	//now we are going to create the database tables of the supporting data sets... 

	foreach($referral_data as $referral_row){
		extract($referral_row);

		if(strlen($referral_identifier) == 0){
			$referral_identifier = 0;
		}

		if(strlen($referral_name) > 0){
			$referral_name = f_mysql_real_escape_string($referral_name);
			$insert_sql = "
INSERT INTO $database.$tablename"."_referral (`id`, `referral_identifier`, `referral_name`) VALUES (NULL, '$referral_identifier', '$referral_name');
";

			f_mysql_query($insert_sql);
			echo 'r';
		}

	}

	foreach($agency_data as $agency_row){
		extract($agency_row);

		if(strlen("$agency_identifier") == 0){
			$agency_identifier = 0;
		}

		if(strlen($agency_name) > 0){
			$agency_name = f_mysql_real_escape_string($agency_name);
			$insert_sql = "
INSERT INTO $database.$tablename"."_agency (`id`, `agency_identifier`, `agency_name`) VALUES (NULL, '$agency_identifier', '$agency_name');
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


	//now we load the data back for need
	$need_query = "SELECT * FROM  $database.$tablename"."_need";
	$result = f_mysql_query($need_query);
	$need_data = [];
	while($row = mysqli_fetch_assoc($result)){
		$need_data[$row['need_taxonomy']] = $row;
	}

	//now we load the data back for agency
	$agency_query = "SELECT * FROM  $database.$tablename"."_agency";
	$result = f_mysql_query($agency_query);
	$agency_data = [];
	while($row = mysqli_fetch_assoc($result)){
		$agency_data[$row['agency_identifier']] = $row;
	}

	//now we load the data back 
	$referral_query = "SELECT * FROM  $database.$tablename"."_referral";
	$result = f_mysql_query($referral_query);
	$referral_data = [];
	while($row = mysqli_fetch_assoc($result)){
		$referral_data[$row['referral_identifier']] = $row;
	}


	///noww... we loop over the main data again...

	$select_sql = "SELECT * FROM $database.$tablename";
	$result = f_mysql_query($select_sql); //fyi f_mysql_query is just a wrapper for mysqli_query

	//the second loop we perform the linking...	
	$row_count = 0;
//	while($row = mysqli_fetch_assoc($result)){
	while(false){
		$row_count++;

		$id = $row['id'];


		extract($row); //cannot forget

		$need_taxonomy_array = explode('*',$need_taxonomy_code);
		$agency_id_array = explode(';',$parent_agency_identifier);
		$referral_id_array = explode(';',$referral_identifier);

		foreach($need_taxonomy_array as $this_need_taxonomy){
			if(isset($need_data[$this_need_taxonomy])){
				$need_id = $need_data[$this_need_taxonomy]['id'];
				$insert_sql = "INSERT IGNORE INTO $database.$tablename"."_to_need (`call_id`,`need_id`) VALUES ('$id','$need_id')";
				f_mysql_query($insert_sql);
			}
	//		echo 'ln';
		}

		foreach($agency_id_array as $this_agency_identifier){
			if(!isset($agency_data[$this_agency_identifier])){
			//	echo "xa";
			}else{

				$agency_id = $agency_data[$this_agency_identifier]['id'];
				$insert_sql = "INSERT IGNORE INTO $database.$tablename"."_to_agency (`call_id`,`agency_id`) VALUES ('$id','$agency_id')";
				f_mysql_query($insert_sql);
			//	echo 'la';
			}
		}
			
		foreach($referral_id_array as $this_referral_identifier){
			if(isset($referral_data[$this_referral_identifier])){
				$referral_id = $referral_data[$this_referral_identifier]['id'];
				$insert_sql = "INSERT IGNORE INTO $database.$tablename"."_to_referral (`call_id`,`referral_id`) VALUES ('$id','$referral_id')";
				f_mysql_query($insert_sql);
//				echo "$this_referral_identifier->$referral_id ";
			}
		}

		echo 'l';

		if($row_count > $row_limit){
			break;
		}

	}

	$drop_extra_columns_sql = "
ALTER TABLE $database.$tablename
  DROP `date_of_call_start_text`,
  DROP `date_of_call_end_text`,
  DROP `age`,
  DROP `city`,
  DROP `county`;
";

	f_mysql_query($drop_extra_columns_sql);
	


	echo "\nall done.\n";


function isWeekday($timestamp){
	return (date('N', $timestamp < 6));
}


function getStartAndEndDate($week, $year) {
  $dto = new DateTime();
  $ret['week_start'] = $dto->setISODate($year, $week)->format('Y-m-d');
  $ret['week_end'] = $dto->modify('+6 days')->format('Y-m-d');
  return $ret;
}
