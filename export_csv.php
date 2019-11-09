<?php
	require_once('./util/mysqli.php');

	$out_dir = './data/safe/';

	$database = 'hackathon';

	$list_tables_sql = "SHOW TABLES IN $database";
	$result = f_mysql_query($list_tables_sql);

	while($row = mysqli_fetch_array($result)){
		$table = $row[0];
		$output_file = "$out_dir$table.safe.csv";
		echo "Exporting $database $table to $output_file\n";
		export_db_table($database,$table,$output_file);
	}


//	export_db_table('hackathon','travis211','./data/travis211.safe.csv');

function export_db_table($database,$tablename,$outfile_name){
	$query = "SELECT * FROM $database.$tablename";

	
	$result = f_mysql_query($query);
	if (!$result) die('Couldn\'t f	etch records');
	$headers = $result->fetch_fields();
	$col_names = [];
	foreach($headers as $header) {
    		$col_names[] = $header->name;
	}
	if(file_exists($outfile_name)){
		echo "$outfile_name already exists... doing nothing\n";
		exit();	
	}
	$fp = fopen($outfile_name, 'w');

	if ($fp && $result) {
    		fputcsv($fp, array_values($col_names)); 
    		while ($row = $result->fetch_array(MYSQLI_NUM)) {
        		fputcsv($fp, array_values($row));
    		}
	}else{
		echo "could not open the file or get the mysql result\n";
	}

}
