<?php
	require_once('./util/mysqli.php');

	$out_dir = './data/mysql/';

	$database = 'hackathon';

	$list_tables_sql = "SHOW TABLES IN $database";
	$result = f_mysql_query($list_tables_sql);

	$password = readline('password:');

	while($row = mysqli_fetch_array($result)){
		$table = $row[0];
		$output_file = "$out_dir$table.safe.sql";
		echo "Exporting $database $table to $output_file\n";
		$cmd = "mysqldump -u root --password=$password $database $table > $output_file";
		system($cmd);
	}

