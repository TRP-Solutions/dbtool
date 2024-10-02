<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/

declare(strict_types=1);
class Userdiff {
	private $name, $file_stmt, $db_stmt, $parsed_user = false, $diff_calculated = false, $diff, $sources = [];

	public function __construct($name){
		$this->name = $name;
	}

	public function from_file($stmt, $source){
		if(!isset($this->file_stmt)){
			$this->file_stmt = $stmt;
			if(isset($stmt['error'])){
				$this->errors[] = ['errno'=>1,'error'=>"Parse Error in file \"$source\" table `$stmt[name]`: $stmt[error]"];
			}
			$this->diff_calculated = false;
			$this->sources[] = $source;
		} else {
			$this->errors[] = ['errno'=>999,'error'=>'Userdiff->from_file uses self::compare which is not implemented yet'];
			return;
			$diff = self::compare($this->file_stmt, $stmt);
			if($diff['is_empty']){
				$this->sources[] = $source;
			} else {
				$msg = "Collision Error in file \"$source\" user `$this->name`:\nUser differs from a user with the same name and host in \"$this->sources[0]\"";
				$this->errors[] = ['errno'=>3,'error'=>$msg];
			}
		}
	}

	public function from_database($stmt){
		$this->errors[] = ['errno'=>999,'error'=>'Userdiff->from_database is not implemented yet'];
	}

	private function get_db_stmt(){
		if(!$this->parsed_user){
			try {
				$query = DB::sql("SHOW CREATE USER $this->name");
			} catch (Exception $e){
				$query = false;
			}
			if($query && $query->num_rows){
				$stmt = \Parser\statement($query->fetch_array()[0]);
				if(isset($stmt['error'])){
					$this->errors[] = ['errno'=>2,'error'=>"Parse Error in database user $this->name: $stmt[error]"];
				}
				$this->db_stmt = $stmt;
			}
			$this->parsed_user = true;
		}
		return $this->db_stmt;
	}

	public function diff(){
		if(!$this->diff_calculated){
			$db_stmt = $this->get_db_stmt();
			$file_stmt = $this->file_stmt;
			$this->diff_calculated = true;
			if(isset($db_stmt)){
				if(isset($file_stmt)){
					$this->diff = $this->generate_alter($db_stmt, $file_stmt);
				} else {
					$this->diff = 'drop';
				}
			} elseif(isset($file_stmt)){
				$this->diff = 'create';
			}
		}
		return $this->diff;
	}

	private function generate_alter($db, $file){
		$diff = ['alter_sql'=>"ALTER USER $this->name"];
		if($db['tls'] != $file['tls']){
			$db_tls = $this->flatten_tls($db);
			$file_tls = $this->flatten_tls($file);
			$diff['alter_options'][] = [
				't1'=>['TLS'=>$db_tls],
				't2'=>['TLS'=>$file_tls]
			];
			$diff['alter_sql'] .= ' REQUIRE '.$file_tls;
		}
		if(!empty($diff['alter_options'])){
			return $diff;
		}
	}

	private function flatten_tls($stmt){
		if(empty($stmt['tls'])) return 'NONE';
		if(!is_array($stmt['tls'])) return $stmt['tls'];
		$tls = [];
		foreach($stmt['tls'] as $tls_option){
			$tls[] = "$tls_option[0] '$tls_option[1]'";
		}
		return implode(' AND ',$tls);
	}

	public function get_alter(){
		$diff = $this->diff();
		if(is_array($diff)){
			return [
				'type'=>'alter_user',
				'name'=>$this->name,
				'sources'=>array_unique($this->sources),
				'sql'=>[$diff['alter_sql']],
				'options'=>$diff['alter_options']
			];
		}
	}

	public function get_create(){
		$diff = $this->diff();
		if($diff == 'create'){
			return [
				'type'=>'create_user',
				'name'=>$this->name,
				'sources'=>array_unique($this->sources),
				'sql'=>[$this->file_stmt['statement']]
			];
		}
	}

	public function get_drop(){
		$diff = $this->diff();
		if($diff == 'drop'){
			return [
				'type'=>'drop_user',
				'name'=>$this->name,
				'sql'=>["DROP USER $this->name"]
			];
		}
	}
}
