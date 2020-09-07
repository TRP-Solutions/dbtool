<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/
require_once "tablediff.php";
class PermissionDiff {
	private $ignore_host, $files;

	public function __construct($files){
		$this->ignore_host = defined('PERMISSION_IGNORE_HOST') && PERMISSION_IGNORE_HOST;
		$this->files = $files;
	}

	public function run(&$partial_result = []){
		Tablediff::reset();
		$users = [];
		$filter = [];
		foreach($this->files as $file){
			$stmts = [];
			foreach($file->get_all_stmts() as $stmt){
				$obj = SQLFile::parse_statement($stmt, ['ignore_host'=>$this->ignore_host]);
				if($obj['type'] != 'grant' && $obj['type'] != 'revoke') continue;
				$stmts[] = $obj;
			}
			$filename = $file->get_filename();
			foreach($stmts as $stmt){
				Tablediff::file_statement($stmt, $filename);
				if(!in_array($stmt['user'], $users)) $users[] = $stmt['user'];
				$pairkey = $stmt['database'].':'.$stmt['user'];
				$filter[$pairkey] = true;
			}
		}
		$db = $this->get_dbdata($users,$filter);
		foreach($db as $stmt){
			Tablediff::database_statement($stmt);
		}
		Tablediff::diff_all();
		$mode = 0;
		$config_modes = Config::get('mode');
		if(in_array('grant', $config_modes) || empty($config_modes)){
			$mode = $mode | Tablediff::GRANT;
		}
		if(in_array('revoke', $config_modes)|| empty($config_modes)){
			$mode = $mode | Tablediff::REVOKE;
		}
		Tablediff::write_result($partial_result, $mode);
		return $partial_result;
	}

	private function get_table($stmt, $native_db = null){
		if(!isset($native_db)) $native_db = Config::get('database');
		$database = preg_replace('/^`([^`]*)`$/','$1',$stmt['database']);
		$table = preg_replace('/^`([^`]*)`$/','$1',$stmt['table']);
		//if($database == $native_db) return $table;
		return $database.'.'.$table;
	}

	private function get_dbdata($users, $filter = []){
		$grants = [];
		if(DB::$isloggedin){
			$db = Config::get('database');
			
			/*
			$result = DB::sql("SELECT * FROM `information_schema`.`schema_privileges` WHERE `table_schema` = '$db'");
			foreach($result as $row){
				$desc = Format::grant_row_to_description($row);
				$this->merge_into_grants($grants, $desc);
			}
			*/
			$result = DB::sql("SELECT * FROM `information_schema`.`table_privileges` WHERE `table_schema` = '$db'");
			foreach($result as $row){
				$desc = Format::grant_row_to_description($row);
				$this->merge_into_grants($grants, $desc);
			}
			$result = DB::sql("SELECT * FROM `information_schema`.`column_privileges` WHERE `table_schema` = '$db' ORDER BY grantee");
			$rows = [];
			foreach($result as $row){
				$rows[] = json_encode($row);
				$desc = Format::grant_row_to_description($row);
				$this->merge_into_grants($grants, $desc);
			}
			foreach($users as $user){
				if(preg_match("/^'([^']*)'(@'[^']+')?$/", $user, $matches)){
					$result = DB::sql("SELECT 1 FROM mysql.user WHERE user='$matches[1]'");
					if(!$result || !$result->num_rows){
						continue;
					}
					$result = DB::sql("SHOW GRANTS FOR $user");
					if($result){
						while($row = $result->fetch_row()){
							$obj = SQLFile::parse_statement($row[0], ['ignore_host'=>$this->ignore_host]);
							if($this->desc_is_allowed($obj,$filter)){
								$grants[$obj['key']] = $obj;
								$raw[$obj['key']] = $row[0];
							}
							
						}
					}
				}
			}
		}
		return $grants;
	}

	private function desc_is_allowed($desc, $filter){
		$pairkey = $desc['database'].':'.$desc['user'];
		return empty($filter) || isset($filter[$pairkey]) && $filter[$pairkey];
	}

	private function merge_into_grants(&$grants, $desc){
		if(!isset($grants[$desc['key']])){
			$grants[$desc['key']] = $desc;
		} else {
			foreach($desc['priv_types'] as $priv_type => $value){
				if(is_array($value) && isset($grants[$desc['key']]['priv_types'][$priv_type]) && is_array($grants[$desc['key']]['priv_types'][$priv_type])){
					foreach($value['column_list'] as $column_name){
						$grants[$desc['key']]['priv_types'][$priv_type]['column_list'][] = $column_name;
					}
				} else {
					$grants[$desc['key']]['priv_types'][$priv_type] = $value;
				}
			}
		}
	}
}
