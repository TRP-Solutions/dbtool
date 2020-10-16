<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/
require_once __DIR__."/permissiondiff.php";
require_once __DIR__."/definitiondiff.php";
class Tablediff {
	const CREATE = 0b1;
	const ALTER = 0b10;
	const DROP = 0b100;
	const GRANT = 0b1000;
	const REVOKE = 0b10000;

	static private $tables = [];

	private $name, $permissions = [], $definition = null,
		$sourcefiles = [],
		$create = [], $create_sql = [],
		$alter  = [], $alter_sql  = [],
		$drop   = [], $drop_sql   = [],
		$grant  = [], $grant_sql  = [],
		$revoke = [], $revoke_sql = [],
		$errors = [];

	static public function run($files){
		self::reset();

		$users = [];
		$user_filter = [];
		$ignore_host = self::ignore_host();
		foreach($files as $file){
			$filename = $file->get_filename();
			$stmts = $file->get_all_stmts();
			if(is_array($stmts)) foreach($stmts as $stmt){
				$obj = SQLFile::parse_statement($stmt, ['ignore_host'=>$ignore_host]);
				$is_grant = $obj['type'] == 'grant' || $obj['type'] == 'revoke';
				if($is_grant || $obj['type'] == 'table'){
					self::file_statement($obj, $filename);
					if($is_grant){
						if(!in_array($obj['user'], $users)) $users[] = $obj['user'];
						$pairkey = $obj['database'].':'.$obj['user'];
						$user_filter[$pairkey] = true;
					}
				}
			}
		}

		$db = self::load_db_permissions($users,$user_filter);
		foreach($db as $stmt){
			self::database_statement($stmt);
		}

		return self::write_result();
	}

	static public function reset(){
		self::$tables = [];
	}

	static public function get($name){
		if(!isset(self::$tables[$name])){
			self::$tables[$name] = new Self($name);
		}
		return self::$tables[$name];
	}

	static public function write_result(){
		$result = [
			'errors' =>[],
			'tables'=>[],
			'db_only_tables'=>[],
			'drop_queries'=>[],
			'create_database'=>null
		];

		$mode = self::read_mode();
		$create = (bool) ($mode & self::CREATE);
		$alter = (bool) ($mode & self::ALTER);
		$drop = (bool) ($mode & self::DROP);
		$grant = (bool) ($mode & self::GRANT);
		$revoke = (bool) ($mode & self::REVOKE);
		$create_database_sql = self::load_db_tables();
		if(isset($create_database_sql)){
			$result['create_database'] = $create_database_sql;
		}
		foreach(self::$tables as $name => $table){
			$table->diff();
			if(!empty($table->errors)){
				foreach($table->errors as $error){
					$result['errors'][] = $error;
				}
			}
			$table_obj = null;
			if($alter && !empty($table->alter)){
				$table_obj = self::init_table($name, $table->sourcefiles) + $table->alter;
			} elseif($create && !empty($table->create)){
				$table_obj = self::init_table($name, $table->sourcefiles,'file_only') + [
					'filetable'=>$table->create,
					'sql'=>[$table->create_sql]
				];
			} elseif($drop && !empty($table->drop)){
				$result['db_only_tables'][] = $table->drop;
				$result['drop_queries'][$table->drop] = $table->drop_sql;
			}
			if($revoke && !empty($table->revoke)){
				if(!isset($table_obj)){
					$table_obj = self::init_table($name, $table->sourcefiles);
				}
				foreach($table->revoke as $key => $row){
					$table_obj['permissions'][$key.'-revoke'] = $row;
					if(isset($table->revoke_sql[$key])) $table_obj['sql'][$key.'-revoke'] = $table->revoke_sql[$key];
				}
			}
			if($grant && !empty($table->grant)){
				if(!isset($table_obj)){
					$table_obj = self::init_table($name, $table->sourcefiles);
				}
				foreach($table->grant as $key => $row){
					$table_obj['permissions'][$key.'-grant'] = $row;
					if(isset($table->grant_sql[$key])) $table_obj['sql'][$key.'-grant'] = $table->grant_sql[$key];
				}
			}
			if(isset($table_obj)){
				$result['tables'][$name] = $table_obj;
			}
		}
		return $result;
	}

	static private function init_table($name, $sourcefiles, $type = null){
		if(!isset($type)) $type = empty($sourcefiles) ? 'database_only' : 'intersection';
		return ['name'=>$name,'type'=>$type,'sourcefiles'=>array_unique($sourcefiles)];
	}

	static private function read_mode(){
		$mode = 0;
		$config_modes = Config::get('statement');
		if(empty($config_modes)){
			$mode = self::CREATE | self::ALTER | self::DROP | self::GRANT | self::REVOKE;
		} else {
			foreach($config_modes as $config_mode){
				$config_mode = strtolower($config_mode);
				switch($config_mode){
					case 'create': $mode |= self::CREATE; break;
					case 'alter': $mode |= self::ALTER; break;
					case 'drop': $mode |= self::DROP; break;
					case 'grant': $mode |= self::GRANT; break;
					case 'revoke': $mode |= self::REVOKE; break;
				}
			}
		}
		return $mode;
	}

	static private function load_db_tables(){
		$dbname = DB::escape(Config::get('database'));
		if(empty($dbname)) return;

		$query = DB::sql("SHOW DATABASES LIKE '$dbname';");
		if($query->num_rows){
			DB::sql("USE `$dbname`;");
			$query = DB::sql("SHOW TABLES IN `$dbname`;");
			if($query) while($row = $query->fetch_row()){
				self::known_database_table($row[0]);
			}
		} else {
			return "CREATE DATABASE IF NOT EXISTS `$dbname`;";
		}
	}

	static public function load_db_permissions($users, $filter = []){
		$grants = [];
		if(DB::$isloggedin){
			$db = Config::get('database');
			
			/*
			$result = DB::sql("SELECT * FROM `information_schema`.`schema_privileges` WHERE `table_schema` = '$db'");
			foreach($result as $row){
				$desc = Format::grant_row_to_description($row);
				self::merge_into_grants($grants, $desc);
			}
			*/
			$result = DB::sql("SELECT * FROM `information_schema`.`table_privileges` WHERE `table_schema` = '$db'");
			foreach($result as $row){
				$desc = Format::grant_row_to_description($row);
				self::merge_into_grants($grants, $desc);
			}
			$result = DB::sql("SELECT * FROM `information_schema`.`column_privileges` WHERE `table_schema` = '$db' ORDER BY grantee");
			$rows = [];
			foreach($result as $row){
				$rows[] = json_encode($row);
				$desc = Format::grant_row_to_description($row);
				self::merge_into_grants($grants, $desc);
			}
			foreach($users as $user){
				$re1 = "^[']([^']*)['](?:@['][^']+['])?$";
				$re2 = "^[`]([^`]*)[`](?:@[`][^`]+[`])?$";
				$re = "/(?:$re1)|(?:$re2)/";
				if(preg_match($re, $user, $matches)){
					$username = empty($matches[1]) ? $matches[2] : $matches[1];
					$result = DB::sql("SELECT 1 FROM mysql.user WHERE user='$username'");
					if(!$result || !$result->num_rows){
						continue;
					}
					$result = DB::sql("SHOW GRANTS FOR $user");
					if($result){
						while($row = $result->fetch_row()){
							$obj = SQLFile::parse_statement($row[0], ['ignore_host'=>self::ignore_host()]);
							if(self::desc_is_allowed($obj,$filter)){
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

	static private function ignore_host(){
		return defined('PERMISSION_IGNORE_HOST') && PERMISSION_IGNORE_HOST;
	}

	static private function desc_is_allowed($desc, $filter){
		$pairkey = $desc['database'].':'.$desc['user'];
		return empty($filter) || isset($filter[$pairkey]) && $filter[$pairkey];
	}

	static private function merge_into_grants(&$grants, $desc){
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

	private function __construct($name){
		$this->name = $name;
	}

	static public function file_statement($stmt, $filename){
		$name = self::get_table_name($stmt);
		$diff = self::get($name);
		if($stmt['type'] == 'table'){
			if(!isset($diff->definition)) {
				$diff->definition = new Definitiondiff($name);
			}
			$diff->definition->from_file($stmt, $filename);
		} elseif($stmt['type'] == 'grant'){
			if(!isset($stmt['key']) || !isset($stmt['database']) || !isset($stmt['table'])) debug($stmt);
			$key = $stmt['key'];
			if(!isset($diff->permissions[$key])){
				$diff->permissions[$key] = new Permissiondiff($key);
			}
			$diff->permissions[$key]->from_file($stmt, $filename);
		}
		$diff->sourcefiles[] = $filename;
	}

	static public function database_statement($stmt){
		$name = self::get_table_name($stmt);
		$diff = self::get($name);
		if($stmt['type'] == 'table'){
			if(!isset($diff->definition)) {
				$diff->definition = new Definitiondiff($name);
			}
			$diff->definition->from_database($stmt);
		} elseif($stmt['type'] == 'grant'){
			if(!isset($stmt['key']) || !isset($stmt['database']) || !isset($stmt['table'])) debug($stmt);
			$key = $stmt['key'];
			if(!isset($diff->permissions[$key])){
				$diff->permissions[$key] = new Permissiondiff($key);
			}
			$diff->permissions[$key]->from_database($stmt);
		}
	}

	static public function known_database_table($name){
		$fullname = self::get_table_name(['name'=>$name,'type'=>'table']);
		$diff = self::get($fullname);
		if(!isset($diff->definition)) {
			$diff->definition = new Definitiondiff($fullname);
		}
	}

	static private function get_table_name($stmt){
		if(isset($stmt['database'])){
			$database = preg_replace('/^`([^`]*)`$/','$1',$stmt['database']);
		} else {
			$database = Config::get('database');
		}
		if($stmt['type'] == 'grant'){
			$table = preg_replace('/^`([^`]*)`$/','$1',$stmt['table']);
		} else {
			if(!isset($stmt['name'])){
				debug($stmt);
			}
			
			$table = $stmt['name'];
		}
		return $database.'.'.$table;
	}

	private function diff(){
		if(isset($this->definition)){
			list($this->create, $this->create_sql) = $this->definition->get_create();
			list($this->drop, $this->drop_sql) = $this->definition->get_drop();
			$this->alter = $this->definition->get_alter();
			$errors = $this->definition->get_errors();
			if(!empty($errors)){
				$this->errors = array_merge($this->errors, $errors);
			}
		}
		foreach($this->permissions as $key => $statement){
			if(isset($statement->merge_error)){
				$this->errors[] = ['errno'=>5,'error'=>'File parse conflict: '.$statement->merge_error];
				$i = 1;
				foreach($statement->get_unmerged() as $row){
					$this->grant[$key.'-'.$i] = self::create_data_row('Schemafile (merge error)', $row, 'bg-warning');
					$this->grant_sql[$key.'-'.$i] = self::convert_grant_to_sql($row, 'GRANT');
					$i += 1;
				}
			} else {
				$grant = $statement->get_grant();
				if(isset($grant)){
					$title = 'Schemafile'.($statement->is_merged ? ' (merged)':'');
					$this->grant[$key] = self::create_data_row($title, $grant, 'bg-success');
					$this->grant_sql[$key] = self::convert_grant_to_sql($grant, 'GRANT');
				}
			}
			$revoke = $statement->get_revoke();
			if(isset($revoke)){
				$this->revoke[$key] = self::create_data_row("Database", $revoke, 'bg-danger');
				$this->revoke_sql[$key] = self::convert_grant_to_sql($revoke, 'REVOKE');
			}
		}
	}

	static private function create_data_row($location, $stmt, $class = ''){
		$data = ['location' => $location];
		return ['data' => $data + self::flatten_stmt_obj($stmt), 'class' => $class];
	}

	static private function flatten_stmt_obj($stmt){
		if(isset($stmt['priv_types']) && is_array($stmt['priv_types'])){
			foreach($stmt['priv_types'] as $key => $type){
				if(is_array($type)){
					$stmt['priv_types'][$key] = $type['priv_type'] .' (`'.implode('`, `',$type['column_list']).'`)';
				}
			}
			$stmt['priv_types'] = implode(', ', $stmt['priv_types']);
		}
		return $stmt;
	}

	static private function convert_grant_to_sql($stmt, $action){
		if(!isset($stmt['type']) || $stmt['type'] != 'grant') return false;
		$stmt = self::flatten_stmt_obj($stmt);
		$sql = "$action $stmt[priv_types] ON ";
		if(isset($stmt['object_type'])) $sql .= $stmt['object_type'].' ';
		if(isset($stmt['database'])) $sql .= $stmt['database'].'.';
		if($action == 'GRANT'){
			$sql .= "$stmt[table] TO $stmt[user];";
		} elseif($action == 'REVOKE') {
			$sql .= "$stmt[table] FROM $stmt[user];";
		} else {
			$sql = false;
		}
		return $sql;
	}
}
