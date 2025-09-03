<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/

declare(strict_types=1);
require_once __DIR__."/permissiondiff.php";
require_once __DIR__."/definitiondiff.php";
require_once __DIR__."/userdiff.php";
require_once __DIR__."/format.php";
require_once __DIR__."/parser.php";
require_once __DIR__.'/description.php';
class Tablediff {
	const CREATE = 0b1;
	const ALTER = 0b10;
	const DROP = 0b100;
	const GRANT = 0b1000;
	const REVOKE = 0b10000;
	const CREATE_USER = 0b100000;
	const ALTER_USER = 0b1000000;
	const DROP_USER = 0b10000000;

	static private $tables = [], $users = [], $missing_users = [], $database_error, $skipped_statements = 0;

	private $name, $permissions = [], $definition = null,
		$sources = [],
		$create = [], $create_sql = [],
		$alter  = [], $alter_sql  = [],
		$drop   = [], $drop_sql   = [],
		$grant  = [], $grant_sql  = [],
		$revoke = [], $revoke_sql = [],
		$errors = [];

	static public function run($sources){
		self::reset();

		$users = [];
		$user_filter = [];
		$ignore_host = self::ignore_host();
		$database_missing = Config::get('database') === null;
		
		foreach($sources as $source){
			$sourcename = $source->get_name();
			$stmts = $source->get_stmts();
			if(is_array($stmts)) foreach($stmts as $stmt){
				$obj = \Parser\statement($stmt, ['ignore_host'=>$ignore_host]);
				$is_grant = $obj['type'] == 'grant' || $obj['type'] == 'revoke';
				if($is_grant){
					self::file_statement($obj, $sourcename);
					if(!in_array($obj['user'], $users)) $users[] = $obj['user'];
					$pairkey = $obj['database'].':'.$obj['user'];
					$user_filter[$pairkey] = true;
				} elseif($obj['type'] == 'table'){
					if($database_missing){
						if(!isset(self::$database_error)){
							self::$database_error = true;
						}
						self::$skipped_statements += 1;
					} else {
						self::file_statement($obj, $sourcename);
					}
				} elseif($obj['type'] == 'user') {
					self::file_statement($obj, $sourcename);
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
		self::$users = [];
		self::$missing_users = [];
		self::$database_error = null;
		self::$skipped_statements = 0;
		Definitiondiff::reset();
	}

	static public function get($name){
		if(!isset(self::$tables[$name])){
			self::$tables[$name] = new Self($name);
		}
		return self::$tables[$name];
	}

	static public function get_user($name){
		if(!isset(self::$users[$name])){
			self::$users[$name] = new Userdiff($name);
		}
		return self::$users[$name];
	}

	static public function write_result(){
		$result = [];

		if(self::$database_error){
			$result[] = ['type'=>'error','error'=>['errno'=>4,'error'=>"Database missing. ".self::$skipped_statements." statement(s) skipped."]];
		}
		if(!empty(self::$missing_users)){
			foreach(self::$missing_users as $user){
				$result[] = ['type'=>'error','error'=>['errno'=>6,'error'=>"User not found ($user)."]];
			}
		}

		$mode = self::read_mode();
		$create = (bool) ($mode & self::CREATE);
		$alter = (bool) ($mode & self::ALTER);
		$drop = (bool) ($mode & self::DROP);
		$grant = (bool) ($mode & self::GRANT);
		$revoke = (bool) ($mode & self::REVOKE);
		$create_user = (bool) ($mode & self::CREATE_USER);
		$alter_user = (bool) ($mode & self::ALTER_USER);
		$drop_user = (bool) ($mode & self::DROP_USER);

		if($create_user || $alter_user || $drop_user){
			foreach(self::$users as $name => $user){
				if($alter_user){
					$ua_result = $user->get_alter();
					if(isset($ua_result)){
						$result[] = $ua_result;
						continue;
					}
				}
				if($create_user){
					$uc_result = $user->get_create();
					if(isset($uc_result)){
						$result[] = $uc_result;
						continue;
					}
				}
				if($drop_user){
					$ud_result = $user->get_drop();
					if(isset($ud_result)){
						$result[] = $ud_result;
						continue;
					}
				}
			}
		}
		$create_database_sql = self::load_db_tables();
		if(isset($create_database_sql)){
			$result[] = ['type'=>'create_database','sql'=>[$create_database_sql]];
		}
		foreach(self::$tables as $name => $table){
			$table->diff();
			if(!empty($table->errors)){
				foreach($table->errors as $error){
					$result[] = ['type'=>'error','error'=>$error];
				}
			}
			$table_obj = null;
			if($alter && !empty($table->alter)){
				$table_obj = self::init_table($name, $table->sources) + $table->alter;
			} elseif($create && !empty($table->create)){
				$table_obj = self::init_table($name, $table->sources,'file_only') + [
					'filetable'=>$table->create,
					'sql'=>[$table->create_sql]
				];
			} elseif($drop && !empty($table->drop)){
				$result[] = ['type'=>'drop','name'=>$table->drop,'sql'=>[$table->drop_sql]];
			}
			if($revoke && !empty($table->revoke)){
				if(!isset($table_obj)){
					$table_obj = self::init_table($name, $table->sources);
				}
				foreach($table->revoke as $key => $row){
					$table_obj['permissions'][$key.'-revoke'] = $row;
					if(isset($table->revoke_sql[$key])) $table_obj['sql'][$key.'-revoke'] = $table->revoke_sql[$key];
				}
			}
			if($grant && !empty($table->grant)){
				if(!isset($table_obj)){
					$table_obj = self::init_table($name, $table->sources);
				}
				foreach($table->grant as $key => $row){
					$table_obj['permissions'][$key.'-grant'] = $row;
					if(isset($table->grant_sql[$key])) $table_obj['sql'][$key.'-grant'] = $table->grant_sql[$key];
				}
			}
			if(isset($table_obj)){
				$result[] = $table_obj;
			}
		}
		return $result;
	}

	static private function init_table($name, $sourcenames, $type = null){
		if(!isset($type)) $type = empty($sourcenames) ? 'database_only' : 'intersection';
		return ['name'=>$name,'type'=>$type,'sources'=>array_unique($sourcenames)];
	}

	static private function read_mode(){
		$mode = 0;
		$config_modes = Config::get('statement');
		if(empty($config_modes)){
			$mode = self::CREATE | self::ALTER | self::DROP | self::GRANT | self::REVOKE
				| self::CREATE_USER | self::ALTER_USER | self::DROP_USER;
		} else {
			foreach($config_modes as $config_mode){
				$config_mode = strtolower($config_mode);
				switch($config_mode){
					case 'create': $mode |= self::CREATE; break;
					case 'alter': $mode |= self::ALTER; break;
					case 'drop': $mode |= self::DROP; break;
					case 'grant': $mode |= self::GRANT; break;
					case 'revoke': $mode |= self::REVOKE; break;
					case 'create_user': $mode |= self::CREATE_USER; break;
					case 'alter_user': $mode |= self::ALTER_USER; break;
					case 'drop_user': $mode |= self::DROP_USER; break;
				}
			}
		}
		return $mode;
	}

	static private function load_db_tables(){
		$dbname = Config::get('database');
		if(empty($dbname)) return;
		$dbname = DB::escape($dbname);

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
			
			$result = DB::sql("SELECT * FROM `information_schema`.`schema_privileges` WHERE `table_schema` = '$db'");
			foreach($result as $row){
				$desc = Description::from_grant_row($row);;
				self::merge_into_grants($grants, $desc);
			}
			$result = DB::sql("SELECT * FROM `information_schema`.`table_privileges` WHERE `table_schema` = '$db'");
			foreach($result as $row){
				$desc = Description::from_grant_row($row);;
				self::merge_into_grants($grants, $desc);
			}
			$result = DB::sql("SELECT * FROM `information_schema`.`column_privileges` WHERE `table_schema` = '$db' ORDER BY grantee");
			$rows = [];
			foreach($result as $row){
				$rows[] = json_encode($row);
				$desc = Description::from_grant_row($row);;
				self::merge_into_grants($grants, $desc);
			}
			foreach($users as $user){
				$re1 = "^[']([^']*)['](?:@[']([^'])+['])?$";
				$re2 = "^[`]([^`]*)[`](?:@[`]([^`]+)[`])?$";
				$re = "/(?:$re1)|(?:$re2)/";
				if(preg_match($re, $user, $matches)){
					$username = empty($matches[1]) ? $matches[3] : $matches[1];
					$host = empty($matches[2]) ? $matches[4] : $matches[2];
					$sql = "SELECT 1 FROM mysql.user WHERE user='$username'";
					if(!empty($host)){
						$sql .= " AND host='$host'";
					}
					$result = DB::sql($sql);
					if(!$result || !$result->num_rows){
						self::$missing_users[] = $user;
						continue;
					}
					$result = DB::sql("SHOW GRANTS FOR $user");
					if($result){
						while($row = $result->fetch_row()){
							$obj = \Parser\statement($row[0], ['ignore_host'=>self::ignore_host()]);
							if(self::desc_is_allowed($obj,$filter)){
								self::merge_into_grants($grants, $obj);
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
			$grants[$desc['key']] = Description::merge($grants[$desc['key']], $desc);
		}
	}

	private function __construct($name){
		$this->name = $name;
	}

	static public function file_statement($stmt, $sourcename){
		if($stmt['type'] == 'user'){
			self::get_user($stmt['user'])->from_file($stmt, $sourcename);
			return;
		}
		$name = self::get_table_name($stmt);
		$diff = self::get($name);
		if($stmt['type'] == 'table'){
			if(!isset($diff->definition)) {
				$diff->definition = new Definitiondiff($name);
			}
			$diff->definition->from_file($stmt, $sourcename);
		} elseif($stmt['type'] == 'grant'){
			$key = $stmt['key'];
			if(!isset($diff->permissions[$key])){
				$diff->permissions[$key] = new Permissiondiff($key);
			}
			$diff->permissions[$key]->from_file($stmt, $sourcename);
		}
		$diff->sources[] = $sourcename;
	}

	static public function database_statement($stmt){
		if($stmt['type'] == 'user'){
			self::get_user($stmt['user'])->from_database($stmt, $sourcename);
			return;
		}
		$name = self::get_table_name($stmt);
		$diff = self::get($name);
		if($stmt['type'] == 'table'){
			if(!isset($diff->definition)) {
				$diff->definition = new Definitiondiff($name);
			}
			$diff->definition->from_database($stmt);
		} elseif($stmt['type'] == 'grant'){
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
		if(is_array($stmt)){
			$stmt = Description::from_array($stmt);
		}
		return $stmt->to_flat_array();
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
