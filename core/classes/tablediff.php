<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/
class Tablediff {
	const CREATE = 0b1;
	const ALTER = 0b10;
	const DROP = 0b100;
	const GRANT = 0b1000;
	const REVOKE = 0b10000;

	static private $tables = [];

	private $name, $permissions = [],
		$statementdiffs = [],
		$sourcefiles = [],
		$create = [],
		$alter = [],
		$drop = [],
		$grant = [], $grant_sql = [],
		$revoke = [], $revoke_sql = [],
		$errors = [];

	static public function reset(){
		self::$tables = [];
	}

	static public function get($name){
		if(!isset(self::$tables[$name])){
			self::$tables[$name] = new Self($name);
		}
		return self::$tables[$name];
	}

	static public function diff_all(){
		foreach(self::$tables as $table){
			$table->diff();
		}
	}

	static public function write_result(&$partial_result, $include){
		$grant = (bool) ($include & self::GRANT);
		$revoke = (bool) ($include & self::REVOKE);
		$init = function($table, $sourcefiles) use (&$partial_result){
			if(!isset($partial_result['tables'][$table])
				|| isset($partial_result['tables'][$table]['is_empty']) && $partial_result['tables'][$table]['is_empty']){
					$type = empty($sourcefiles) ? 'database_only' : 'intersection';
					$partial_result['tables'][$table] = ['name'=>$table,'sourcefiles'=>array_unique($sourcefiles),'type'=>$type];
			}
		};
		foreach(self::$tables as $name => $table){
			$table->diff();
			if(!empty($table->errors)){
				foreach($table->errors as $error){
					$partial_result['errors'][] = $error;
				}
			}
			//if($name == 'test.workorder_owner') debug($name,$table->grant,$table->revoke,$table->revoke_sql);
			if($revoke && !empty($table->revoke)){
				//debug($name, $table->revoke, $revoke);
				$init($name, $table->sourcefiles);
				$result_table = &$partial_result['tables'][$name];
				foreach($table->revoke as $key => $row){
					$result_table['permissions'][$key.'-revoke'] = $row;
					if(isset($table->revoke_sql[$key])) $result_table['sql'][$key.'-revoke'] = $table->revoke_sql[$key];
				}
			}
			if($grant && !empty($table->grant)){
				//debug($name, $table->grant, $grant);
				$init($name, $table->sourcefiles);
				$result_table = &$partial_result['tables'][$name];
				foreach($table->grant as $key => $row){
					$result_table['permissions'][$key.'-grant'] = $row;
					if(isset($table->grant_sql[$key])) $result_table['sql'][$key.'-grant'] = $table->grant_sql[$key];
				}
			}
		}
	}

	private function __construct($name){
		$this->name = $name;
	}

	static public function file_statement($stmt, $filename){
		if(!isset($stmt['key']) || !isset($stmt['database']) || !isset($stmt['table'])) return;
		$name = self::get_table_name($stmt);
		$key = $stmt['key'];
		$diff = self::get($name);
		if(!isset($diff->permissions[$key])){
			$diff->permissions[$key] = new Statementdiff($key);
		}
		$diff->permissions[$key]->from_file($stmt, $filename);
		$diff->sourcefiles[] = $filename;
	}

	static public function database_statement($stmt){
		if(!isset($stmt['key']) || !isset($stmt['database']) || !isset($stmt['table'])) return;
		$name = self::get_table_name($stmt);
		$key = $stmt['key'];
		$diff = self::get($name);
		if(!isset($diff->permissions[$key])){
			$diff->permissions[$key] = new Statementdiff($key);
		}
		$diff->permissions[$key]->from_database($stmt);
	}

	static private function get_table_name($stmt){
		$database = preg_replace('/^`([^`]*)`$/','$1',$stmt['database']);
		$table = preg_replace('/^`([^`]*)`$/','$1',$stmt['table']);
		return $database.'.'.$table;
	}

	private function diff(){
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

class Statementdiff {
	private $key, $db_stmt, $file_stmts = [], $filenames = [], $diff_calculated = true, $grant, $revoke;
	public $is_merged = false, $merge_error = null;

	public function __construct($key){
		$this->key = $key;
	}

	public function debug_output(){
		return ['key'=>$this->key,'grant'=>$this->grant, 'revoke'=>$this->revoke];
	}

	public function from_database($stmt){
		$this->db_stmt = $stmt;
		$this->diff_calculated = false;
	}

	public function from_file($stmt, $filename){
		$this->file_stmts[] = $stmt;
		$this->diff_calculated = false;
		$this->filenames[] = $filename;
	}

	public function get_grant(){
		$this->diff();
		return $this->grant;
	}

	public function get_revoke(){
		$this->diff();
		return $this->revoke;
	}

	public function get_unmerged(){
		return $this->file_stmts;
	}

	private function diff(){
		if($this->diff_calculated) return;
		$db = $this->db_stmt;
		$file = $this->get_file_stmt();
		if(!isset($file)){
			$this->grant = null;
			$this->revoke = $db;
			$this->diff_calculated = true;
			return;
		} elseif(!isset($db)){
			$this->grant = $file;
			$this->revoke = null;
			$this->diff_calculated = true;
			return;
		}

		unset($file['files']);
		$dbdiff = array_udiff_assoc($db, $file, [$this,'compare']);
		$filediff = array_udiff_assoc($file, $db, [$this,'compare']);

		if(empty($dbdiff) && empty($filediff)){
			$this->grant = null;
			$this->revoke = null;
			return;
		}

		list($remove, $add) = $this->file_is_subset($filediff, $dbdiff);
		if(!empty($add)){
			$this->grant = ['priv_types'=>$add] + $file;
		}
		if(!empty($remove)){
			$this->revoke = ['priv_types'=>$remove] + $db;
		}
	}

	private function get_file_stmt(){
		$len = count($this->file_stmts);
		switch($len){
			case 0: return null;
			case 1: return $this->file_stmts[0];
			default: 
				list($merged,$merge_error) = $this->merge_file_stmts($this->key, ...$this->file_stmts);
				$this->is_merged = true;
				$this->merge_error = $merge_error;
				return $merged;
		}
	}

	private function compare($a, $b){
		if(is_array($a)){
			if(is_array($b)){
				$adiff = array_udiff($a, $b, [$this,'compare']);
				$bdiff = array_udiff($b, $a, [$this,'compare']);
				if(empty($adiff)){
					if(empty($bdiff)){
						return 0;
					} else {
						return -1;
					}
				} else {
					return 1;
				}
			} else {
				return -1;
			}
		} elseif(is_array($b)){
			return 1;
		} else {
			if($a < $b) return -1;
			if($a == $b) return 0;
			if($a > $b) return 1;
		}
	}

	private function file_is_subset($filediff, $dbdiff){
		$remove = [];
		$add = [];
		if(!isset($filediff['priv_types'])){
			debug($filediff, $dbdiff, $this->debug_output());
		}
		$types = array_unique(array_merge(array_keys($filediff['priv_types']), array_keys($dbdiff['priv_types'])));
		foreach($types as $type){
			$filepriv = $filediff['priv_types'][$type] ?? null;
			$dbpriv = $dbdiff['priv_types'][$type] ?? null;
			if(isset($filepriv['column_list']) && isset($dbpriv['column_list'])){
				// file columns, db columns
				$file_columns = array_combine($filepriv['column_list'],$filepriv['column_list']);
				$db_columns = array_combine($dbpriv['column_list'],$dbpriv['column_list']);

				$remove_columns = array_udiff_assoc($db_columns,$file_columns,[$this,'compare']);
				$add_columns = array_udiff_assoc($file_columns,$db_columns,[$this,'compare']);

				if(!empty($remove_columns)){
					$remove[$type] = [
						'priv_type'=>$type,
						'column_list'=>$remove_columns
					];
				}
				if(!empty($add_columns)){
					$add[$type] = [
						'priv_type'=>$type,
						'column_list'=>$add_columns
					];
				}
			} elseif($filepriv != $dbpriv) {
				if(isset($dbpriv['column_list'])){
					// db columns
					$remove[$type] = [
						'priv_type'=>$type,
						'column_list'=>$dbpriv['column_list']
					];
				} elseif(isset($dbpriv)) {
					// db whole table
					$remove[$type] = $type;
				}
				// else: db blank
				if(isset($filepriv['column_list'])){
					// file columns
					$add[$type] = [
						'priv_type'=>$type,
						'column_list'=>$filepriv['column_list']
					];
				} elseif(isset($filepriv)) {
					// file whole table
					$add[$type] = $type;
				}
				// else: file blank
			}
			// else: file whole table, db whole table => match
		}
		return [$remove, $add];
	}

	private function merge_file_stmts($stmtkey,...$file_stmts){
		$merged = [];
		foreach($file_stmts as $stmt){
			if(!isset($merged)) continue;
			foreach($stmt as $key => $value){
				if(!isset($merged[$key])){
					$merged[$key] = $value;
				} elseif($merged[$key]!=$value){
					if($key == 'priv_types'){
						$keys = [];
						$values = [];
						$priv_keys = array_unique(array_merge(array_keys($merged[$key]),array_keys($value)));
						foreach($priv_keys as $priv_key){
							$a = isset($merged[$key][$priv_key]) ? $merged[$key][$priv_key] : null;
							$b = isset($value[$priv_key]) ? $value[$priv_key] : null;
							if(is_string($a)){
								$values[$a] = $a;
								continue;
							}
							if(is_string($b)){
								$values[$b] = $b;
								continue;
							}
							if(!isset($a)){
								$values[$b['priv_type']] = $b;
								continue;
							}
							if(!isset($b)){
								$values[$a['priv_type']] = $a;
								continue;
							}
							$values[$a['priv_type']] = ['priv_type'=>$a['priv_type'],'column_list'=>array_merge($a['column_list'],$b['column_list'])];
						}
						$merged[$key] = $values;
					} else {
						$merged = null;
						break;
					}
				}
			}
		}
		$files = implode(",\n",array_unique($this->filenames));
		if(isset($merged) && $merged['type']=='grant'){
			return [$merged,null];
		}
		return [null,"Failed merging [$stmtkey] in files:\n$files"];
	}
}
