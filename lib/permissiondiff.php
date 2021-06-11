<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/

require_once __DIR__.'/description.php';
class Permissiondiff {
	static private $file_schema_permissions = [], $db_schema_permissions = [], $schema_changed = [];
	private $key, $db_stmt, $file_stmt, $filenames = [], $diff_calculated = true, $grant, $revoke, $schema_change_version = 0, $schema_key;
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

		$schema_key = $this->schema_key();
		if(!isset(Self::$schema_changed[$schema_key])) Self::$schema_changed[$schema_key] = 0;

		if($this->db_stmt['table'] == '*'){
			Self::$db_schema_permissions[$schema_key] = $this->db_stmt;
			Self::$schema_changed[$schema_key] += 1;
		}
		$this->schema_change_version = Self::$schema_changed[$schema_key];
	}

	public function from_file($stmt, $filename){
		if(isset($this->file_stmt)){
			$this->file_stmt = Description::merge($this->file_stmt, $stmt);
			$this->is_merged = true;
		} else {
			$this->file_stmt = $stmt;
		}

		$schema_key = $this->schema_key();
		if(!isset(Self::$schema_changed[$schema_key])) Self::$schema_changed[$schema_key] = 0;

		if($this->file_stmt['table'] == '*'){
			Self::$file_schema_permissions[$schema_key] = $this->file_stmt;
			Self::$schema_changed[$schema_key] += 1;
		}
		$this->schema_change_version = Self::$schema_changed[$schema_key];
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

	private function schema_key(){
		if(!isset($this->schema_key)){
			if(isset($this->file_stmt)){
				$stmt = $this->file_stmt;
			} elseif(isset($this->db_stmt)){
				$stmt = $this->db_stmt;
			} else {
				return;
			}
			$this->schema_key = "schema:$stmt[user]:$stmt[database]";
		}
		return $this->schema_key;
	}

	private function has_schema_changed(){
		$schema_key = $this->schema_key();
		if(!isset($schema_key)){
			return false;
		}
		return Self::$schema_changed[$schema_key] > $this->schema_change_version;
	}

	private function update_schema_changes(){
		if($this->has_schema_changed()){
			if(isset($this->db_stmt)){
				$changed = $this->db_stmt->schema_statement(Self::$db_schema_permissions[$this->schema_key()]);
				if($changed){
					$this->diff_calculated = false;
				}
			}
			if(isset($this->file_stmt)){
				$changed = $this->file_stmt->schema_statement(Self::$file_schema_permissions[$this->schema_key()]);
				if($changed){
					$this->diff_calculated = false;
				}
			}
		}
	}

	private function diff(){
		$this->update_schema_changes();
		if($this->diff_calculated) return;
		$db = $this->db_stmt;
		$file = $this->file_stmt;
		if(!isset($file)){
			$this->grant = null;
			$this->revoke = empty($db['priv_types']) ? null : $db;
			$this->diff_calculated = true;
			return;
		} elseif(!isset($db)){
			$this->grant = empty($file['priv_types']) ? null : $file;
			$this->revoke = null;
			$this->diff_calculated = true;
			return;
		}

		list($remove, $add) = $db->diff($file);

		if(!empty($add)){
			$this->grant = ['priv_types'=>$add] + $file->to_array();
		}
		if(!empty($remove)){
			$this->revoke = ['priv_types'=>$remove] + $db->to_array();
		}
		$this->diff_calculated = true;
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
						'priv_type'=>$dbpriv['priv_type'],
						'column_list'=>$remove_columns
					];
				}
				if(!empty($add_columns)){
					$add[$type] = [
						'priv_type'=>$dbpriv['priv_type'],
						'column_list'=>$add_columns
					];
				}
			} elseif($filepriv != $dbpriv) {
				if(isset($dbpriv)){
					// db columns or whole table
					$remove[$type] = $dbpriv;
				}
				// else: db blank
				if(isset($filepriv)){
					// file columns or while table
					$add[$type] = $filepriv;
				}
				// else: file blank
			}
			// else: file whole table, db whole table => match
		}
		return [$remove, $add];
	}
}
