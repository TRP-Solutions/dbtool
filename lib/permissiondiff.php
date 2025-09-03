<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/


declare(strict_types=1);
require_once __DIR__.'/description.php';
class Permissiondiff {
	static private $file_schema_permissions = [], $db_schema_permissions = [], $schema_changed = [];
	private $key, $db_stmt, $file_stmt, $filenames = [], $diff_calculated = true, $grant, $revoke, $schema_change_version = 0, $schema_key;
	private $errors = [];
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
		if(!isset(self::$schema_changed[$schema_key])) self::$schema_changed[$schema_key] = 0;

		if($this->db_stmt['table'] == '*'){
			self::$db_schema_permissions[$schema_key] = $this->db_stmt;
			self::$schema_changed[$schema_key] += 1;
		}
		$this->schema_change_version = self::$schema_changed[$schema_key];
	}

	public function from_file($stmt, $filename){
		if(isset($stmt['error'])){
			$database = $stmt['database'] ?? 'NULL';
			$table = $stmt['table'] ?? 'NULL';
			$this->errors[] = ['errno'=>1,'error'=>"Parse Error in file \"$filename\" permission in database $database on table $table."];
			return;
		}
		if($stmt['type'] == 'revoke'){
			$database = $stmt['database'] ?? 'NULL';
			$table = $stmt['table'] ?? 'NULL';
			$this->errors[] = ['errno'=>7,'error'=>"Ignored REVOKE PERMISSION in file \"$filename\" in database $database on table $table."];
			return;
		}
		if(isset($this->file_stmt)){
			$this->file_stmt = Description::merge($this->file_stmt, $stmt);
			$this->is_merged = true;
		} else {
			$this->file_stmt = $stmt;
		}

		$schema_key = $this->schema_key();
		if(!isset(self::$schema_changed[$schema_key])) self::$schema_changed[$schema_key] = 0;

		if($this->file_stmt['table'] == '*'){
			self::$file_schema_permissions[$schema_key] = $this->file_stmt;
			self::$schema_changed[$schema_key] += 1;
		}
		$this->schema_change_version = self::$schema_changed[$schema_key];
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

	public function get_errors(){
		return $this->errors;
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
			$user = $stmt['user'] ?? 'NULL';
			$this->schema_key = "schema:$user:$stmt[database]";
		}
		return $this->schema_key;
	}

	private function has_schema_changed(){
		$schema_key = $this->schema_key();
		if(!isset($schema_key)){
			return false;
		}
		return self::$schema_changed[$schema_key] > $this->schema_change_version;
	}

	private function update_schema_changes(){
		if($this->has_schema_changed()){
			$schema_key = $this->schema_key();
			if(isset($this->db_stmt) && isset(self::$db_schema_permissions[$schema_key])){
				$changed = $this->db_stmt->schema_statement(self::$db_schema_permissions[$this->schema_key()]);
				if($changed){
					$this->diff_calculated = false;
				}
			}
			if(isset($this->file_stmt) && isset(self::$file_schema_permissions[$schema_key])){
				$changed = $this->file_stmt->schema_statement(self::$file_schema_permissions[$this->schema_key()]);
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
		} elseif(!isset($db) && $file['type']=='grant'){
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
}
