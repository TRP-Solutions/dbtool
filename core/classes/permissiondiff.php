<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/
class Permissiondiff {
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
