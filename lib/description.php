<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/
class Description implements ArrayAccess, JsonSerializable {
	public static function from_grant_row($row){
		$ignore_host = defined('PERMISSION_IGNORE_HOST') && PERMISSION_IGNORE_HOST;
		if($ignore_host){
			$grantee = explode('@',$row['GRANTEE'])[0];
		} else {
			$grantee = $row['GRANTEE'];
		}
		if(isset($row['COLUMN_NAME'])){
			$priv_types = [$row['PRIVILEGE_TYPE'].'*'=>['priv_type'=>$row['PRIVILEGE_TYPE'],'column_list'=>[$row['COLUMN_NAME']]]];
		} else {
			$priv_types = [$row['PRIVILEGE_TYPE']=>$row['PRIVILEGE_TYPE']];
		}
		$table = isset($row['TABLE_NAME']) ? $row['TABLE_NAME'] : '*';
		return new Self('grant',$grantee,$table,$row['TABLE_SCHEMA'],$priv_types);
	}

	public static function from_array($arr){
		return new Self('grant',$arr['user'],$arr['table'],$arr['database'],$arr['priv_types']);
	}

	public static function merge($d1, $d2){
		foreach($d2['priv_types'] as $key=>$value){
			$merged = false;
			if(is_array($value)){
				foreach($d1['priv_types'] as &$d1_priv_type){
					if(is_array($d1_priv_type) && $d1_priv_type['priv_type']==$value['priv_type']){
						$d1_priv_type['column_list'] = array_unique(array_merge($d1_priv_type['column_list'],$value['column_list']));
						$merged = true;
					}
				}
			}
			if(!$merged){
				$d1['priv_types'][$key] = $value;
			}
		}

		ksort($d1['priv_types']);
		return $d1;
	}

	private static function build_grant_key($user,$database,$table){
		return "grant:$user:$database".(isset($table) ? ".$table":'');
	}

	private static function quote_id($id){
		if(isset($id)){
			return $id[0]!='`' && $id!='*'? "`$id`" : $id;
		} else {
			return null;
		}
	}

	private $key, $type, $user, $table, $database, $priv_types, $original_priv_types;

	private function __construct($type, $user, $table, $database, $priv_types){
		if($user[0]=="'") $user = str_replace("'", "`", $user);
		$database = self::quote_id($database);
		$table = self::quote_id($table);
		$this->key = Self::build_grant_key($user,$database,$table);
		$this->type = $type;
		$this->user = $user;
		$this->table = $table;
		$this->database = $database;
		$this->priv_types = $priv_types;
	}

	public function to_array(){
		return [
			'key'=>$this->key,
			'type'=>$this->type,
			'user'=>$this->user,
			'table'=>$this->table,
			'database'=>$this->database,
			'priv_types'=>$this->priv_types
		];
	}

	public function to_flat_array(){
		$stmt = $this->to_array();
		if(isset($stmt['priv_types']) && is_array($stmt['priv_types'])){
			foreach($stmt['priv_types'] as &$type){
				if(is_array($type)){
					$type = $type['priv_type'] .' (`'.implode('`, `',$type['column_list']).'`)';
				}
			}
			$stmt['priv_types'] = implode(', ', $stmt['priv_types']);
		}

		return $stmt;
	}

	public function diff($ideal){
		unset($ideal['files']);
		$db = $this->to_array();
		$ideal = $ideal->to_array();
		$dbdiff = array_udiff_assoc($db, $ideal, [$this,'compare']);
		$idealdiff = array_udiff_assoc($ideal, $db, [$this,'compare']);

		if(empty($dbdiff) && empty($idealdiff)){
			return [null,null];
		}

		return $this->file_is_subset($idealdiff, $dbdiff);
	}

	public function schema_statement($schema_stmt){
		$table = ['priv_types'=>$this->priv_types];
		$schema = ['priv_types'=>$schema_stmt->priv_types];
		$tablediff = array_udiff_assoc($table, $schema, [$this,'compare']);

		if(empty($tablediff)){
			$this->set_shadowing_priv_types([]);
		} else {
			$schemadiff = array_udiff_assoc($schema, $table, [$this,'compare']);
			$subset = $this->file_is_subset($tablediff, $schemadiff);
			foreach($subset[0] as $key => $priv){
				if(isset($subset[1][$key.'*'])){
					// If there's a table-level permission from the schema,
					// don't set the same column-level permission in the shadowing permission set
					unset($subset[1][$key.'*']);
				}
			}
			$this->set_shadowing_priv_types($subset[1]);
		}
	}

	private function set_shadowing_priv_types($priv_types){
		if(!isset($this->original_priv_types)){
			$this->original_priv_types = $this->priv_types;
		}
		$this->priv_types = $priv_types;
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
				$typename = $dbpriv['priv_type'];

				$file_columns = array_combine($filepriv['column_list'],$filepriv['column_list']);
				$db_columns = array_combine($dbpriv['column_list'],$dbpriv['column_list']);

				$remove_columns = array_udiff_assoc($db_columns,$file_columns,[$this,'compare']);
				$add_columns = array_udiff_assoc($file_columns,$db_columns,[$this,'compare']);

				if(!empty($remove_columns)){
					$remove[$type] = [
						'priv_type'=>$typename,
						'column_list'=>$remove_columns
					];
				}
				if(!empty($add_columns)){
					$add[$type] = [
						'priv_type'=>$typename,
						'column_list'=>$add_columns
					];
				}
			} elseif($filepriv != $dbpriv) {
				if(isset($dbpriv['column_list'])){
					// db columns
					$typename = $dbpriv['priv_type'];
					$remove[$type] = [
						'priv_type'=>$typename,
						'column_list'=>$dbpriv['column_list']
					];
				} elseif(isset($dbpriv)) {
					// db whole table
					$remove[$type] = $type;
				}
				// else: db blank
				if(isset($filepriv['column_list'])){
					// file columns
					$typename = $filepriv['priv_type'];
					$add[$type] = [
						'priv_type'=>$typename,
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

	// INTERFACE IMPLEMENTATIONS
	// ArrayAccess:

	public function &offsetGet($key){
		switch($key){
			case 'key': return $this->key;
			case 'type': return $this->type;
			case 'user': return $this->user;
			case 'table': return $this->table;
			case 'database': return $this->database;
			case 'priv_types': return $this->priv_types;
			default: return null;
		}
	}

	public function offsetSet($key, $value){
		switch($key){
			case 'key': $this->key = $value; break;
			case 'type': $this->type = $value; break;
			case 'user': $this->user = $value; break;
			case 'table': $this->table = $value; break;
			case 'database': $this->database = $value; break;
			case 'priv_types': $this->priv_types = $value; break;
		}
	}

	public function offsetUnset($key){

	}

	public function offsetExists($key){
		switch($key){
			case 'key': return isset($this->key);
			case 'type': return isset($this->type);
			case 'user': return isset($this->user);
			case 'table': return isset($this->table);
			case 'database': return isset($this->database);
			case 'priv_types': return isset($this->priv_types);
			default: return false;
		}
	}

	// JsonSerializable:
	public function jsonSerialize(){
		return $this->to_array()+['__CLASS__'=>'Description'];
	}
}