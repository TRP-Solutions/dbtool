<?php

class PermissionDiff {
	private $prim, $ignore_host, $sql_vars, $files, $file_stmts = [];

	public function __construct($files){
		$vars = Config::get('variables');
		$this->prim = isset($vars['PRIM']) ? $vars['PRIM'] : Config::get('database');
		$this->ignore_host = defined('PERMISSION_IGNORE_HOST') && PERMISSION_IGNORE_HOST;
		$this->sql_vars = $vars;
		$this->files = $files;
	}

	public function run(&$partial_result = []){
		$db = $this->get_dbdata();
		if(empty($db['table'])){
			$file_stmts = [];
			$user_counts = [];
			foreach($this->files as $file){
				$stmts = $this->get_stmts($file);
				$file_stmts[$file->get_filename()] = $stmts;
				foreach($stmts['table'] as $table){
					if(!isset($user_counts[$table['user']])) $user_counts[$table['user']] = 1;
					else $user_counts[$table['user']] += 1;
				}
			}
			arsort($user_counts);
			foreach($user_counts as $user => $count){
				if(preg_match("/^'([^']*)'(@'[^']+')?$/", $user, $matches)){
					$db = $this->get_dbdata($user, $matches[1]);
					break;
				}
			}
			foreach($file_stmts as $filename => $stmts){
				$this->diff($partial_result, $filename, $stmts, $db);
			}
		} else {
			foreach($this->files as $file){
				$this->diff($partial_result, $file->get_filename(), $this->get_stmts($file), $db);
			}
		}
		return $partial_result;
	}

	private function diff(&$partial_result, $filename, $stmts, $db){
		$keys = array_unique(array_merge(array_keys($db['table']), array_keys($stmts['table'])));
		foreach($keys as $key){
			if(isset($stmts['table'][$key])){
				ksort($stmts['table'][$key]['priv_types']);
				$table = $this->get_table($stmts['table'][$key]);
				$conflict = !empty($this->file_stmts[$table][$key]);
				$this->file_stmts[$table][$key][] = $stmt = ['table'=>$stmts['table'][$key],'raw'=>$stmts['raw'][$key],'files'=>[$filename]];
				if($conflict){
					list($merged_stmt,$merge_error) = $this->merge_file_stmts($key,...$this->file_stmts[$table][$key]);
					if(isset($merged_stmt)){
						$stmt = $merged_stmt;
						$this->file_stmts[$table][$key] = [$merged_stmt];
					}
					if(isset($merge_error)){
						$partial_result['errors'][] = ['errno'=>4,'error'=>'Permission conflict: '.$merge_error];
					}
				}
				if(isset($db['table'][$key])){
					ksort($db['table'][$key]['priv_types']);
					$dbdiff = array_udiff_assoc($db['table'][$key], $stmt['table'], [$this,'compare']);
					$filediff = array_udiff_assoc($stmt['table'], $db['table'][$key], [$this,'compare']);
					if(!empty($dbdiff) || !empty($filediff)){
						if(!isset($partial_result['tables'][$table])){
							$partial_result['tables'][$table] = ['name'=>$table,'sourcefiles'=>$stmt['files'],'type'=>'intersection'];
						}
						$result = &$partial_result['tables'][$table];
						$result['permissions'][$key.'-db'] = $this->create_data_row('Database', $db['table'][$key], 'bg-info');
						$result['sql'][$key.'-db'] = $this->convert_grant_to_sql($db['table'][$key],'REVOKE');
						if($conflict && $merge_error){
							unset($result['permissions'][$key.'-file']);
							unset($result['sql'][$key.'-file']);
							$i = 0;
							foreach($this->file_stmts[$table][$key] as $conflict_stmt){
								$result['permissions'][$key.'-file-'.$i] = $this->create_data_row('Schemafile', $conflict_stmt['table'], 'bg-warning');
								$result['sql'][$key.'-file-'.$i] = $conflict_stmt['raw'].';';
								$i++;
							}
						} else {
							$result['permissions'][$key.'-file'] = $this->create_data_row('Schemafile'.($conflict ? 's (merged)':''), $stmt['table'], $conflict ? 'bg-warning' : 'bg-info');
							$result['sql'][$key.'-file'] = $stmt['raw'].';';
						}
					} elseif($conflict){
						$result = &$partial_result['tables'][$table];
						unset($result['permissions'][$key.'-db']);
						unset($result['permissions'][$key.'-file']);
						unset($result['sql'][$key.'-db']);
						unset($result['sql'][$key.'-file']);
					}
				} else {
					if(!isset($partial_result['tables'][$table])){
						$partial_result['tables'][$table] = ['name'=>$table,'sourcefiles'=>$stmt['files'],'type'=>'file_only'];
					}
					$result = &$partial_result['tables'][$table];
					$result['permissions'][$key.'-file'] = $this->create_data_row('Schemafile'.($conflict ? ' (merged)':''), $stmt['table'], $conflict ? 'bg-warning' : 'bg-success');
					$result['sql'][$key.'-file'] = $stmt['raw'].';';
				}
			} else {
				// TODO: strict permission handling:
				// add $key to list of database_only keys, removing again if it's found later
				// this would detect permissions that are in the database, but not designated by the schema
			}
		}
	}

	private function merge_file_stmts($stmtkey,...$file_stmts){
		$files = [];
		$merged = [];
		foreach($file_stmts as $stmt){
			$files = array_merge($files,$stmt['files']);
			if(!isset($merged)) continue;
			foreach($stmt['table'] as $key => $value){
				if(!isset($merged[$key])){
					$merged[$key] = $value;
				} elseif($merged[$key]!=$value){
					if($key == 'priv_types'){
						$merged[$key] = array_merge($merged[$key],$value);
					} else {
						$merged = null;
						break;
					}
				}
			}
		}
		if($merged['type']=='grant'){
			$sql = $this->convert_grant_to_sql($merged,'GRANT');
			if($sql) return [['table'=>$merged,'raw'=>$sql,'files'=>$files],null];
		}
		$files = implode(",\n",$files);
		return [null,"Failed merging [$stmtkey] in files:\n$files"];
	}

	private function do_revoke_section($db, $active_keys){
		$data = [];
		$sql = [];
		foreach($db['table'] as $key => $stmt){
			if(!in_array($key, $active_keys)){
				if($stmt['priv_types'] == ['USAGE' => 'USAGE']){
					continue;
				}
				$data[] = $this->create_data_row('Database', $stmt, 'remove');
				$sql[] = $this->convert_grant_to_sql($stmt, 'REVOKE');
			}
		}
		return ['data' => $data, 'sql' => $sql];
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

	private function get_table($stmt, $native_db = null){
		$database = preg_replace('/^`([^`]*)`$/','$1',$stmt['database']);
		$table = preg_replace('/^`([^`]*)`$/','$1',$stmt['table']);
		if($database == $native_db) return $table;
		return $database.'.'.$table;
	}

	private function get_stmts($file){
		$table = [];
		$raw = [];
		foreach($file->get_all_stmts() as $stmt){
			$obj = SQLFile::parse_statement($stmt, ['ignore_host'=>$this->ignore_host]);
			if($obj['type'] != 'grant' && $obj['type'] != 'revoke') continue;
			if(!isset($user) || $obj['user'] == $user){
				$table[$obj['key']] = $obj;
				$raw[$obj['key']] = $stmt;
			}
		}
		return ['table'=>$table,'raw'=>$raw];
	}

	private function get_dbdata($dbuser = null, $db = null){
		if(DB::$isloggedin){
			if(!isset($db)) $db = Config::get('database');
			
			$grants = [];
			$raw = [];
			$result = DB::sql("SELECT * FROM `information_schema`.`table_privileges` WHERE `table_schema` = '$db'");
			foreach($result as $row){
				$this->merge_into_grants($grants, Format::grant_row_to_description($row));
			}
			$result = DB::sql("SELECT * FROM `information_schema`.`column_privileges` WHERE `table_schema` = '$db'");
			foreach($result as $row){
				$this->merge_into_grants($grants, Format::grant_row_to_description($row));
			}
			if(!isset($dbuser) && !empty($db)){
				$result = DB::sql("SELECT 1 FROM mysql.user WHERE user='$db'");
				if($result && $result->num_rows){
					$dbuser = "'$db'@'localhost'";
				}
			}
			if(isset($dbuser)){
				$result = DB::sql("SHOW GRANTS FOR $dbuser");
				if($result){
					while($row = $result->fetch_row()){
						$obj = SQLFile::parse_statement($row[0], ['ignore_host'=>$this->ignore_host]);
						$grants[$obj['key']] = $obj;
						$raw[$obj['key']] = $row[0];
					}
				}
			}
		}
		return ['table'=>$grants,'raw'=>$raw];
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

	private function create_data_row($location, $stmt, $class = ''){
		$data = ['location' => $location];
		return ['data' => $data + $this->flatten_stmt_obj($stmt), 'class' => $class];
	}

	private function flatten_stmt_obj($stmt){
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

	private function convert_grant_to_sql($stmt, $action){
		if($stmt['type'] != 'grant') return false;
		$stmt = $this->flatten_stmt_obj($stmt);
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
?>