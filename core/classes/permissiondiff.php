<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/
class PermissionDiff {
	private $ignore_host, $files, $file_stmts = [];

	public function __construct($files){
		$this->ignore_host = defined('PERMISSION_IGNORE_HOST') && PERMISSION_IGNORE_HOST;
		$this->files = $files;
	}

	public function run(&$partial_result = []){
		$file_stmts = [];
		$users = [];
		$filter = [];
		foreach($this->files as $file){
			$stmts = $this->get_stmts($file);
			if(isset($stmts['error'])){
				$partial_result['errors'][] = ['errno'=>5,'error'=>'File parse conflict: '.$stmts['error']];
				continue;
			}
			$file_stmts[$file->get_filename()] = $stmts;
			foreach($stmts['table'] as $table){
				if(!in_array($table['user'], $users)) $users[] = $table['user'];
				$pairkey = $table['database'].':'.$table['user'];
				$filter[$pairkey] = true;
			}
		}
		$db = $this->get_dbdata($users,$filter);
		foreach($file_stmts as $filename => $stmts){
			$this->diff($partial_result, $filename, $stmts, $db);
		}
		return $partial_result;
	}

	private function diff(&$partial_result, $filename, $stmts, $db){
		$selected_db = Config::get('database');
		$strict_permission_handling = !(Config::get('foreign_permissions') == 'allow');
		$keys = array_unique(array_merge(array_keys($db['table']), array_keys($stmts['table'])));
		foreach($keys as $key){
			if(isset($stmts['table'][$key])){
				// Grant exists in the current file
				ksort($stmts['table'][$key]['priv_types']);
				$table = $this->get_table($stmts['table'][$key]);
				if(isset($partial_result['tables'][$table]) && $partial_result['tables'][$table]['type']=='database_only'){
					// Grant previously not found in a file is given fully or partially now
					// -> Remove database only diff
					unset($partial_result['tables'][$table]);
				}
				$conflict = !empty($this->file_stmts[$table][$key]);
				$this->file_stmts[$table][$key][] = $stmt = ['table'=>$stmts['table'][$key],'raw'=>$stmts['raw'][$key],'files'=>[$filename]];
				if($conflict){
					// Grant also exists in a previous file
					// -> try to merge
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
					// Grant also exists in database
					// -> find difference
					ksort($db['table'][$key]['priv_types']);
					$dbdiff = array_udiff_assoc($db['table'][$key], $stmt['table'], [$this,'compare']);
					$filediff = array_udiff_assoc($stmt['table'], $db['table'][$key], [$this,'compare']);
					if(!empty($dbdiff) || !empty($filediff)){
						if(isset($merge_error)) {
							$this->init_result($partial_result, $table, 'intersection', $stmt['files']);
							$this->write_merge_error($partial_result, $table, $key);
						} else {
							list($remove, $add) = $this->file_is_subset($filediff, $dbdiff);
							if(!empty($remove) && $strict_permission_handling || !empty($add)){
								$this->init_result($partial_result, $table, 'intersection', $stmt['files']);
							}
							if(!empty($remove) && $strict_permission_handling){
								$this->remove_permission(
									$partial_result['tables'][$table],
									$key,
									['priv_types'=>$remove] + $db['table'][$key]
								);
							}
							if(!empty($add)){
								$this->add_permission($partial_result['tables'][$table], $key, ['priv_types'=>$add] + $stmt['table'], $conflict);
							}
						}
					} else {
						// Grant is the same in database and file(s)
						// -> nothing should be done and any previous diffs found are removed
						$this->unset_permission_in_result($partial_result, $table, $key.'-file');
						$this->unset_permission_in_result($partial_result, $table, $key.'-db');
					}
				} else {
					// Grant only exists in file
					$this->init_result($partial_result, $table, 'file_only', $stmt['files']);
					$this->add_permission($partial_result['tables'][$table], $key, $stmt['table']);
				}
			} elseif($strict_permission_handling) {
				// Grant only exists in database
				if($db['table'][$key]['priv_types'] == ['USAGE'=>'USAGE']){
					continue;
				}
				if(!empty($selected_db) && "`$selected_db`" != $db['table'][$key]['database']){
					continue;
				}
				$table = $this->get_table($db['table'][$key]);
				if(isset($this->file_stmts[$table][$key])){
					// Grant is given (fully or partially) in previous file
					continue;
				}

				$this->init_result($partial_result, $table, 'database_only');
				$this->remove_permission(
					$partial_result['tables'][$table],
					$key,
					$db['table'][$key]
				);
			}
		}
	}

	private function add_permission(&$result_table, $key, $stmt_table, $conflict = false){
		$this->write_permission_to_result(
			$result_table,
			$key.'-file',
			$stmt_table,
			'Schemafile'.($conflict ? ' (merged)':''),
			['class'=>$conflict ? 'bg-warning' : 'bg-success']
		);
	}

	private function remove_permission(&$result_table, $key, $stmt_table){
		$this->write_permission_to_result(
			$result_table,
			$key.'-db',
			$stmt_table,
			'Database',
			['type'=>'REVOKE','class'=>'bg-danger']
		);
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

	private function write_merge_error(&$partial_result, $table, $key){
		$this->unset_permission_in_result($partial_result, $table, $key.'-file');
		$i = 0;
		foreach($this->file_stmts[$table][$key] as $conflict_stmt){
			$this->write_permission_to_result(
				$partial_result['tables'][$table],
				$key.'-file-'.$i,
				$conflict_stmt['table'],
				'Schemafile',
				['class'=>'bg-warning']
			);
			$i++;
		}
	}

	private function init_result(&$partial_result, $table, $type, $files = []){
		if(!isset($partial_result['tables'][$table]) || isset($partial_result['tables'][$table]['is_empty']) && $partial_result['tables'][$table]['is_empty']){
			$partial_result['tables'][$table] = ['name'=>$table,'sourcefiles'=>$files,'type'=>$type];
		} else {
			$result = &$partial_result['tables'][$table];
			if($result['type'] != $type){
				$result['type'] = 'intersection';
			}
			foreach($files as $file){
				if(!in_array($file, $result['sourcefiles'])){
					$result['sourcefiles'][] = $file;
				}
			}
		}
	}

	private function write_permission_to_result(&$result_table, $key, $stmt_table, $title, $options = []){
		$class = $options['class'] ?? 'bg-info';
		$type = $options['type'] ?? 'GRANT';
		$result_table['permissions'][$key] = $this->create_data_row($title, $stmt_table, $class);
		$result_table['sql'][$key] = $this->convert_grant_to_sql($stmt_table, $type);
	}

	private function unset_permission_in_result(&$partial_result, $table, $key){
		unset($partial_result['tables'][$table]['permissions'][$key]);
		unset($partial_result['tables'][$table]['sql'][$key]);
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
						$keys = [];
						$values = array_map(function($a, $b) use (&$keys){
							if(!isset($a) || !isset($b['column_list'])){
								$keys[] = $b;
								return $b;
							}
							if(!isset($b) || !isset($a['column_list'])){
								$keys[] = $a;
								return $a;
							}
							$keys[] = $a['priv_type'];
							return ['priv_type'=>$a['priv_type'],'column_list'=>array_merge($a['column_list'],$b['column_list'])];
						},$merged[$key],$value);
						$merged[$key] = array_combine($keys,$values);
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
		if(!isset($native_db)) $native_db = Config::get('database');
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
			$filename = $file->get_filename();
			if($obj['type'] != 'grant' && $obj['type'] != 'revoke') continue;
			if(!isset($user) || $obj['user'] == $user){
				if(isset($table[$obj['key']])){
					list($merged,$merge_error) = $this->merge_file_stmts($obj['key'],
						['table'=>$table[$obj['key']],'files'=>[$filename]],
						['table'=>$obj,'files'=>[$filename]]);
					if(isset($merge_error)){
						return ['table'=>null,'raw'=>null,'error'=>$merge_error];
					}
					$table[$obj['key']] = $merged['table'];
					$raw[$obj['key']] = $merged['raw'];
				} else {
					$table[$obj['key']] = $obj;
					$raw[$obj['key']] = $stmt;
				}
			}
		}
		return ['table'=>$table,'raw'=>$raw,'error'=>null];
	}

	private function get_dbdata($users, $filter = []){
		$grants = [];
		$raw = [];
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
		return ['table'=>$grants,'raw'=>$raw];
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