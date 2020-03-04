<?php

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
			$file_stmts[$file->get_filename()] = $stmts;
			foreach($stmts['table'] as $table){
				if(!in_array($table['user'], $users)) $users[] = $table['user'];
				$pairkey = $table['database'].':'.$table['user'];
				$filter[$pairkey] = true;
			}
		}
		$db = $this->get_dbdata($users, $filter);
		foreach($file_stmts as $filename => $stmts){
			$this->diff($partial_result, $filename, $stmts, $db);
		}
		return $partial_result;
	}

	private function diff(&$partial_result, $filename, $stmts, $db){
		$strict_permission_handling = Config::get('strict') == true;
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
						if(array_keys($filediff) == ['priv_types']){
							$priv_diff = array_udiff_assoc($filediff['priv_types'], $dbdiff['priv_types'], [$this,'compare']);
							$file_priv_is_subset = empty($priv_diff);
						} else {
							$file_priv_is_subset = false;
						}
						if(!$file_priv_is_subset || $strict_permission_handling){
							// Grant is meaningfully different in database and file(s)
							// -> write intersection diff
							$this->handle_intersection_permission($partial_result, $table, $key, $db, $stmt, $conflict, $conflict && isset($merge_error));
						}
						// Else: Grant is a subset and strict handling is off -> do nothing
					} elseif($conflict){
						// Grant is the same in database and file(s)
						// -> nothing should be done and any previous diffs found are removed
						$this->unset_permission_in_result($partial_result, $table, $key.'-file');
						$this->unset_permission_in_result($partial_result, $table, $key.'-db');
					}
				} else {
					// Grant only exists in file
					$this->init_result($partial_result, $table, 'file_only', $stmt['files']);
					$this->write_permission_to_result(
						$partial_result['tables'][$table],
						$key.'-file',
						$stmt['table'],
						'Schemafile'.($conflict ? ' (merged)':''),
						['class'=>$conflict ? 'bg-warning' : 'bg-success']
					);
				}
			} elseif($strict_permission_handling) {
				// Grant only exists in database
				if($db['table'][$key]['priv_types'] == ['USAGE'=>'USAGE']){
					continue;
				}
				$table = $this->get_table($db['table'][$key]);
				if(isset($this->file_stmts[$table][$key])){
					// Grant is given (fully or partially) in previous file
					continue;
				}

				$this->init_result($partial_result, $table, 'database_only');
				$this->write_permission_to_result(
					$partial_result['tables'][$table],
					$key.'-db',
					$db['table'][$key],
					'Database',
					['type'=>'REVOKE','class'=>'bg-danger']
				);
			}
		}
	}

	private function handle_intersection_permission(&$partial_result, $table, $key, $db, $stmt, $conflict, $merge_error){
		$this->init_result($partial_result, $table, 'intersection', $stmt['files']);
		$this->write_permission_to_result(
			$partial_result['tables'][$table],
			$key.'-db',
			$db['table'][$key],
			'Database',
			['type'=>'REVOKE']
		);
		if($merge_error) {
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
		} else {
			$this->write_permission_to_result(
				$partial_result['tables'][$table],
				$key.'-file',
				$stmt['table'],
				'Schemafile'.($conflict ? ' (merged)':''),
				['class'=>$conflict ? 'bg-warning' : 'bg-info']
			);
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
			if($obj['type'] != 'grant' && $obj['type'] != 'revoke') continue;
			if(!isset($user) || $obj['user'] == $user){
				$table[$obj['key']] = $obj;
				$raw[$obj['key']] = $stmt;
			}
		}
		return ['table'=>$table,'raw'=>$raw];
	}

	private function get_dbdata($users, $filter = []){
		$grants = [];
		$raw = [];
		if(DB::$isloggedin){
			$db = Config::get('database');
			
			$result = DB::sql("SELECT * FROM `information_schema`.`table_privileges` WHERE `table_schema` = '$db'");
			foreach($result as $row){
				$desc = Format::grant_row_to_description($row);
				if($this->desc_is_allowed($desc,$filter)){
					$this->merge_into_grants($grants, $desc);
				}
			}
			$result = DB::sql("SELECT * FROM `information_schema`.`column_privileges` WHERE `table_schema` = '$db'");
			foreach($result as $row){
				$desc = Format::grant_row_to_description($row);
				if($this->desc_is_allowed($desc,$filter)){
					$this->merge_into_grants($grants, $desc);
				}
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