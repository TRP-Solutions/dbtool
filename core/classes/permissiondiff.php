<?php

class PermissionDiff {
	private $prim, $seco, $modules, $ignore_host, $sql_vars;

	public function __construct($prim, $seco, $modules, $vars = []){
		$this->prim = $prim;
		$this->seco = $seco;
		$this->modules = $modules;
		$this->ignore_host = defined('PERMISSION_IGNORE_HOST') && PERMISSION_IGNORE_HOST;
		$this->sql_vars = $vars;
	}

	public function diff_files($filelist, $show_all = false){
		$diff = ['files' => []];
		$db = $this->get_dbdata();
		$active_keys = [];
		foreach($filelist as $filename){
			$d = $this->do_section($filename, $db, $show_all);
			if(is_array($d['keys'])){
				$active_keys = array_merge($active_keys,$d['keys']);
			}
			$d['title'] = basename($filename);
			$diff['files'][$filename] = $d;
		}
		$diff['revoke'] = $this->do_revoke_section($db, $active_keys);
		return $diff;
	}

	public function diff($show_all = false){
		$diff = ['files' => []];

		$used_modules = [];
		$sections = [];
		$rest = [];
		foreach(SQLFile::get_all('priv.sql') as $fname){
			$filename = explode('/',$fname);
			$filename = $filename[count($filename)-1];
			$mods = explode('-', explode('.', $filename, 2)[0]);
			$included = true;
			foreach($mods as $mod){
				if(!in_array($mod, $this->modules)){
					$included = false;
					break;
				} elseif(!in_array($mod, $used_modules)) {
					$used_modules[] = $mod;
				}
			}
			if($included){
				$sections[$fname] = 'Modules: '.implode(' & ', $mods);
			} elseif($filename == 'for_all.priv.sql'){
				$sections[$fname] = 'For all';
			} else {
				$rest[$fname] = "File: $fname";
			}
		}
		if(defined('PERMISSION_INCLUDE_ALL_FILES') && PERMISSION_INCLUDE_ALL_FILES){
			$sections = array_merge($sections, $rest);
		}
		$diff['unused_mods'] = array_diff($this->modules, $used_modules);
		$db = $this->get_dbdata();
		$active_keys = [];
		foreach($sections as $filename => $title){
			$d = $this->do_section($filename, $db, $show_all);
			if(is_array($d['keys'])){
				$active_keys = array_merge($active_keys,$d['keys']);
			}
			$d['title'] = $title;
			$diff['files'][$filename] = $d;
		}
		$diff['revoke'] = $this->do_revoke_section($db, $active_keys);
		return $diff;
	}

	private function do_section($filename, $db, $show_all = false){
		$file = $this->get_filedata($filename);
		if($file){
			$diff = $this->do_diff($db, $file, true, $show_all);
			$diff['filename'] = $filename;
			return $diff;
		}
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
				$sql[] = $this->convert_grant_to_revoke($stmt);
			}
		}
		return ['data' => $data, 'sql' => $sql];
	}


	private function do_diff($db, $file, $file_only = false, $show_matches = false){
		$compare = function($a, $b) use (&$compare){
			if(is_array($a)){
				if(is_array($b)){
					$adiff = array_udiff($a, $b, $compare);
					$bdiff = array_udiff($b, $a, $compare);
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
		};

		$data = [];
		$sql = [];
		$keys = array_unique(array_merge(array_keys($db['table']), array_keys($file['table'])));
		foreach($keys as $key){
			if(isset($db['table'][$key])){
				if(isset($file['table'][$key])){
					ksort($db['table'][$key]['priv_types']);
					ksort($file['table'][$key]['priv_types']);
					$dbdiff = array_udiff_assoc($db['table'][$key], $file['table'][$key], $compare);
					$filediff = array_udiff_assoc($file['table'][$key], $db['table'][$key], $compare);
					if(!empty($dbdiff) || !empty($filediff)){
						$data[] = $this->create_data_row('Database', $db['table'][$key], 'diff');
						$data[] = $this->create_data_row('Schemafile', $file['table'][$key], 'diff');
						$sql[] = $this->convert_grant_to_revoke($db['table'][$key]).';';
						$sql[] = $file['raw'][$key].';';
					} elseif($show_matches) {
						$data[] = $this->create_data_row('Both', $db['table'][$key], 'match');
					}
				} elseif(!$file_only) {
					$data[] = $this->create_data_row('Database', $db['table'][$key], 'remove');
					$sql[] = $this->convert_grant_to_revoke($db['table'][$key]).';';
				}
			} elseif(isset($file['table'][$key])){
				$data[] = $this->create_data_row('Schemafile', $file['table'][$key], 'add');
				$sql[] = $file['raw'][$key].';';
			}
		}
		return ['data' => $data, 'sql'=> $sql, 'keys' => array_keys($file['table'])];
	}

	

	private function get_filedata($filename){
		$vars = ['[PRIM]' => $this->prim];
		if(isset($this->seco)){
			$vars['[SECO]'] = $this->seco;
		}
		foreach($this->sql_vars as $key => $value){
			$vars["[$key]"] = $value;
		}
		$file = new SQLFile($filename, $vars);
		if($file->exists){
			$table = [];
			$raw = [];
			foreach($file->get_all_stmts() as $stmt){
				$obj = SQLFile::parse_statement($stmt, ['ignore_host'=>$this->ignore_host]);
				if(!isset($user) || $obj['user'] == $user){
					$table[$obj['key']] = $obj;
					$raw[$obj['key']] = $stmt;
				}
			}
			return ['table'=>$table,'raw'=>$raw];
		} else {
			return false;
		}
	}

	private function get_dbdata(){
		if(DB::$isloggedin && isset($this->prim)){
			$grants = [];
			$raw = [];
			$result = DB::sql("SELECT * FROM `information_schema`.`table_privileges` WHERE `table_schema` = '{$this->prim}'");
			foreach($result as $row){
				if($this->ignore_host){
					$grantee = explode('@',$row['GRANTEE'])[0];
				} else {
					$grantee = $row['GRANTEE'];
				}
				$key = 'grant:'.$grantee.':`'.$row['TABLE_SCHEMA'].'`.`'.$row['TABLE_NAME'].'`';
				if(!isset($grants[$key])){
					$desc = [
						'type' => 'grant',
						'key' => $key,
						'priv_types' => [$row['PRIVILEGE_TYPE'] => $row['PRIVILEGE_TYPE']],
						'database' => '`'.$row['TABLE_SCHEMA'].'`',
						'table' => '`'.$row['TABLE_NAME'].'`',
						'user' => $grantee
					];
					$grants[$key] = $desc;
				} else {
					$grants[$key]['priv_types'][$row['PRIVILEGE_TYPE']] = $row['PRIVILEGE_TYPE'];
				}
			}
			$result = DB::sql("SELECT * FROM `information_schema`.`column_privileges` WHERE `table_schema` = '{$this->prim}'");
			foreach($result as $row){
				if($this->ignore_host){
					$grantee = explode('@',$row['GRANTEE'])[0];
				} else {
					$grantee = $row['GRANTEE'];
				}
				$key = 'grant:'.$grantee.':`'.$row['TABLE_SCHEMA'].'`.`'.$row['TABLE_NAME'].'`';
				if(!isset($grants[$key])){
					$desc = [
						'type' => 'grant',
						'key' => $key,
						'priv_types' => [$row['PRIVILEGE_TYPE'] => ['priv_type'=>$row['PRIVILEGE_TYPE'],'column_list'=>[$row['COLUMN_NAME']]]],
						'database' => '`'.$row['TABLE_SCHEMA'].'`',
						'table' => '`'.$row['TABLE_NAME'].'`',
						'user' => $grantee
					];
					$grants[$key] = $desc;
				} else {
					$types = $grants[$key]['priv_types'];
					if(isset($types[$row['PRIVILEGE_TYPE']]) && is_array($types[$row['PRIVILEGE_TYPE']])){
						$grants[$key]['priv_types'][$row['PRIVILEGE_TYPE']]['column_list'][] = $row['COLUMN_NAME'];
					} else {
						$grants[$key]['priv_types'][$row['PRIVILEGE_TYPE']] = ['priv_type'=>$row['PRIVILEGE_TYPE'],'column_list'=>[$row['COLUMN_NAME']]];
					}
				}
			}
			$result = DB::sql("SHOW GRANTS FOR '{$this->prim}'@'localhost'");
			if($result){
				while($row = $result->fetch_row()){
					$obj = SQLFile::parse_statement($row[0], ['ignore_host'=>$this->ignore_host]);
					$grants[$obj['key']] = $obj;
					$raw[$obj['key']] = $row[0];
				}
			}
			return ['table'=>$grants,'raw'=>$raw];
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
					$stmt['priv_types'][$key] = $type['priv_type'] .' (`'.implode('`,`',$type['column_list']).'`)';
				}
			}
			$stmt['priv_types'] = implode(', ', $stmt['priv_types']);
		}
		return $stmt;
	}

	private function convert_grant_to_revoke($stmt){
		if($stmt['type'] != 'grant') return false;
		$stmt = $this->flatten_stmt_obj($stmt);
		$sql = "REVOKE {$stmt['priv_types']} ON ";
		if(isset($stmt['object_type'])) $sql .= "{$stmt['object_type']} ";
		if(isset($stmt['database'])) $sql .= "{$stmt['database']}.";
		$sql .= "{$stmt['table']} FROM {$stmt['user']};";
		return $sql;
	}
}
?>