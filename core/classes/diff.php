<?php
require_once __DIR__.'/db.php';
require_once __DIR__.'/sqlfile.php';

class Diff {
	const DB_PREFIX = 'diff_php_temporary_database_';
	private $compare_tables_stmt;
	private $compare_keys_stmt;
	private $compare_tables = <<<SQL
		SELECT
			't1' as `table`,
			column_name,
			ordinal_position,
			column_default as `default`,
			is_nullable as `nullable`,
			data_type,
			character_maximum_length as `char_max_length`,
			numeric_precision as `num_precision`,
			numeric_scale as `num_scale`,
			character_set_name as `char_set`,
			collation_name as `collation`,
			column_type as `type`,
			extra,
			column_comment as `comment`
		FROM information_schema.columns AS i1
		WHERE table_schema=? AND table_name=?
		UNION ALL
		SELECT
			't2' as `table`,
			column_name,
			ordinal_position,
			column_default as `default`,
			is_nullable as `nullable`,
			data_type,
			character_maximum_length as `char_max_length`,
			numeric_precision as `num_precision`,
			numeric_scale as `num_scale`,
			character_set_name as `char_set`,
			collation_name as `collation`,
			column_type as `type`,
			extra,
			column_comment as `comment`
		FROM information_schema.columns AS i2
		WHERE table_schema=? AND table_name=?
		ORDER BY `table`, ordinal_position
SQL;

	private $compare_keys = <<<SQL
		SELECT
			't1' as `table`,
			index_name,
			column_name,
			non_unique,
			seq_in_index
		FROM information_schema.statistics as i1
		WHERE table_schema=? AND table_name=?
		UNION ALL
		SELECT
			't2' as `table`,
			index_name,
			column_name,
			non_unique,
			seq_in_index
		FROM information_schema.statistics as i2
		WHERE table_schema=? AND table_name=?
		ORDER BY index_name, seq_in_index
SQL;

private $compare_options = <<<SQL
		SELECT
			't1' as `table`,
			engine,
			table_comment
		FROM information_schema.tables as i1
		WHERE table_schema=? AND table_name=?
		UNION ALL
		SELECT
			't2' as `table`,
			engine,
			table_comment
		FROM information_schema.tables as i2
		WHERE table_schema=? AND table_name=?
SQL;

	function __construct()
	{
		if(DB::$isloggedin){
			$this->compare_tables_stmt = DB::prepare($this->compare_tables);
			$this->compare_keys_stmt = DB::prepare($this->compare_keys);
			$this->compare_options_stmt = DB::prepare($this->compare_options);
		}
	}

	public function diff_sql($dbname, $files, $vars = []){
		$results = ['errno'=>0,'files'=>[]];
		$tables = $this->list_tables($dbname);
		$tables = array_combine($tables,$tables);
		foreach($files as $filename){
			$file_only_tables = [];
			$intersection_columns = [];
			$intersection_keys = [];
			$intersection_options = [];
			$alter_queries = [];
			$file = new SQLFile($filename, $vars);
			$stmts = $file->get_all_stmts();
			foreach($stmts as $stmt){
				$file_table = SQLFile::parse_statement($stmt);
				if(isset($file_table['error'])){
					return [
						'errno'=> 1,
						'error'=> "Parse Error in file \"$filename\", table `$file_table[name]`: $file_table[error]"
					];
				} elseif(isset($tables[$file_table['name']])){
					unset($tables[$file_table['name']]);
					$query = DB::sql("SHOW CREATE TABLE `$file_table[name]`");
					if($query->num_rows){
						$db_table = SQLFile::parse_statement($query->fetch_assoc()['Create Table']);
						if(isset($db_table['error'])){
							return [
								'errno' =>2,
								'error' => "Parse Error in database table `$db_table[name]`: $db_table[error]"
							];
						}
						$diff = $this->compare_tables($file_table, $db_table);
						if(!empty($diff['columns'])) $intersection_columns[$file_table['name']] = $diff['columns'];
						if(!empty($diff['keys'])) $intersection_keys[$file_table['name']] = $diff['keys'];
						if(!empty($diff['options'])) $intersection_options[$file_table['name']] = $diff['options'];
						$queries = $this->generate_alter_queries($file_table['name'], $diff);
						if(!empty($queries)) $alter_queries[$file_table['name']] = $queries;
					}
				} else {
					$file_only_tables[$file_table['name']] = trim($stmt).';';
				}
			}
			$db_only_tables = array_keys($tables);
			$results['files'][$filename] = [
				'errno' => 0,
				'tables_in_database_only' => $db_only_tables,
				'tables_in_file_only' => $file_only_tables,
				'intersection_columns' => $intersection_columns,
				'intersection_keys' => $intersection_keys,
				'intersection_options' => $intersection_options,
				'drop_queries' => $this->build_drop_query($db_only_tables, $dbname),
				'create_queries' => $file_only_tables,
				'alter_queries' => $alter_queries
			];
		}
		return $results;
	}

	private function compare_tables($file_table, $db_table){
		$db_key = 't1';
		$file_key = 't2';
		$file_columns = [];
		$file_keys = [];

		// FIXME : avoid duplicate "AFTER" moves

		$file_ordinals = [];

		foreach($file_table['columns'] as $col){
			if($col['type'] == 'column'){
				$col = $this->convert_to_format_A($col); //compatibility with web diffview
				$file_ordinals[$col['colname']] = $col['ordinal_number'];
				unset($col['ordinal_number']);
				$file_columns[$col['colname']] = $col;
			} elseif($col['type'] == 'index'){
				unset($col['type']); //compatibility with web diffview
				$col['cols'] = array_map(['SQLFile','encode_index_column'], $col['index_columns']);
				$name = $col['index_type'] == 'primary' ? 'PRIMARY' : (isset($col['name']) ? $col['name'] : implode(', ',$col['cols']));
				if(isset($file_keys[$name])){
					$i = 1;
					$basename = $name;
					while(isset($file_keys[$name])){
						$name = "$basename,$i";
						$i++;
					}
				}
				$file_keys[$name] = $col;
			}
		}

		$db_ordinals = [];
		$file_to_db_order_offset = 0;
		$out_of_order = [];
		$columns = [];
		$keys = [];
		foreach($db_table['columns'] as $col){
			if($col['type'] == 'column'){
				$col = $this->convert_to_format_A($col); //compatibility with web diffview
				$name = $col['colname'];
				if($file_to_db_order_offset || $col['after'] != $file_columns[$name]['after']){
					$search = array_search($col['ordinal_number'],$file_ordinals);
					if(in_array($search,$out_of_order)) $file_to_db_order_offset -= 1;
				}
				if($file_ordinals[$name] + $file_to_db_order_offset != $col['ordinal_number']){
					if($file_ordinals[$name] > $col['ordinal_number']){
						if($file_ordinals[$file_columns[$name]['after']] + $file_to_db_order_offset == $col['ordinal_number']){
							// a column was moved to the ordinal position of this column
							$out_of_order[] = $file_columns[$name]['after'];
							$file_to_db_order_offset -= 1;
						} else {
							// this column was moved to later in the order
							$out_of_order[] = $name;
							$file_to_db_order_offset += 1;
						}
					}
				}
				
				$db_ordinals[$name] = $col['ordinal_number'];
				unset($col['ordinal_number']);
				if(!isset($file_columns[$name])) $columns[$name] = [$db_key=>$col];
				else {
					if($col['after'] != $file_columns[$name]['after']
						&& !in_array($name, $out_of_order)){
						unset($col['after']);
						unset($file_columns[$name]['after']);
					}
					if($col != $file_columns[$name]) $columns[$name] = [$db_key=>$col,$file_key=>$file_columns[$name]];
					unset($file_columns[$name]);
				}
			} elseif($col['type'] == 'index'){
				unset($col['type']); //compatibility with web diffview
				$col['cols'] = array_map(['SQLFile','encode_index_column'], $col['index_columns']);
				$name = $col['index_type'] == 'primary' ? 'PRIMARY' : (isset($col['name']) ? $col['name'] : implode(', ',$col['cols']));
				if(!isset($file_keys[$name])) $keys[$name] = [$db_key=>$col];
				else {
					if($col != $file_keys[$name]) $keys[$name] = [$db_key=>$col,$file_key=>$file_keys[$name]];
					unset($file_keys[$name]);
				}
			}
		}
		foreach($file_columns as $name => $col){
			$columns[$name] = [$file_key=>$col];
		}
		foreach($file_keys as $name => $col){
			$keys[$name] = [$file_key=>$col];
		}
		$options = [];
		$ignore_db_only_options = ['AUTO_INCREMENT'];
		foreach($db_table['table_options'] as $key => $value){
			if(!isset($file_table['table_options'][$key])){
				if(!in_array($key, $ignore_db_only_options)) $options[$key] = [$db_key=>$value];
			} elseif($file_table['table_options'][$key] != $value) $options[$key] = [$db_key=>$value,$file_key=>$file_table['table_options'][$key]];
		}
		foreach($file_table['table_options'] as $key => $value){
			if(!isset($db_table['table_options'][$key])) $options[$key] = [$file_key=>$value];
		}
		return ['columns'=>$columns,'keys'=>$keys,'options'=>$options];
	}

	private function convert_to_format_A($old_col){
		switch($old_col['nullity']){
			case 'NOT NULL': $nullable = 'NO'; break;
			case 'NULL': $nullable = 'YES'; break;
			default: $nullable = 'YES '; //hack to make default visually identical to NULL, but still be treated as undefined in detection
		}
		$new_col = [
			'colname'=>$old_col['name'],
			'nullable'=>$old_col['nullity'] == 'NULL' ? 'YES' : ($old_col['nullity'] == 'NOT NULL' ? 'NO' : 'YES '),
			'data_type'=>$old_col['datatype']['name']
		];
		if(isset($old_col['default'])) $new_col['default'] = $old_col['default'];
		if(isset($old_col['datatype']['char_max_length'])) $new_col['char_max_length'] = $old_col['datatype']['char_max_length'];
		if(isset($old_col['datatype']['precision'])) $new_col['num_precision'] = $old_col['datatype']['precision'];
		if(isset($old_col['datatype']['decimals'])) $new_col['num_scale'] = $old_col['datatype']['decimals'];
		if(isset($old_col['datatype']['character set'])) $new_col['char_set'] = $old_col['datatype']['character set'];
		if(isset($old_col['datatype']['collate'])) $new_col['collation'] = $old_col['datatype']['collate'];

		$new_col['type'] = SQLFile::encode_datatype($old_col['datatype']);

		if(isset($old_col['auto_increment']) && $old_col['auto_increment']) $new_col['extra'] = 'auto_increment';
		if(isset($old_col['comment'])){
			$new_col['comment'] = $old_col['comment'];
			$len = strlen($new_col['comment']);
			if($new_col['comment'][0] == "'" && $new_col['comment'][$len-1] == "'"){
				$new_col['comment'] = substr($new_col['comment'], 1, -1);
			}
		}

		$new_col['after'] = $old_col['after'];
		$new_col['ordinal_number'] = $old_col['ordinal_number'];

		return $new_col;
	}

	private function format_A_to_def($col){
		$def = "`$col[colname]` $col[type]";
		if($col['nullable'] == 'NO') $def .= ' NOT NULL';
		if(isset($col['default'])) $def .= " DEFAULT $col[default]";
		if(isset($col['extra']) && $col['extra'] == 'auto_increment') $def .= ' AUTO_INCREMENT';
		if(isset($col['comment'])) $def .= " COMMENT '$col[comment]'";
		return $def;
	}

	private function generate_alter_queries($table_name, $table_diff){
		$drop_keys = [];
		$alter_columns = [];
		$add_keys = [];
		$alter_options = [];

		foreach($table_diff['columns'] as $colname => $diff){
			if(isset($diff['t1']) && isset($diff['t2'])){
				// modify
				$query = "ALTER TABLE `$table_name` MODIFY COLUMN ".$this->format_A_to_def($diff['t2']);
				if($diff['t1']['after'] != $diff['t2']['after']
					&& $diff['t1']['ordinal_number'] >= $diff['t2']['ordinal_number']){
					$query .= $this->build_column_query_after($diff['t2']);
				}
			} elseif(isset($diff['t2'])){
				// add
				$query = "ALTER TABLE `$table_name` ADD COLUMN ".$this->format_A_to_def($diff['t2']).$this->build_column_query_after($diff['t2']);
			} elseif(isset($diff['t1'])){
				// drop
				$query = "ALTER TABLE `$table_name` DROP COLUMN `$colname`";
			}
			$alter_columns[] = $query.';';
		}

		foreach($table_diff['keys'] as $keyname => $diff){
			if(isset($diff['t1'])){
				if($diff['t1']['index_type'] == 'primary'){
					$drop_keys[] = "ALTER TABLE `$table_name` DROP PRIMARY KEY;";
				} else {
					$drop_keys[] = "ALTER TABLE `$table_name` DROP KEY $keyname;";
				}
			}
			if(isset($diff['t2'])){
				$query = "ALTER TABLE `$table_name` ADD ";
				if($diff['t2']['index_type'] == 'unique') $query .= 'UNIQUE ';
				elseif($diff['t2']['index_type'] == 'primary') $query .= 'PRIMARY ';
				$query .= "KEY ";
				if(isset($diff['t2']['name'])) $query .= $diff['t2']['name'];
				$query .= '('.implode(',',$diff['t2']['cols']).');';
				$add_keys[] = $query;
			}
		}

		$option_defaults = [
			'COMMENT' => "''"
		];
		foreach($table_diff['options'] as $optname => $diff){
			if(isset($diff['t2'])){
				$query = "ALTER TABLE `$table_name` $optname = $diff[t2]";
			} elseif(isset($option_defaults[$optname])) {
				$query = "ALTER TABLE `$table_name` $optname = ".$option_defaults[$optname];
			} else {
				$query = null;
			}
			if(isset($query)) $alter_options[] = $query.';';
		}

		return array_merge($drop_keys,$alter_columns,$add_keys,$alter_options);
	}

	public function diff_multi($dbname, $files, $vars = []){
		$results = ['errno'=>0,'files'=>[]];
		foreach($files as $filename){
			$file = new SQLFile($filename, $vars);
			$result = $this->diff($dbname, $file);
			if($result['errno']!=0) return $result;
			$results['files'][basename($filename)] = $result;
		}
		return $results;
	}

	public function diff($dbname, $sqlfile){
		if(!DB::$isloggedin){
			return ['errno'=>3,'error'=>"Not connected to database."];
		}
		$flatten_array = function($carry, $item) use (&$flatten_array) {
			if(is_array($item)){
				$reduced = array_reduce($item, $flatten_array, []);
				$merged = array_merge($carry, $reduced);
				return $merged;
			} else {
				$carry[] = $item;
				return $carry;
			}
		};

		$dbname = DB::escape($dbname);
		$result = DB::sql("SHOW DATABASES LIKE '$dbname'");
		if(!$result || $result->num_rows != 1){ return ['errno' => 1, 'error' => "Database '$dbname' does not exist."]; }
		$temp_id = $this->create_temp_db();
		$result = $sqlfile->execute_tables_only();
		if(!$result){ return ['errno' => 2, 'error' => "Error executing SQL File.", 'sqlerror' => DB::get()->error]; }
		$temp_dbname = self::DB_PREFIX . $temp_id;
		$file_tables = $this->list_tables($temp_dbname);
		$db_tables = $this->list_tables($dbname);
		$intersection_columns = array();
		$intersection_keys = array();
		$intersection_options = array();
		$alter_queries = array();
		$table_options = ['engine'=>'ENGINE','table_comment'=>'COMMENT'];

		foreach(array_intersect($file_tables, $db_tables) as $table){
			$options = array();
			$result = $this->compare_options($dbname, $table, $temp_dbname, $table);
			foreach($result as $row){
				$tX = $row['table'];
				unset($row['table']);
				$options[$table][$tX] = $row;
			}
			$option_sql = '';
			foreach($options as $tname => $row){
				foreach($table_options as $key => $option){
					if($row['t1'][$key] != $row['t2'][$key]){
						$intersection_options[$table][$key]['t1'] = $row['t1'][$key];
						$intersection_options[$table][$key]['t2'] = $row['t2'][$key];
						$option_sql .= " $option='".$row['t2'][$key]."'";
					}
				}
			}
			if(!empty($option_sql)) $alter_queries[$table][] = "ALTER TABLE `$table`".$option_sql;

			$rows = array();
			$order = array();
			$result = $this->compare($dbname, $table, $temp_dbname, $table);
			foreach($result as $row){
				$tX = $row['table'];
				unset($row['table']);
				$colname = $row['column_name'];
				unset($row['column_name']);
				$order[$tX][$row['ordinal_position']] = $colname;
				unset($row['ordinal_position']);
				$rows[$colname][$tX] = $row;
			}
			$offset = 0;
			$skipped = [];
			$defs = $sqlfile->get_col_defs($table)['cols'];
			foreach($order['t1'] as $colname){
				if(!in_array($colname, $order['t2'])){
					$skipped[$colname] = true;
					$intersection_columns[$table][$colname] = $rows[$colname];
					$alter_queries[$table][] = "ALTER TABLE `$table` DROP COLUMN `$colname`";
				}
			}
			foreach($order['t2'] as $pos => $colname){
				while(isset($order['t1'][$pos+$offset]) && isset($skipped[$order['t1'][$pos+$offset]])){
					unset($skipped[$order['t1'][$pos+$offset]]);
					$offset++;
				}
				if(!isset($order['t1'][$pos+$offset]) || $order['t2'][$pos] != $order['t1'][$pos+$offset]){
					$p = array_search($colname, $order['t1']);
					if($p !== false){
						$rows[$colname]['t1']['after'] = $order['t1'][$p-1].' ';
					} 
					$rows[$colname]['t2']['after'] = $pos==1 ? '#FIRST' : $order['t2'][$pos-1];
					$offset--;
					$skipped[$colname] = true;
				}
				if(isset($rows[$colname]['t1'])){
					$diff = $rows[$colname];
					$diff1 = array_diff($diff['t1'], $diff['t2']);
					$diff['t2'] = array_diff($diff['t2'], $diff['t1']);
					$diff['t1'] = $diff1;
					if(!empty($diff['t1']) || !empty($diff['t2'])){
						$rows[$colname] = $diff;
						$intersection_columns[$table][$colname] = $diff;
						$alter_queries[$table][] = "ALTER TABLE `$table` MODIFY COLUMN ".$defs[$colname].$this->build_column_query_after($diff['t2']);
					} else {
						unset($rows[$colname]);
					}
				} else {
					$intersection_columns[$table][$colname] = $rows[$colname];
					$alter_queries[$table][] = "ALTER TABLE `$table` ADD COLUMN ".$defs[$colname].$this->build_column_query_after($rows[$colname]['t2']);
				}
			}

			$result = $this->compare_keys($dbname, $table, $temp_dbname, $table);
			$keys = array();
			foreach($result as $row){
				$tX = $row['table'];
				$index = $row['index_name'];
				$col = $row['column_name'];
				$non_unique = $row['non_unique'];
				$seq = $row['seq_in_index'];
				if(!isset($keys[$index])){
					$keys[$index] = [];
				}
				if(!isset($keys[$index][$tX])){
					$keys[$index][$tX] = ['cols' => [], 'non_unique' => $non_unique];
				}
				$keys[$index][$tX]['cols'][$seq] = $col;
			}
			$rows = [];
			foreach($keys as $name => $key){
				foreach($key as $tX => $data){
					if(!isset($rows[$name])){
						$rows[$name] = [$tX => $data];
					} else {
						$tY = $tX == 't1' ? 't2' : 't1';
						$flatY = $flatten_array([$table, $name], $rows[$name][$tY]);
						$flatX = $flatten_array([$table, $name], $data);
						$diffX = array_diff($flatX, $flatY);
						$diffY = array_diff($flatY, $flatX);
						if(!empty($diffX) || !empty($diffY)){
							$rows[$name][$tX] = $data;
						} else {
							unset($rows[$name]);
						}
					}
				}
				if(isset($rows[$name])){
					$alter_queries[$table][] = $this->build_alter_key_query($table, $name, $rows[$name]);
				}
			}
			$intersection_keys[$table] = $rows;
		}
		$db_only_tables = array_diff($db_tables, $file_tables);
		$file_only_tables = array_diff($file_tables, $db_tables);
		return array(
			'errno' => 0,
			'tables_in_database_only' => $db_only_tables,
			'tables_in_file_only' => $file_only_tables,
			'intersection_columns' => $intersection_columns,
			'intersection_keys' => $intersection_keys,
			'intersection_options' => $intersection_options,
			'drop_queries' => $this->build_drop_query($db_only_tables, $dbname),
			'create_queries' => $this->build_create_query($file_only_tables, $temp_dbname),
			'alter_queries' => $alter_queries
		);
	}

	private function build_drop_query($tables, $dbname){
		$stmt_list = [];
		foreach($tables as $table){
			$stmt_list[$table] = "DROP TABLE `$dbname`.`$table`;";
		}
		return $stmt_list;
	}

	private function build_create_query($tables, $dbname){
		$stmt_list = [];
		foreach($tables as $table){
			$result = DB::sql("SHOW CREATE TABLE `$dbname`.`$table`");
			if(!$result){
				return false;
			}
			foreach($result as $row){
				$stmt_list[$row['Table']] = $row['Create Table'].';';
			}
		}
		return $stmt_list;
	}

	private function build_column_query_after($row){
		if(isset($row['after'])){
			if($row['after'] == '#FIRST'){
				return ' FIRST';
			} else {
				return ' AFTER `'.$row['after'].'`';
			}
		}
		return '';
	}

	private function build_alter_key_query($tablename, $keyname, $diff){
		$name_to_desc = function($name, $row, $allow_unique = true){
			if($name == 'PRIMARY'){
				return 'PRIMARY KEY';
			} else {
				$desc = "KEY `$name`";
				if(isset($row['non_unique']) && !$row['non_unique'] && $allow_unique) {
					$desc = 'UNIQUE '.$desc;
				}
				return $desc;
			}
		};

		if(isset($diff['t1'])){
			return "ALTER TABLE `$tablename` DROP ".$name_to_desc($keyname, $diff['t1'], false);
		}
		if(isset($diff['t2'])){
			$query = "ALTER TABLE `$tablename` ADD ".$name_to_desc($keyname, $diff['t2']);
			if(isset($diff['t2']['type'])){
				$query .= ' '.$diff['t2']['type'];
			}
			if(isset($diff['t2']['cols'])){
				$query .= ' (`'.implode('`,`', $diff['t2']['cols']).'`)';
			}
			if(isset($diff['t2']['options'])){
				foreach($diff['t2']['options'] as $option){
					$query .= " $option";
				}
			}
			return $query;
		}
	}

	private function compare($db1, $t1, $db2, $t2){
		$stmt = $this->compare_tables_stmt;
		$stmt->bind_param('ssss', $db1, $t1, $db2, $t2);
		$stmt->execute();
		return $stmt->get_result();
	}

	private function compare_keys($db1, $t1, $db2, $t2){
		$stmt = $this->compare_keys_stmt;
		$stmt->bind_param('ssss', $db1, $t1, $db2, $t2);
		$stmt->execute();
		return $stmt->get_result();
	}

	private function compare_options($db1, $t1, $db2, $t2){
		$stmt = $this->compare_options_stmt;
		$stmt->bind_param('ssss', $db1, $t1, $db2, $t2);
		$stmt->execute();
		return $stmt->get_result();
	}

	private function list_tables($db){
		$result = DB::sql("SHOW TABLES IN `$db`");
		$list = array();
		while($row = $result->fetch_row()){
			$list[] = $row[0];
		}
		return $list;
	}

	private function create_temp_db($id = 0, $mysqli = null){
		if(!isset($mysqli)){
			$mysqli = DB::get();
		}
		if(!is_int($id) || $id <= 0){
			$id = rand();
		}
		$dbname = self::DB_PREFIX . $id;
		$result = $mysqli->query("CREATE DATABASE $dbname");
		if($mysqli->errno == 1007){
			return $this->create_temporary_db($id + 1, $mysqli);
		}
		$mysqli->query("USE `$dbname`");
		register_shutdown_function(function() use ($mysqli, $dbname){
			$mysqli->query("DROP DATABASE IF EXISTS $dbname");
		});
		return $id;
	}
}
?>