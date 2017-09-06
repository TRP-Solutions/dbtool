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

	public function diff($dbname, $sqlfile){
		if(!DB::$isloggedin){
			View::msg('error', 'Please log in first');
			return;
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