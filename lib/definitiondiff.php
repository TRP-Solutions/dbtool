<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/
class Definitiondiff {
	static private $known_tables;

	public static function reset(){
		self::$known_tables = [];
	}

	private $dbname, $name, $file_stmt, $db_stmt, $filenames = [], $errors = [], $diff, $diff_sql, $diff_calculated = true, $parsed_database = false;

	public function __construct($name){
		$names = explode('.',$name);
		if(count($names)==2){
			$this->dbname = $names[0];
			$this->name = $names[1];
		} else {
			$this->name = $name;
		}
	}

	public function from_file($stmt, $filename){
		if(!isset($this->file_stmt)){
			$this->file_stmt = $stmt;
			if(isset($stmt['error'])){
				$this->errors[] = ['errno'=>1,'error'=>"Parse Error in file \"$filename\" table `$stmt[name]`: $stmt[error]"];
			}
			$this->diff_calculated = false;
			$this->filenames[] = $filename;
		} else {
			$diff = self::compare_tables($this->file_stmt, $stmt);
			if($diff['is_empty']){
				$this->filenames[] = $filename;
			} else {
				$msg = "Collision Error in file \"$filename\" table `$stmt[name]`:\nTable differs from a table with the same name in \"$this->filenames[0]\"";
				$this->errors[] = ['errno'=>3,'error'=>$msg];
			}
		}
	}

	public function from_database($stmt){
		$this->db_stmt = $stmt;
		if(isset($stmt['error'])){
			$this->errors[] = ['errno'=>2,'error'=>"Parse Error in database table `$stmt[name]`: $stmt[error]"];
		}
		$this->diff_calculated = false;
		$this->parsed_database = true;
	}

	private static function get_known_tables(){
		$db = Config::get('database');
		if(!isset(self::$known_tables[$db])){
			$query = DB::sql("SELECT DATABASE()");
			$active_database = $query->num_rows ? $query->fetch_array()[0] : null;
			if($active_database == $db){
				$query = DB::sql("SHOW TABLES");
				self::$known_tables[$db] = array_map(function($row){return $row[0];}, $query->fetch_all());
			} else {
				self::$known_tables[$db] = [];
			}
		}
		return self::$known_tables[$db];
	}

	private function get_db_stmt(){
		if(!$this->parsed_database && isset($this->name)){
			if(in_array($this->name, self::get_known_tables())){
				$query = DB::sql("SHOW CREATE TABLE `$this->name`");
				if($query && $query->num_rows){
					$stmt = \Parser\statement($query->fetch_assoc()['Create Table']);
					if(isset($stmt['error'])){
						$this->errors[] = ['errno'=>2,'error'=>"Parse Error in database table `$stmt[name]`: $stmt[error]"];
					}
					$this->db_stmt = $stmt;
				}
			}

			$this->parsed_database = true;
		}
		return $this->db_stmt;
	}

	public function get_create(){
		$db_stmt = $this->get_db_stmt();
		if(isset($this->file_stmt) && !isset($db_stmt) && !isset($this->file_stmt['error'])){
			return [$this->file_stmt,$this->file_stmt['statement'].';'];
		}
		return [null,null];
	}

	public function get_alter(){
		$db_stmt = $this->get_db_stmt();
		if(isset($this->file_stmt) && isset($db_stmt) && !isset($this->file_stmt['error']) && !isset($db_stmt['error'])){
			return $this->diff();
		}
		return null;
	}

	public function get_drop(){
		$db_stmt = $this->get_db_stmt();
		if(!isset($this->file_stmt) && isset($db_stmt) && !isset($db_stmt['error'])){
			$sql = "DROP TABLE `$this->dbname`.`$this->name`;";
			return [$db_stmt['name'],$sql];
		}
		return [null,null];
	}

	public function get_errors(){
		return $this->errors;
	}

	private function diff(){
		if(!$this->diff_calculated){
			$db_stmt = $this->get_db_stmt();
			if(isset($db_stmt) && isset($this->file_stmt)){
				$this->diff = self::compare_tables($this->file_stmt, $db_stmt);
				if(isset($this->diff)){
					$this->diff['sql'] = self::generate_alter_queries($this->name, $this->diff);
				}
			}
			$this->diff_calculated = true;
		}
		return $this->diff;
	}

	private static function compare_tables($file_table, $db_table){
		$db_key = 't1';
		$file_key = 't2';
		$file_columns = [];
		$file_indexes = [];
		$db_columns = [];
		$db_indexes = [];

		foreach($file_table['columns'] as $col){
			self::organize_column($col, $file_columns, $file_indexes);
		}

		foreach($db_table['columns'] as $col){
			self::organize_column($col, $db_columns, $db_indexes);
		}

		$file_column_order = array_keys($file_columns);
		$db_column_order = array_keys($db_columns);
		if($file_column_order != $db_column_order){
			$stable_columns = self::longest_common_subsequence($file_column_order, $db_column_order);
			foreach($stable_columns as $colname){
				$file_columns[$colname]['after'] = null;
				$db_columns[$colname]['after'] = null;
			}
			$moving_columns = array_diff($file_column_order, $stable_columns);
			foreach($moving_columns as $colname){
				if(isset($file_columns[$colname])
					&& isset($db_columns[$colname])
					&& $file_columns[$colname]['after'] == $db_columns[$colname]['after']
				){
					$db_columns[$colname]['after'] .= ' (moved)';
				}
			}
		}

		$columns = self::compare_elems($file_columns, $db_columns, $file_key, $db_key, ['DefinitionDiff','column_is_equal']);
		$keys = self::compare_elems($file_indexes, $db_indexes, $file_key, $db_key);

		$ignore_db_only_options = ['AUTO_INCREMENT'];
		$db_options = array_filter($db_table['table_options'], function($key) use ($ignore_db_only_options){
			return !in_array($key, $ignore_db_only_options);
		}, ARRAY_FILTER_USE_KEY);
		$options = self::compare_elems($file_table['table_options'], $db_options, $file_key, $db_key, ['DefinitionDiff','option_is_equal']);

		if(!empty($columns) || !empty($keys) || !empty($options)){
			return ['columns'=>$columns,'keys'=>$keys,'options'=>$options];
		}
	}

	private static function compare_elems($file_inputs, $db_inputs, $file_key, $db_key, $is_equal = null){
		$output = [];
		$names = array_unique(array_merge(array_keys($file_inputs),array_keys($db_inputs)));
		foreach($names as $name){
			$file_elem = isset($file_inputs[$name]) ? $file_inputs[$name] : null;
			$db_elem = isset($db_inputs[$name]) ? $db_inputs[$name] : null;
			$equality = isset($is_equal) ? $is_equal($file_elem, $db_elem, $name) : $file_elem == $db_elem;
			if(!$equality){
				$output[$name] = array_filter([$db_key=>$db_elem, $file_key=>$file_elem]);
			}
		}
		return $output;
	}

	private static function organize_column($col, &$columns, &$keys){
		if($col['type'] == 'column'){
			$columns[$col['name']] = Format::column_description_to_A($col); //compatibility with web diffview
		} elseif($col['type'] == 'index'){
			unset($col['type']); //compatibility with web diffview
			$col['cols'] = array_map('\Parser\encode_index_column', $col['index_columns']);
			$name = $col['index_type'] == 'primary' ? 'PRIMARY' : (isset($col['name']) ? $col['name'] : $col['cols'][0]);
			if(isset($keys[$name])){
				$i = 1;
				$basename = $name;
				while(isset($keys[$name])){
					$name = "{$basename}_$i";
					$i++;
				}
			}
			if(!isset($col['name']) && $name != 'PRIMARY') $col['name'] = $name;
			$keys[$name] = $col;
		}
	}

	private static function column_is_equal($col_a, $col_b){
		if(!isset($col_a) || !isset($col_b)) return false;
		$keys = array_unique(array_merge(array_keys($col_a), array_keys($col_b)));
		foreach($keys as $key){
			if($key == 'type'){
				// 'type' field is kept around to generate the SQL statements
				continue;
			}
			if(!array_key_exists($key, $col_a) || !array_key_exists($key, $col_b)){
				if($key == 'length'
					||$key == 'collation' && self::is_collation_default($col_a,$col_b)){
					// if one side has length and other size doesn't, assume it's default length
					continue;
				}
				return false;
			}
			if($col_a[$key] != $col_b[$key]) {
				if(
					$key == 'default' && self::is_synonym($col_a[$key],$col_b[$key],self::$default_synonyms)
					|| $key == 'data_type' && self::compare_type($col_a[$key],$col_b[$key])
					|| $key == 'char_set' && self::is_synonym($col_a[$key],$col_b[$key],self::$charset_synonyms)
					|| $key == 'collation' && self::compare_collation($col_a[$key],$col_b[$key])
				){
					continue;
				}
				return false;
			}
		}
		return true;
	}

	private static function option_is_equal($opt_a, $opt_b, $name){
		if(!isset($opt_a) || !isset($opt_b)) return false;
		if($opt_a === $opt_b) return true;
		if($name === 'CHARSET'){
			return self::is_synonym($opt_a, $opt_b, self::$charset_synonyms);
		}
		return false;
	}

	private static $charset_synonyms = [
		['utf8','utf8mb3']
	];
	private static $default_synonyms = [
		['current_timestamp','current_timestamp()','now()']
	];
	private static function is_synonym($a, $b, $synonym_lists){
		$a = mb_strtolower($a);
		$b = mb_strtolower($b);
		$a_match = false;
		$b_match = false;
		foreach($synonym_lists as $synonym_list){
			foreach($synonym_list as $term){
				if($a == $term){
					$a_match = true;
					if($a_match && $b_match){
						return true;
					}
				}
				if($b == $term){
					$b_match = true;
					if($a_match && $b_match){
						return true;
					}
				}
			}
			// if either term matches at this point, then the terms are not in the same synonym list
			// this optimization assumes the synonym lists aren't malformed by having the a term in multiple lists
			if($a_match || $b_match) return false;
		}
		// neither term was found in the synonym lists
		return false;
	}
	private static function compare_type($a, $b){
		$pattern = "/\([0-9 ]+\)/";
		$a_replace = preg_replace($pattern, '', $a);
		if($a_replace == $b) return true;
		$b_replace = preg_replace($pattern, '', $b);
		return $b_replace == $a;
	}
	private static function compare_collation($a, $b){
		$a = explode('_',$a,2);
		$b = explode('_',$b,2);
		if(!isset($a[1])) $a[1] = null;
		if(!isset($b[1])) $b[1] = null;
		return $a[1] == $b[1] && self::is_synonym($a[0], $b[0], self::$charset_synonyms);
	}
	private static function is_collation_default($first, $second){
		if(empty($first['collation'])
			&& !empty($second['collation'])
			&& !empty($first['char_set'])
		){
			$charset = $first['char_set'];
			$collation = $second['collation'];
		}
		elseif(empty($second['collation'])
			&& !empty($first['collation'])
			&& !empty($second['char_set'])
		){
			$charset = $second['char_set'];
			$collation = $first['collation'];
		} else {
			return false;
		}

		$query = \DB::sql("SHOW CHARACTER SET WHERE Charset = '$charset'");
		return $query->num_rows == 1 && $query->fetch_assoc()['Default collation'] == $collation;
	}

	private static function generate_alter_queries($table_name, $table_diff){
		$drop_keys = [];
		$add_columns = [];
		$modify_columns = [];
		$drop_columns = [];
		$add_keys = [];
		$alter_options = [];

		foreach($table_diff['columns'] as $colname => $diff){
			if(isset($diff['t1']) && isset($diff['t2'])){
				// modify
				$query = "ALTER TABLE `$table_name` MODIFY COLUMN ".Format::column_A_to_definition($diff['t2']);
				if(isset($diff['t1']['after'])
					&& $diff['t1']['after'] != $diff['t2']['after']){
					$query .= self::build_column_query_after($diff['t2']);
				}
				$modify_columns[] = $query.';';
			} elseif(isset($diff['t2'])){
				// add
				$add_columns[] = "ALTER TABLE `$table_name` ADD COLUMN ".Format::column_A_to_definition($diff['t2']).self::build_column_query_after($diff['t2']).';';
			} elseif(isset($diff['t1'])){
				// drop
				$drop_columns[] = "ALTER TABLE `$table_name` DROP COLUMN `$colname`;";
			}
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
				if(isset($diff['t2']['name'])) $query .= '`'.$diff['t2']['name'].'` ';
				$query .= '(`'.implode('`,`',$diff['t2']['cols']).'`);';
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

		return array_merge($drop_keys,$add_columns,$modify_columns,$drop_columns,$add_keys,$alter_options);
	}

	private static function build_column_query_after($row){
		if(isset($row['after'])){
			if($row['after'] == '#FIRST'){
				return ' FIRST';
			} else {
				return ' AFTER `'.$row['after'].'`';
			}
		}
		return '';
	}

	private static $lcs_cache;
	private static function longest_common_subsequence($x, $y){
		// reset cache to avoid unnecessarily large memory use
		self::$lcs_cache = [];
		return self::_longest_common_subsequence($x,$y);
	}

	private static function _longest_common_subsequence($x, $y){
		/*
		inspired by
		https://en.wikipedia.org/wiki/Longest_common_subsequence_problem#Solution_for_two_sequences
		fetched 2021-05-28
		*/
		$cache_index = implode(';',$x).'|'.implode(';',$y);

		if(!isset($x[0]) || !isset($y[0])){
			// if either array is empty, the LCS is empty
			return [];
		}
		if(isset(self::$lcs_cache[$cache_index])){
			// if we have calculated the LCS before, fetch it from the cache
			return self::$lcs_cache[$cache_index];
		}
		if($x[0] == $y[0]){
			// if the first elements of the two arrays match,
			// the LCS is that first element followed by the LCS of the two arrays with that element removed
			$result = self::_longest_common_subsequence(
				array_slice($x, 1),
				array_slice($y, 1)
			);
			array_unshift($result, $x[0]);
			// store calculated result in the cache
			self::$lcs_cache[$cache_index] = $result;
			return $result;
		} else {
			// if the first elements of the two arrays DON'T match,
			// then the LCS is equal to the one of the two LCSs
			// where the first element is removed from either array
			$a = self::_longest_common_subsequence(array_slice($x, 1),$y);
			$b = self::_longest_common_subsequence($x,array_slice($y, 1));

			// pick the longest LCS of the two and cache the result
			// if their equally long arbitrarily pick one
			// here we pick $a (LCS where one element is removed from $x)
			if(count($a) >= count($b)){
				self::$lcs_cache[$cache_index] = $a;
				return $a;
			} else {
				self::$lcs_cache[$cache_index] = $b;
				return $b;
			}
		}
	}
}
