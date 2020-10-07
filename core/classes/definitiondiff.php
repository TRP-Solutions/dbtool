<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/
class Definitiondiff {
	static private $known_tables;

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
				$this->errors[] = ['errno'=>1,'error'=>"Parse Error in file \"$filename\" table `$name`: $stmt[error]"];
			}
			$this->diff_calculated = false;
			$this->filenames[] = $filename;
		} else {
			$diff = self::compare_tables($this->file_stmt, $stmt);
			if($diff['is_empty']){
				$this->filenames[] = $filename;
			} else {
				$msg = "Collision Error in file \"$filename\" table `$name`:\nTable differs from a table with the same name in \"$this->filenames[0]\"";
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
			$query = DB::sql("SHOW TABLES");
			self::$known_tables[$db] = array_map(function($row){return $row[0];}, $query->fetch_all());
		}
		return self::$known_tables[$db];
	}

	private function get_db_stmt(){
		if(!$this->parsed_database && isset($this->name)){
			if(in_array($this->name, self::get_known_tables())){
				$query = DB::sql("SHOW CREATE TABLE `$this->name`");
				if($query && $query->num_rows){
					$stmt = SQLFile::parse_statement($query->fetch_assoc()['Create Table']);
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
				$this->diff['sql'] = self::generate_alter_queries($this->name, $this->diff);
			}
			$this->diff_calculated = true;
		}
		return $this->diff;
	}

	private static function compare_tables($file_table, $db_table){
		$db_key = 't1';
		$file_key = 't2';
		$file_columns = [];
		$file_keys = [];

		// FIXME : avoid duplicate "AFTER" moves

		$file_ordinals = [];

		foreach($file_table['columns'] as $col){
			if($col['type'] == 'column'){
				$col = Format::column_description_to_A($col); //compatibility with web diffview
				$file_ordinals[$col['colname']] = $col['ordinal_number'];
				unset($col['ordinal_number']);
				$file_columns[$col['colname']] = $col;
			} elseif($col['type'] == 'index'){
				unset($col['type']); //compatibility with web diffview
				$col['cols'] = array_map(['SQLFile','encode_index_column'], $col['index_columns']);
				$name = $col['index_type'] == 'primary' ? 'PRIMARY' : (isset($col['name']) ? $col['name'] : $col['index_columns'][0]['name']);
				if(isset($file_keys[$name])){
					$i = 1;
					$basename = $name;
					while(isset($file_keys[$name])){
						$name = "{$basename}_$i";
						$i++;
					}
				}
				if(!isset($col['name']) && $name != 'PRIMARY') $col['name'] = $name;
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
				$col = Format::column_description_to_A($col); //compatibility with web diffview
				$name = $col['colname'];
				if(isset($file_columns[$name]) && ($file_to_db_order_offset || $col['after'] != $file_columns[$name]['after'])){
					$search = array_search($col['ordinal_number'],$file_ordinals);
					if(in_array($search,$out_of_order)) $file_to_db_order_offset -= 1;
				}
				if(!isset($file_ordinals[$name]) || $file_ordinals[$name] + $file_to_db_order_offset != $col['ordinal_number']){
					if(isset($file_ordinals[$name]) && $file_ordinals[$name] > $col['ordinal_number']){
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
						//unset($col['after']);
						//unset($file_columns[$name]['after']);
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
		return ['columns'=>$columns,'keys'=>$keys,'options'=>$options,'is_empty'=>empty($columns)&&empty($keys)&&empty($options)];
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
}
