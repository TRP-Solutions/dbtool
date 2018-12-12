<?php
require_once __DIR__.'/db.php';
require_once __DIR__.'/sqlfile.php';
require_once __DIR__.'/format.php';

class Diff {
	private $files, $dbname, $dbtables;
	public function __construct($files){
		$this->files = $files;
	}

	public function run(){
		$tables = [];
		$errors = [];

		$db_tables = $this->get_db_tables();
		foreach($this->files as $file){
			$filename = $file->get_filename();
			$stmts = $file->get_create_table_stmts();
			if(is_array($stmts)) foreach($stmts as $stmt){
				$file_table = SQLFile::parse_statement($stmt);
				$name = $file_table['name'];

				if(isset($file_table['error'])){
					$errors[] = ['errno'=>1,'error'=>"Parse Error in file \"$filename\" table `$name`: $file_table[error]"];
				} elseif(isset($db_tables[$name])){
					unset($db_tables[$name]);
					$query = DB::sql("SHOW CREATE TABLE `$name`");
					if($query->num_rows){
						$db_table = SQLFile::parse_statement($query->fetch_assoc()['Create Table']);
						if(isset($db_table['error'])){
							$errors[] = ['errno'=>2,'error'=>"Parse Error in database table `$db_table[name]`: $db_table[error]"];
							continue;
						}
						$diff = $this->compare_tables($file_table, $db_table);
						$diff['name'] = $name;
						$diff['sourcefiles'] = [$filename];
						$diff['sql'] = $this->generate_alter_queries($name, $diff);
						$diff['type'] = 'intersection';
						$tables[] = $diff;
					}
				} elseif(isset($file_only_tables[$name])){
					$other_file = isset($file_only_tables[$name]['sourcefile']) ? $file_only_tables[$name]['sourcefile'] : '[unknown file]';
					$errors[] = ['errno'=>3,'error'=>"Collision Error: table `$name` exists in file \"$filename\" and in file \"$other_file\""];
				} elseif(isset($intersection_tables[$name])){
					$other_file = isset($intersection_tables[$name]['sourcefile']) ? $intersection_tables[$name]['sourcefile'] : '[unknown file]';
					$errors[] = ['errno'=>3,'error'=>"Collision Error: table `$name` exists in file \"$filename\" and in file \"$other_file\""];
				} else {
					$file_table['sourcefile'] = $filename;
					$tables[] = [
						'name'=>$file_table['name'],
						'type'=>'file_only',
						'sourcefiles'=>[$filename],
						'sql'=>[$file_table['statement'].';']
					];
				}
			}
		}

		$db_only_tables = isset($db_tables) ? array_keys($db_tables) : [];
		return [
			'errors'=> $errors,
			'tables'=> $tables,
			'db_only_tables'=> $db_only_tables,
			'drop_queries'=> $this->build_drop_query($db_only_tables, $this->dbname)
		];
	}

	private function get_db_tables(){
		if(!isset($this->dbtables)){
			$this->dbname = DB::escape(Config::get('database'));
			if(empty($this->dbname)) return null;
			DB::sql("USE $this->dbname;");
			$query = DB::sql("SHOW TABLES IN `$this->dbname`");
			$tables = array();
			while($row = $query->fetch_row()){
				$tables[] = $row[0];
			}
			$this->dbtables = array_combine($tables,$tables);
		}
		return $this->dbtables;
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
						$name = "$basename_$i";
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

	private function generate_alter_queries($table_name, $table_diff){
		$drop_keys = [];
		$alter_columns = [];
		$add_keys = [];
		$alter_options = [];

		foreach($table_diff['columns'] as $colname => $diff){
			if(isset($diff['t1']) && isset($diff['t2'])){
				// modify
				$query = "ALTER TABLE `$table_name` MODIFY COLUMN ".Format::column_A_to_definition($diff['t2']);
				if(isset($diff['t1']['after']) && isset($diff['t1']['ordinal_number'])
					&& $diff['t1']['after'] != $diff['t2']['after']
					&& $diff['t1']['ordinal_number'] >= $diff['t2']['ordinal_number']){
					$query .= $this->build_column_query_after($diff['t2']);
				}
			} elseif(isset($diff['t2'])){
				// add
				$query = "ALTER TABLE `$table_name` ADD COLUMN ".Format::column_A_to_definition($diff['t2']).$this->build_column_query_after($diff['t2']);
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

	private function build_drop_query($tables, $dbname){
		$stmt_list = [];
		if(isset($dbname)) foreach($tables as $table){
			$stmt_list[$table] = "DROP TABLE `$dbname`.`$table`;";
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
}
?>