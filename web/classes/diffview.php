<?php
class DiffView {
	public static function build($parent, $result) {
		$data = self::prepare_data($result);

		$table_count = count($data['tables']);
		$db_count = count($result['tables_in_database_only']);
		$file_count = count($result['tables_in_file_only']);

		if($table_count || $db_count || $file_count) {
			
		} else {
			$parent->el('div')->te('No difference found.');
		}

		if($table_count > 0){
			$ul = HTML::head($parent, 'ul', 'li', "Tables in intersection");
			foreach($data['tables'] as $tablename){
				$headtext = "Table: `$tablename`";
				$li = HTML::tab_head($ul, 'li', "t_$tablename", 'div', $headtext, 'SQL');
				$datatab = HTML::tab($li, 'div', 'Data', true);
				if(!empty($data['opt'][$tablename])){
					$table = $datatab->el('table');
					$tr = $table->el('tr');
					$th = $tr->el('th');
					$th->te('Option mismatch');
					$th = $tr->el('th');
					$th->te('Database');
					$th = $tr->el('th');
					$th->te('Schemafile');
					foreach($data['opt'][$tablename] as $type => $diff){
						$tr = $table->el('tr');
						$tr->at('class','diff');
						$tr->el('td')->te($type);
						$tr->el('td')->te($diff['t1']);
						$tr->el('td')->te($diff['t2']);
					}
				}
				if(!empty($data['cols'][$tablename])){
					$datatab->te('Columns:');
					HTML::table($datatab, $data['cols'][$tablename]);
				}
				if(!empty($data['keys'][$tablename])){
					$datatab->te('Keys:');
					HTML::table($datatab, $data['keys'][$tablename], ['location' => 'Location', 'keyname' => 'Keyname', 'cols' => 'Columns', 'non_unique' => 'Non Unique']);
				}
				HTML::itemize(HTML::tab($li, 'ul', 'SQL'), $result['alter_queries'][$tablename]);
			}
		}
		if($db_count || $file_count){
			$div = $parent->el('div');
			$div->at('class','flex row');
			if($db_count > 0){
				$headtext = "Tables only in database: {$_POST['database']}";
				$db_tables = HTML::tab_head($div, 'ul', 'db_tables', 'li', $headtext, 'SQL', 'li');
				$run = HTML::$alt_elem->el('button');
				HTML::itemize(HTML::tab($db_tables, 'div', 'Data', true), $result['tables_in_database_only']);
				HTML::itemize(HTML::tab($db_tables, 'div', 'SQL'), $result['drop_queries']);
			}
			if($file_count > 0){
				$headtext = "Tables only in schema file: {$_POST['schemafile']}";
				$file_tables = HTML::tab_head($div, 'ul', 'file_tables', 'li', $headtext, 'SQL', 'li');
				HTML::itemize(HTML::tab($file_tables, 'div', 'Data', true), $result['tables_in_file_only']);
				HTML::itemize(HTML::tab($file_tables, 'div', 'SQL'), $result['create_queries']);
			}
		}
	}

	private static function prepare_data($result){
		$key_data = [];
		$col_data = [];
		$opt_data = $result['intersection_options'];
		foreach($result['intersection_keys'] as $tablename => $keys){
			foreach($keys as $keyname => $diff){
				foreach($diff as $source => $row){
					$row['location'] = $source=='t1' ? "Database" : "Schemafile";
					$row['keyname'] = $keyname;
					$row['cols'] = implode(', ', $row['cols']);
					$key_data[$tablename][] = ['data' => $row, 'class' => self::diff_class($diff)];
				}
			}
		}

		foreach($result['intersection_columns'] as $tablename => $table){
			foreach($table as $colname => $diff){
				$col_columns[$tablename]['location'] = 'Location';
				$col_columns[$tablename]['colname'] = 'Column Name';
				foreach($diff as $source => $row){
					$rowdata = [
						'location' => $source=='t1' ? "Database" : "Schemafile",
						'colname' => $colname
					];
					foreach($row as $key => $value){
						if(isset($value) && $value !== ''){
							if(!isset($col_columns[$tablename][$key])){
								$col_columns[$tablename][$key] = ucwords(str_replace('_', ' ', $key));
							}
							$rowdata[$key] = $value;
						}
					}
					$col_data[$tablename][] = ['data' => $rowdata, 'class' => self::diff_class($diff)];
				}
			}
		}

		$tables = array_keys(array_merge($key_data, $col_data, $opt_data));

		return [
			'keys' => $key_data,
			'cols' => $col_data,
			'opt' => $opt_data,
			'tables' => $tables
		];
	}

	private static function diff_class($diff){
		return isset($diff['t1']) ? (isset($diff['t2']) ? 'diff' : 'remove') : 'add';
	}
}

?>