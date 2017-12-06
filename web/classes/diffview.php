<?php
class DiffView {
	public static function build($parent, $res) {
		$output = false;
		foreach($res['files'] as $filename => $result){
			$data = self::prepare_data($result);

			$table_count = count($data['tables']);
			$db_count = count($result['tables_in_database_only']);
			$file_count = count($result['tables_in_file_only']);

			if($table_count > 0){
				$output = true;
				$parent->el('h1')->te("Tables In Intersection");
				foreach($data['tables'] as $tablename){
					$card = $parent->el('div',['class'=>'card mb-3']);
					$card->el('h2',['class'=>'card-header'])->te($tablename);
					
					if(!empty($data['cols'][$tablename])){
						HTML::table($card, $data['cols'][$tablename], [], 'Columns');
					}
					if(!empty($data['keys'][$tablename])){
						HTML::table($card, $data['keys'][$tablename], ['location' => 'Location', 'keyname' => 'Keyname', 'cols' => 'Columns', 'non_unique' => 'Non Unique'],'Keys');
					}
					if(!empty($data['opt'][$tablename])){
						$table = $card->el('table',['class'=>'table m-0']);
						$table->el('tr',['class'=>'thead-light'])->el('th',['class'=>'h4','colspan'=>3])->te('Options');
						$tr = $table->el('tr',['class'=>'thead-light']);
						$th = $tr->el('th');
						$th->te('Option mismatch');
						$th = $tr->el('th');
						$th->te('Database');
						$th = $tr->el('th');
						$th->te('Schemafile');
						foreach($data['opt'][$tablename] as $type => $diff){
							$tr = $table->el('tr');
							$tr->at(['class'=>'diff']);
							$tr->el('td')->te($type);
							$tr->el('td')->te($diff['t1']);
							$tr->el('td')->te($diff['t2']);
						}
					}
					$pre = $card->el('pre',['class'=>'card-body text-light bg-dark']);
					foreach($result['alter_queries'][$tablename] as $query){
						$pre->te($query."\n");
					}
				}
			}
			if($db_count || $file_count){
				$output = true;
				if($db_count > 0){
					$card = $parent->el('div',['class'=>'card mb-3']);
					$headtext = "Tables only in database: $filename";
					$card->el('h2',['class'=>'card-header'])->te($headtext);
					$list = $card->el('ul',['class'=>'list-group list-group-flush']);
					foreach($result['tables_in_database_only'] as $item){
						$list->el('li',['class'=>'list-group-item'])->te($item);
					}
					$pre = $card->el('pre',['class'=>'card-body text-light bg-dark']);
					foreach($result['drop_queries'] as $query){
						$pre->te($query."\n");
					}
				}
				if($file_count > 0){
					$card = $parent->el('div',['class'=>'card mb-3']);
					$headtext = "Tables only in schema file: $filename";
					$card->el('h2',['class'=>'card-header'])->te($headtext);
					$list = $card->el('ul',['class'=>'list-group list-group-flush']);
					foreach($result['tables_in_file_only'] as $item){
						$list->el('li',['class'=>'list-group-item'])->te($item);
					}
					$pre = $card->el('pre',['class'=>'card-body text-light bg-dark']);
					foreach($result['create_queries'] as $query){
						$pre->te($query."\n");
					}
				}
			}
		}
		if(!$output) $parent->el('div',['class'=>'alert alert-success h3'])->te('No Differences');
		return $output;
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