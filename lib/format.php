<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/
class Format {
	public static function prettify_create_table($sql){
		$result = '';
		$depth = 0;
		$arr = str_split($sql);
		$len = count($arr);
		$debug = '';
		foreach($arr as $i => $c){
			if($depth == 0 && $c == '(' || $depth == 1 && $c == ','){
				$result .= $c."\n  ";
				$depth += 1;
				continue;
			} elseif($depth == 1){
				if($c == '(') $depth++;
				elseif($c == ')'){
					$depth = 0;
					$result .= "\n".substr($sql, $i);
					break;
				}
			} elseif($depth >= 2 && $c == ')'){
				$depth-=1;
			}
			$result .= $c;
		}
		return $result;
	}

	public static function diff_to_display($data){
		$error_cards = [];
		$cards = [];
		$card_tabledrop = null;
		foreach($data as $entry){
			if($entry['type'] == 'error'){
				$error_cards[] = [
					'errors'=>array_map(function($o){return $o['error'];}, $entry['error']),
				];
			} elseif($entry['type'] == 'create_database'){
				$cards[] = [
					'title'=>'Missing database',
					'sql'=>[$entry['sql']],
					'id'=>'sql:create_database'
				];
			} elseif($entry['type'] == 'database_only' || $entry['type'] == 'intersection'){
				$display = [];
				if(!empty($entry['columns'])) $display[] = ['title'=>'Columns','table'=>self::table_columns_to_display($entry['columns'])];
				if(!empty($entry['keys'])) $display[] = ['title'=>'Keys','table'=>self::table_keys_to_display($entry['keys'])];
				if(!empty($entry['options'])) $display[] = ['title'=>'Options','table'=>self::table_options_to_display($entry['options'])];
				if(!empty($entry['permissions'])) $display[] = ['title'=>'Permissions','table'=>self::table_permissions_to_display($entry['permissions'])];
				$card = [
					'title'=>$entry['name'],
					'subtitle'=>$entry['type']=='database_only' ? 'Database Only' : 'Files: "'.implode('"; "',$entry['sources']).'"',
					'sql'=>$entry['sql'],
					'id'=>'table:'.$entry['name']
				];
				if(!empty($display)){
					$card['display'] = $display;
				}
				if($entry['type']!='intersection' || !empty($display)){
					$cards[] = $card;
				}
			} elseif($entry['type'] == 'drop'){
				if(!isset($card_tabledrop)){
					$card_tabledrop = [
						'title'=>'Tables only in database',
						'display'=>[['list'=>[]]],
						'sql'=>[],
						'id'=>'sql:drop'
					];
				}
				$card_tabledrop['display'][0]['list'][] = $entry['name'];
				$card_tabledrop['sql'][] = $entry['sql'];
			}
		}
		if(isset($card_tabledrop)){
			$cards[] = $card_tabledrop;
		}
		return array_merge($error_cards,$cards);
	}

	public static function table_columns_to_display($data){
		$output = [];
		foreach($data as $colname => $diff){
			foreach($diff as $source => $row){
				$rowdata = [
					'location' => $source=='t1' ? "Database" : "Schemafile"
				];
				foreach($row as $key => $value){
					if($key == 'colname') $key = 'column_name';
					if(!empty($value)){
						$rowdata[$key] = $value;
					}
				}
				$output[] = ['data' => $rowdata, 'class' => self::diff_class($diff)];
			}
		}
		return $output;
	}

	public static function table_keys_to_display($data){
		$output = [];
		foreach($data as $keyname => $diff){
			foreach($diff as $source => $row){
				$output[] = ['data' => [
					'location'=> $source=='t1' ? "Database" : "Schemafile",
					'keyname'=> $keyname,
					'columns'=> implode(', ', $row['cols']),
					'non_unique'=> isset($row['non_unique']) ? $row['non_unique'] : ''
				], 'class' => self::diff_class($diff)];
			}
		}
		return $output;
	}

	public static function table_options_to_display($data){
		$output = [];
		foreach($data as $optionname => $diff){
			$output[] = ['data' =>[
				'option_mismatch'=>$optionname,
				'database'=> isset($diff['t1']) ? $diff['t1'] : '',
				'schemafile'=> isset($diff['t2']) ? $diff['t2'] : ''
			], 'class' => self::diff_class($diff)];
		}
		return $output;
	}

	public static function table_permissions_to_display($data){
		return $data;
	}

	public static function column_description_to_A($old_col){
		$new_col = [
			'colname'=>$old_col['name'],
			'nullable'=>$old_col['nullity'] == 'NOT NULL' ? 'NO' : 'YES',
			'data_type'=>$old_col['datatype']['name']
		];
		if(isset($old_col['default'])) $new_col['default'] = $old_col['default'];
		elseif($new_col['nullable']=='YES') $new_col['default'] = 'NULL';
		if(isset($old_col['datatype']['length'])) $new_col['length'] = $old_col['datatype']['length'];
		if(isset($old_col['datatype']['char_max_length'])) $new_col['char_max_length'] = $old_col['datatype']['char_max_length'];
		if(isset($old_col['datatype']['precision'])) $new_col['num_precision'] = $old_col['datatype']['precision'];
		if(isset($old_col['datatype']['decimals'])) $new_col['num_scale'] = $old_col['datatype']['decimals'];
		if(isset($old_col['datatype']['fsp'])) $new_col['fractional_seconds_precision'] = $old_col['datatype']['fsp'];
		if(isset($old_col['datatype']['character set'])) $new_col['char_set'] = $old_col['datatype']['character set'];
		if(isset($old_col['datatype']['collate'])) $new_col['collation'] = $old_col['datatype']['collate'];
		if(isset($old_col['datatype']['unsigned'])) $new_col['unsigned'] = $old_col['datatype']['unsigned'] ? 'YES' : 'NO';
		if(isset($old_col['datatype']['zerofill'])) $new_col['zerofill'] = $old_col['datatype']['zerofill'] ? 'YES' : 'NO';

		$new_col['type'] = \Parser\encode_datatype($old_col['datatype'], true);

		if(isset($old_col['auto_increment']) && $old_col['auto_increment']) $new_col['extra'] = 'auto_increment';
		if(isset($old_col['comment'])){
			$new_col['comment'] = $old_col['comment'];
			$len = strlen($new_col['comment']);
			if($new_col['comment'][0] == "'" && $new_col['comment'][$len-1] == "'"){
				$new_col['comment'] = substr($new_col['comment'], 1, -1);
			}
		}

		$new_col['after'] = $old_col['after'];

		return $new_col;
	}

	public static function column_A_to_definition($col){
		$def = "`$col[colname]` $col[type]";
		if($col['nullable'] == 'NO') $def .= ' NOT NULL';
		if(isset($col['default'])) $def .= " DEFAULT $col[default]";
		if(isset($col['extra']) && $col['extra'] == 'auto_increment') $def .= ' AUTO_INCREMENT';
		if(isset($col['comment'])) $def .= " COMMENT '$col[comment]'";
		return $def;
	}

	private static function diff_class($diff){
		return isset($diff['t1']) ? (isset($diff['t2']) ? 'bg-info' : 'bg-danger') : 'bg-success';
	}
}
