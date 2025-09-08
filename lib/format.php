<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/

declare(strict_types=1);
require_once __DIR__.'/sqltype.php';
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
		$card_userdrop = null;
		foreach($data as $entry){
			if($entry['type'] == 'error'){
				if(isset($entry['error']['error'])){
					$error_cards[] = [
						'errors'=>[$entry['error']['error']]
					];
				} else {
					$error_cards[] = [
						'errors'=>array_map(function($o){return $o['error'];}, $entry['error']),
					];
				}
			} elseif($entry['type'] == 'create_user'){
				$cards[] = [
					'title'=>'Missing user: '.$entry['name'],
					'sql'=>$entry['sql'],
					'id'=>'create_user:'.$entry['name']
				];
			} elseif($entry['type'] == 'create_database'){
				$cards[] = [
					'title'=>'Missing database',
					'sql'=>$entry['sql'],
					'id'=>'sql:create_database'
				];
			} elseif($entry['type'] == 'database_only' || $entry['type'] == 'intersection' || $entry['type'] == 'file_only'){
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
			} elseif($entry['type'] == 'alter_user'){
				$cards[] = [
					'title'=>$entry['name'],
					'sql'=>$entry['sql'],
					'id'=>'alter_user:'.$entry['name'],
					'display'=>[['title'=>'Options','table'=>self::table_columns_to_display($entry['options'])]]
				];
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
				$card_tabledrop['sql'] = array_merge($card_tabledrop['sql'],$entry['sql']);
			} elseif($entry['type'] == 'drop_user'){
				if(!isset($card_userdrop)){
					$card_userdrop = [
						'title'=>'Users only in database',
						'display'=>[['list'=>[]]],
						'sql'=>[],
						'id'=>'drop_user:'.$entry['name']
					];
				}
				$card_userdrop['display'][0]['list'][] = $entry['name'];
				$card_userdrop['sql'][] = $entry['sql'];
			}
		}
		if(isset($card_tabledrop)){
			$cards[] = $card_tabledrop;
		}
		if(isset($card_userdrop)){
			$cards[] = $card_userdrop;
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
					if(!empty($value) || $value === '0'){
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
					'non_unique'=> isset($row['non_unique']) ? $row['non_unique'] : '',
					'using' => $row['index_using'] ?? '',
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
			'data_type'=>new DataTypeNameProxy($old_col['datatype'])
		];
		$new_col['nullable'] = match($old_col['nullity']){
			'NULL' => 'YES',
			'NOT NULL' => 'NO',
			default => $old_col['datatype']->is_nullable_by_default() ? 'YES' : 'NO'
		};
		if(isset($old_col['default'])) $new_col['default'] = $old_col['default'];
		elseif($new_col['nullable']=='YES') $new_col['default'] = 'NULL';
		if(isset($old_col['on_update'])) $new_col['on_update'] = $old_col['on_update'];
		if(isset($old_col['datatype']['length'])) $new_col['length'] = $old_col['datatype']['length'];
		if(isset($old_col['datatype']['char_max_length'])) $new_col['char_max_length'] = $old_col['datatype']['char_max_length'];
		if(isset($old_col['datatype']['precision'])) $new_col['num_precision'] = $old_col['datatype']['precision'];
		if(isset($old_col['datatype']['decimals'])) $new_col['num_scale'] = $old_col['datatype']['decimals'];
		if(isset($old_col['datatype']['fsp'])) $new_col['fractional_seconds_precision'] = $old_col['datatype']['fsp'];
		if(isset($old_col['datatype']['character set'])) $new_col['char_set'] = $old_col['datatype']['character set'];
		if(isset($old_col['datatype']['collate'])) $new_col['collation'] = $old_col['datatype']['collate'];
		if(isset($old_col['datatype']['unsigned'])) $new_col['unsigned'] = $old_col['datatype']['unsigned'] ? 'YES' : 'NO';
		if(isset($old_col['datatype']['zerofill'])) $new_col['zerofill'] = $old_col['datatype']['zerofill'] ? 'YES' : 'NO';
		if(isset($old_col['datatype']['values'])) $new_col['enum_values'] = implode(', ',$old_col['datatype']['values']);

		if(is_a($old_col['datatype'], '\Datatype')){
			$new_col['type'] = $old_col['datatype']->string_with_attribute();
		} else {
			$new_col['type'] = \Parser\encode_datatype($old_col['datatype'], true);
		}
		

		if(isset($old_col['auto_increment']) && $old_col['auto_increment']) $new_col['extra'] = 'auto_increment';
		if(isset($old_col['key'])) $new_col['key'] = $old_col['key'];
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
		if(isset($col['on_update'])) $def .= " ON UPDATE $col[on_update]";
		if(isset($col['extra']) && $col['extra'] == 'auto_increment') $def .= ' AUTO_INCREMENT';
		if(isset($col['key'])) $def .= " $col[key]";
		if(isset($col['comment'])) $def .= " COMMENT '$col[comment]'";
		return $def;
	}

	private static function diff_class($diff){
		return isset($diff['t1']) ? (isset($diff['t2']) ? 'bg-info' : 'bg-danger') : 'bg-success';
	}
}
