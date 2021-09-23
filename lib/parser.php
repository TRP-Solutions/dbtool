<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/

namespace Parser;
require_once __DIR__.'/description.php';

function statement($stmt){
	$stmttype = '';
	$stmt = trim($stmt);
	$types = ['CREATE TABLE' => 'table', 'INSERT' => 'insert', 'GRANT' => 'grant', 'REVOKE' => 'grant'];
	foreach($types as $prefix => $type){
		if(strpos($stmt, $prefix) === 0){
			$stmttype = $type;
			break;
		}
	}
	switch($stmttype){
		case 'table':
			return statement_table($stmt);
		case 'insert':
			return statement_insert($stmt);
		case 'grant':
			return statement_grant($stmt);
		default:
			return ['type' => 'unknown', 'statement' => $stmt];
	}
}

function encode_datatype($datatype, $include_attributes = true){
	$str = $datatype['name'];
	if(isset($datatype['length'])) $length = $datatype['length'];
	elseif(isset($datatype['precision'])) $length = $datatype['precision'];
	elseif(isset($datatype['char_max_length'])) $length = $datatype['char_max_length'];
	else $length = null;
	if(isset($length)){
		$str .= '('.$length;
		if(isset($datatype['decimals'])) $str .= ', '.$datatype['decimals'];
		$str .= ')';
	}
	if(isset($datatype['fsc'])){
		$str .= " ($datatype[fsc])";
	}
	if(isset($datatype['values'])){
		$str .= " (".implode(', ',$datatype['values']).')';
	}
	if($include_attributes){
		if(isset($datatype['unsigned']) && $datatype['unsigned']) $str .= " UNSIGNED";
		if(isset($datatype['zerofill']) && $datatype['zerofill']) $str .= " ZEROFILL";
		if(isset($datatype['character set'])) $str .= " CHARACTER SET ${datatype['character set']}";
		if(isset($datatype['collate'])) $str .= " COLLATE $datatype[collate]";
	}
	return $str;
}

function encode_index_column($col){
	$str = $col['name'];
	if(!empty($col['size'])) $str .= "($col[size])";
	if(!empty($col['sort'])) $str .= " $col[sort]";
	return $str;
}

// private; shouldn't be use outside this namespace
function statement_insert($stmt){
	return ['type' => 'insert', 'statement' => $stmt, 'key' => 'insert_'.rand()];
}

// private; shouldn't be use outside this namespace
function statement_table($stmt){
	$tokens = preg_split('/(\'[^\']*\')|(`[^`]+`)|([.,()=@])|[\s]+/', $stmt, null, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);

	$fail = function($msg) use (&$desc, &$tokens){
		$desc['error'] = $msg;
		$desc['trace'] = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS);
		$desc['rest'] = '';
		while(current($tokens) !== false){
			$desc['rest'] .= current($tokens).' ';
			next($tokens);
		}
		return $desc;
	};
	$expect = function($token) use (&$tokens, $fail){
		if(match_token($tokens, $token)){
			return false;
		} else {
			$prev = implode(' ',array_reverse([prev($tokens),prev($tokens),prev($tokens)])).' ';
			next($tokens);next($tokens);
			$current = next($tokens);
			$next = [next($tokens),next($tokens),next($tokens)];
			$context = $prev.$current.' '.implode(' ',$next);
			$msg = "\n  expected ".(is_array($token) ? implode(', ', $token) : $token)." in";
			$msg .= "\n  ".$context;
			$msg .= "\n  ".str_repeat(' ', strlen($prev)).str_repeat('^', strlen($current));
			return $fail($msg);
		}
	};
	$phrase = function($phrase) use ($expect){
		if(is_string($phrase)) $phrase = explode(' ',$phrase);
		foreach($phrase as $token){
			$e = $expect($token);
			if($e) return $e;
		}
		return false;
	};
	$pop = function() use (&$tokens){
		$token = current($tokens);
		next($tokens);
		return $token;
	};
	$identifier = function(&$value) use ($pop, $fail){
		$token = $pop();
		if($token[0] == '`'){
			$len = strlen($token);
			if($token[$len-1] == '`'){
				$value = substr($token,1,-1);
				return false;
			}
		} elseif(preg_match('/[0-9a-zA-Z$_]/', $token)) {
			$value = $token;
			return false;
		}
		return $fail('expected: valid identifier [ '.$token.' ]');
	};

	$type_lengthy = ['VARBINARY','VARCHAR'];

	$type_lengthy_opt = ['BIT','BINARY','BLOB','CHAR','TEXT'];
	$type_inty = ['TINYINT','SMALLINT','MEDIUMINT','INT','INTEGER','BIGINT']; // all inty types are optionally lengthy
	$type_floaty = ['FLOAT','REAL','DOUBLE']; // all floaty types are inty
	$type_decimaly = ['DECIMAL','NUMERIC']; // all decimaly types are inty

	$type_stringy = ['CHAR','VARCHAR','TINYTEXT','TEXT','MEDIUMTEXT','LONGTEXT'];
	$type_enumy = ['ENUM','SET']; // all enumy types are stringy

	$type_timeywimey = ['TIME','TIMESTAMP','DATETIME'];

	$type_voidy = ['DATE','YEAR','TINYBLOB','MEDIUMBLOB','LONGBLOB','JSON'];

	$type_synonyms = ['BOOL'=>'TINYINT','BOOLEAN'=>'TINYINT'];
	$type_default_length = ['BOOL'=>1,'BOOLEAN'=>1,'SMALLINT'=>6];

	$type_inty = array_merge($type_inty, $type_floaty, $type_decimaly);
	$type_lengthy_opt = array_merge($type_lengthy_opt, $type_inty);
	$type_stringy = array_merge($type_stringy, $type_enumy);
	$known_types = array_merge($type_lengthy, $type_lengthy_opt, $type_stringy, $type_timeywimey, $type_voidy, array_keys($type_synonyms));

	$data_type = function(&$coldesc) use (&$tokens, $pop, $expect, $type_synonyms, $type_default_length,
			$type_lengthy, $type_lengthy_opt, $type_inty, $type_floaty, $type_decimaly,
			$type_timeywimey, $type_stringy, $type_enumy, $type_voidy, $known_types){
		$coldesc['datatype'] = [];
		$close_paren = false;
		$typename = match_token($tokens, $known_types);
		$coldesc['datatype']['name'] = $type = isset($type_synonyms[$typename]) ? $type_synonyms[$typename] : $typename;
		if(in_array($type, $type_floaty)){
			match_token($tokens,'PRECISION');
		}

		if(in_array($type, $type_floaty) || in_array($type, $type_decimaly)) $lengthy_name = 'precision';
		elseif(in_array($type, $type_stringy)) $lengthy_name = 'char_max_length';
		else $lengthy_name = 'length';
		if(in_array($type, $type_lengthy)){
			if($e = $expect('(')) return $e;
			$coldesc['datatype'][$lengthy_name] = $pop();
			$close_paren = true;
		} elseif(in_array($type, $type_lengthy_opt)){
			if(match_token($tokens,'(')){
				$coldesc['datatype'][$lengthy_name] = $pop();
				$close_paren = true;
				if(in_array($type, $type_floaty)){
					if($e = $expect(',')) return $e;
					$coldesc['datatype']['decimals'] = $pop();
				} elseif(in_array($type, $type_decimaly) && match_token($tokens,',')){
					$coldesc['datatype']['decimals'] = $pop();
				}
			} elseif(isset($type_default_length[$typename])){
				$coldesc['datatype']['length'] = $type_default_length[$typename];
			}
			
		} elseif(in_array($type, $type_timeywimey) && match_token($tokens,'(')){
			$coldesc['datatype']['fsc'] = $pop();
			$close_paren = true;
		} elseif(in_array($type, $type_enumy)){
			if($e = $expect('(')) return $e;
			$coldesc['datatype']['values'] = [];
			do {
				$coldesc['datatype']['values'][] = $pop();
			} while (match_token($tokens,','));
			$close_paren = true;
		} elseif($type === 'YEAR'){
			if(match_token($tokens,'(')){
				if($e = $expect('4')) return $e;
				if($e = $expect(')')) return $e;
			}
		}
		if($close_paren && $e = $expect(')')) return $e;

		if(in_array($type, $type_inty)){
			if(match_token($tokens,'UNSIGNED')) $coldesc['datatype']['unsigned'] = true;
			elseif(match_token($tokens,'SIGNED')) $coldesc['datatype']['unsigned'] = false;
			if(match_token($tokens,'ZEROFILL')) $coldesc['datatype']['zerofill'] = true;
		} elseif(in_array($type, $type_stringy)){
			if(match_token($tokens,'CHARACTER')){
				if($e = $expect('SET')) return $e;
				$coldesc['datatype']['character set'] = $pop();
			}
			if(match_token($tokens,'COLLATE')) $coldesc['datatype']['collate'] = $pop();
		}
	};

	$nullity = function(&$coldesc) use (&$tokens, $expect){
		if(match_token($tokens, 'NOT')){
			if($e = $expect('NULL')) return $e;
			$coldesc['nullity'] = 'NOT NULL';
		} elseif(match_token($tokens, 'NULL')){
			$coldesc['nullity'] = 'NULL';
		} else {
			$coldesc['nullity'] = '';
		}
	};

	$index_type = function(&$coldesc) use (&$tokens, $fail){
		if(match_token($tokens, ['USING'])){
			$coldesc['index_using'] = match_token($tokens, ['BTREE','HASH']);
			if(!$coldesc['index_using']){
				return $fail('expected: USING {BTREE | HASH}');
			}
		}
	};

	$index_columns = function(&$coldesc) use (&$tokens, $expect, $identifier, $pop){
		$coldesc['index_columns'] = [];
		if($e = $expect('(')) return $e;
		do {
			$col = [];
			if($e = $identifier($col['name'])) return $e;
			if(match_token($tokens,'(')){
				$col['size'] = $pop();
				if($e = $expect(')')) return $e;
			}
			$col['sort'] = match_token($tokens,['ASC','DESC']);
			$coldesc['index_columns'][] = $col;
		} while (match_token($tokens, ','));
		if($e = $expect(')')) return $e;
	};

	$optional_index_name = function(&$value) use (&$tokens, $identifier){
		$token = current($tokens);
		if($token != '(' && strtoupper($token) != 'USING'){
			if($e = $identifier($value)) return $e;
		}
	};

	$last_column = '#FIRST';
	$column = function() use (&$tokens, &$desc, $expect, $identifier, $index_columns, $index_type,
			$optional_index_name, $data_type, $nullity, $fail, $pop, $type_stringy, &$last_column){
		$coldesc = [];
		$is_unique = null;
		if($token = match_token($tokens, ['INDEX','KEY','UNIQUE','PRIMARY','FULLTEXT'])){
			$coldesc['type'] = 'index';
			if($token == 'FULLTEXT'){
				match_token($tokens, ['INDEX','KEY']);
				$coldesc['index_type'] = 'fulltext';
			} elseif($token == 'UNIQUE'){
				match_token($tokens, ['INDEX','KEY']);
				$coldesc['index_type'] = 'unique';
			} elseif($token == 'PRIMARY'){
				if($e = $expect('KEY')) return $e;
				$coldesc['index_type'] = 'primary';
				$optional_index_name($ignore);
			} else {
				$coldesc['index_type'] = '';
			}
			if($token != 'PRIMARY') $optional_index_name($coldesc['name']);
			if($e = $index_type($coldesc)) return $e;
			if($e = $index_columns($coldesc)) return $e;
		} else {
			$coldesc['type'] = 'column';
			if($e = $identifier($coldesc['name'])) return $e;
			if($e = $data_type($coldesc)) return $e;
			if($e = $nullity($coldesc)) return $e;
			if(match_token($tokens,'DEFAULT')){
				$coldesc['default'] = $pop();
				if(current($tokens) == '('){
					while(current($tokens)!= ')'){
						$coldesc['default'] .= $pop();
					}
					$coldesc['default'] .= $pop();
				}
				$len = strlen($coldesc['default']);
				$is_stringy = in_array($coldesc['datatype']['name'],$type_stringy);
				if(!$is_stringy && $coldesc['default'][0]=="'" && $coldesc['default'][$len-1]=="'"){
					$coldesc['default'] = substr($coldesc['default'], 1, -1);
				}
			}
			
			if(match_token($tokens,'AUTO_INCREMENT')) $coldesc['auto_increment'] = true;
			if(match_token($tokens,'COMMENT')) $coldesc['comment'] = $pop();

			$coldesc['after'] = $last_column;
			$last_column = $coldesc['name'];
		}
		$desc['columns'][] = isset($coldesc['type']) ? $coldesc : $coldesc['sql'];
	};

	$desc = ['type' => 'table', 'statement' => $stmt, 'key' => 'table_'.rand()];

	if($e = $expect('CREATE')) return $e;
	$desc['temporary'] = match_token($tokens, 'TEMPORARY') ? true : false;
	if($e = $expect('TABLE')) return $e;
	if(match_token($tokens, 'IF')){
		if($e = $phrase('NOT EXISTS')) return $e;
		$desc['if not exists'] = true;
	} else {
		$desc['if not exists'] = false;
	}

	if($e = $identifier($desc['name'])) return $e;
	if(match_token($tokens, '.')){
		$desc['database'] = $desc['name'];
		if($e = $identifier($desc['name'])) return $e;
	}

	if($e = $expect('(')) return $e;
	do if($e = $column()) return $e;
	while (match_token($tokens, ','));
	if($e = $expect(')')) return $e;

	$table_options = [
		'AUTO_INCREMENT'=>true,
		'AVG_ROW_LENGTH'=>true,
		'CHARSET'=>true,
		'CHECKSUM'=>['0','1'],
		'COLLATE'=>true,
		'COMMENT'=>true,
		'COMPRESSION'=>['ZLIB','LZ4','NONE'],
		'DATA DIRECTORY'=>true,
		'INDEX DIRECTORY'=>true,
		'DELAY_KEY_WRITE'=>['0','1'],
		'ENCRYPTION'=>['Y','N'],
		'ENGINE'=>true,
		'INSERT_METHOD'=>['NO','FIRST','LAST'],
		'KEY_BLOCK_SIZE'=>true,
		'MAX_ROWS'=>true,
		'MIN_ROWS'=>true,
		'PACK_KEYS'=>['0','1','DEFAULT'],
		'PASSWORD'=>true,
		'ROW_FORMAT'=>['DEFAULT','DYNAMIC','FIXED','COMPRESSED','REDUNDANT','COMPACT'],
		'STATS_AUTO_RECALC'=>['DEFAULT','0','1'],
		'STATS_PERSISTENT'=>['DEFAULT','0','1'],
		'STATS_SAMPLE_PAGES'=>true,
		'TABLESPACE'=>false,
		'UNION'=>false
	];

	$table_option_tokens = array_keys($table_options);
	$table_option_tokens[] = 'DEFAULT';
	$table_option_tokens[] = 'CHARACTER';

	$token = strtoupper(current($tokens));
	while(in_array($token, $table_option_tokens)){
		if($token == 'DEFAULT'){
			$token = strtoupper(next($tokens));
			next($tokens);
			if($token == 'COLLATE') $key = 'COLLATE';
			elseif($token == 'CHARSET') $key = 'CHARSET';
			elseif($token == 'CHARACTER'){
				if($e = $expect('SET')) return $e;
				$key = 'CHARACTER SET';
			} else {
				return $fail("expected: COLLATE, CHARACTER SET or CHARSET, found [$token]");
			}
		} elseif($token == 'CHARACTER'){
			if($e = $expect('SET')) return $e;
			$key = 'CHARSET';
		} else {
			$key = strtoupper($token);
			next($tokens);
		}
		if(!isset($table_options[$key])) return $fail("unknown table option: $key");
		if(isset($desc['table_options'][$key])) return $fail("duplicate table option: $key");
		match_token($tokens, '=');
		if($table_options[$key] === true){
			$value = current($tokens);
			next($tokens);
		} else {
			$value = match_token($tokens, $table_options[$key]);
			if($value === false) return $fail("Invalid value for table option [$key]");
		}
		$desc['table_options'][$key] = $value;

		match_token($tokens, ',');
		$token = strtoupper(current($tokens));
	}

	return $desc;
}

// private; shouldn't be use outside this namespace
function statement_grant($stmt){
	$priv_type_tokens = ['ALL', 'ALTER', 'ALTER', 'CREATE', 'DELETE', 'DROP', 'EVENT', 'EXECUTE', 'FILE', 'GRANT', 'INDEX', 'INSERT', 'LOCK', 'PROCESS', 'PROXY', 'REFERENCES', 'RELOAD', 'REPLICATION', 'SELECT', 'SHOW', 'SHUTDOWN', 'SUPER', 'TRIGGER', 'UPDATE', 'USAGE'];
	$priv_type_sec_tokens = ['ALL' => ['PRIVILEGES'], 'ALTER' => ['ROUTINE'], 'CREATE' => ['ROUTINE', 'TABLESPACE', 'TEMPORARY', 'USER', 'VIEW'], 'GRANT' => ['OPTION'], 'LOCK' => ['TABLES'], 'REPLICATION' => ['CLIENT', 'SLAVE'], 'SHOW' => ['DATABASES', 'VIEW']];
	$priv_type_ter_tokens = ['CREATE TEMPORARY' => ['TABLES']];
	$desc = ['type' => 'not grant/revoke', 'key' => 'unknown_'.rand()];
	$tokens = preg_split('/(\([^\)]*\))|(\'[^\']+\')|(@)|(`[^`]+`)|(\.)|[\s,]+/', $stmt, null, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
	
	$expect = function($token) use (&$tokens, &$desc){
		if(!is_array($token)){
			$token = [$token];
		}
		foreach($token as $t){
			if(match_token($tokens, $t)){
				return false;
			}
		}
		$desc['expected'] = implode(', ', $token);
		return $desc;
	};
	$pop = function() use (&$tokens){
		$token = current($tokens);
		next($tokens);
		return $token;
	};
	$desc['type'] = current($tokens) == 'GRANT' ? 'grant' : (current($tokens) == 'REVOKE' ? 'revoke' : 'not grant/revoke');
	if($e = $expect(['GRANT', 'REVOKE'])) return $e;
	//$desc['type'] = 'grant';
	$priv_types = [];
	while($token = match_token($tokens, $priv_type_tokens)){
		if(in_array($token, array_keys($priv_type_sec_tokens)) && $t = match_token($tokens, $priv_type_sec_tokens[$token])){
			$token .= " $t";
			if(in_array($token, array_keys($priv_type_ter_tokens)) && $t = match_token($tokens, $priv_type_ter_tokens[$token])){
				$token .= " $t";
			}
		}
		$next_token = current($tokens);
		if($next_token[0] == '('){
			$columns = preg_split('/[\(\)\s,`]+/', current($tokens), null, PREG_SPLIT_NO_EMPTY);
			$priv_types[$token.'*'] = ['priv_type' => $token, 'column_list' => $columns];
			next($tokens);
		} else {
			$priv_types[$token] = $token;
		}
	}
	ksort($priv_types);
	$desc['priv_types'] = $priv_types;

	if($e = $expect('ON')) return $e;

	if($object_type = match_token($tokens, ['TABLE','FUNCTION','PROCEDURE'])) $desc['object_type'] = $object_type;

	$desc['database'] = $pop();
	if($desc['database'][0] != '`') $desc['database'] = '`'.$desc['database'].'`';
	if($e = $expect('.')) return $e;
	$desc['table'] = $pop();
	if($desc['table'][0] != '`' && $desc['table'] != '*') $desc['table'] = '`'.$desc['table'].'`';

	if($desc['type'] == 'revoke'){
		if($e = $expect('FROM')) return $e;
	} else {
		if($e = $expect('TO')) return $e;
	}

	$desc['user'] = $pop();
	if($desc['user'][0]=="'") $desc['user'] = str_replace("'", "`", $desc['user']);
	if($e = $expect('@')) return $e;
	$host = $pop();
	if($host[0]=="'") $host = str_replace("'", "`", $host);
	//TODO: re-support ignore host?
	//if(!isset($config['ignore_host']) || !$config['ignore_host']) $desc['user'] .= '@'.$host;
	$desc['user'] .= '@'.$host;
	

	$desc['key'] = $desc['type'].':'.$desc['user'].':'.$desc['database'].'.'.$desc['table'];
	$desc = \Description::from_array($desc);
	return $desc;
}

// private; shouldn't be use outside this namespace
function match_token(&$tokens, $match, $casesensitive = false){
	$token = current($tokens);
	
	if(is_array($match)){
		foreach($match as $string){
			if($token == $string || !$casesensitive && strtolower($token) == strtolower($string)){
				next($tokens);
				return $string;
			}
		}
		return false;
	} else {
		if($token == $match || !$casesensitive && strtolower($token) == strtolower($match)){
			next($tokens);
			return $match;
		} else {
			return false;
		}
	}
}
