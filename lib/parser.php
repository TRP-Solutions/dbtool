<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/

namespace Parser;
require_once __DIR__.'/description.php';

define('PARSER\TYPE_LENGTHY',['VARBINARY','VARCHAR']);
define('PARSER\TYPE_DECIMALY',['DECIMAL','NUMERIC']); // all decimaly types are inty
define('PARSER\TYPE_FLOATY',['FLOAT','REAL','DOUBLE']); // all floaty types are inty
define('PARSER\TYPE_INTY',array_merge(['TINYINT','SMALLINT','MEDIUMINT','INT','INTEGER','BIGINT'],TYPE_FLOATY,TYPE_DECIMALY)); // all inty types are optionally lengthy
define('PARSER\TYPE_LENGTHY_OPT',array_merge(['BIT','BINARY','BLOB','CHAR','TEXT'],TYPE_INTY));
define('PARSER\TYPE_ENUMY',['ENUM','SET']); // all enumy types are stringy
define('PARSER\TYPE_STRINGY',array_merge(['CHAR','VARCHAR','TINYTEXT','TEXT','MEDIUMTEXT','LONGTEXT'],TYPE_ENUMY));
define('PARSER\TYPE_TIMEYWIMEY',['TIME','TIMESTAMP','DATETIME']);
define('PARSER\TYPE_VOIDY',['DATE','YEAR','TINYBLOB','MEDIUMBLOB','LONGBLOB','JSON']);
define('PARSER\TYPE_SYNONYMS',['BOOL'=>'TINYINT','BOOLEAN'=>'TINYINT']);
define('PARSER\TYPE_DEFAULT_LENGTH',['BOOL'=>1,'BOOLEAN'=>1,'SMALLINT'=>6]);

function statement($stmt){
	$stmttype = '';
	$stmt = trim($stmt);
	$types = [
		'CREATE TABLE' => 'table',
		'INSERT' => 'insert',
		'GRANT' => 'grant',
		'REVOKE' => 'grant',
		'CREATE USER' => 'user'
	];
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
		case 'user':
			return statement_user($stmt);
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
	if(isset($datatype['fsp'])){
		$str .= " ($datatype[fsp])";
	}
	if(isset($datatype['values'])){
		$str .= " (".implode(', ',$datatype['values']).')';
	}
	if($include_attributes){
		if(isset($datatype['unsigned']) && $datatype['unsigned']) $str .= " UNSIGNED";
		if(isset($datatype['zerofill']) && $datatype['zerofill']) $str .= " ZEROFILL";
		if(isset($datatype['character set'])) $str .= " CHARACTER SET ".$datatype['character set'];
		if(isset($datatype['collate'])) $str .= " COLLATE ".$datatype['collate'];
	}
	return $str;
}

function encode_index_column($col){
	$str = $col['name'];
	if(!empty($col['size'])) $str .= "($col[size])";
	if(!empty($col['sort'])) $str .= " $col[sort]";
	return $str;
}

// private; shouldn't be used outside this namespace
function statement_insert($stmt){
	return ['type' => 'insert', 'statement' => $stmt, 'key' => 'insert_'.rand()];
}

// private; shouldn't be used outside this namespace
function statement_table($stmt){
	$tokens = preg_split('/(\'[^\']*\')|(`[^`]+`)|([.,()=@])|[\s]+/', $stmt, 0, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);

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
		} elseif(preg_match('/^[0-9a-zA-Z$_\']+$/', $token)) {
			$value = $token;
			return false;
		}
		return $fail('expected: valid identifier [ '.$token.' ]');
	};

	$type_lengthy = TYPE_LENGTHY;

	$type_lengthy_opt = TYPE_LENGTHY_OPT;
	$type_inty = TYPE_INTY; // all inty types are optionally lengthy
	$type_floaty = TYPE_FLOATY; // all floaty types are inty
	$type_decimaly = TYPE_DECIMALY; // all decimaly types are inty

	$type_stringy = TYPE_STRINGY;
	$type_enumy = TYPE_ENUMY; // all enumy types are stringy

	$type_timeywimey = TYPE_TIMEYWIMEY;

	$type_voidy = TYPE_VOIDY;

	$type_synonyms = TYPE_SYNONYMS;
	$type_default_length = TYPE_DEFAULT_LENGTH;

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
			$coldesc['datatype']['fsp'] = $pop();
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
			else $coldesc['datatype']['unsigned'] = false;
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

	$index_columns = function(&$value) use (&$tokens, $expect, $identifier, $pop){
		if($e = $expect('(')) return $e;
		do {
			$col = [];
			if($e = $identifier($col['name'])) return $e;
			if(match_token($tokens,'(')){
				$col['size'] = $pop();
				if($e = $expect(')')) return $e;
			}
			$col['sort'] = match_token($tokens,['ASC','DESC']);
			$value[] = $col;
		} while (match_token($tokens, ','));
		if($e = $expect(')')) return $e;
	};

	$index_reference = function(&$coldesc) use (&$tokens, $expect, $identifier, $pop, $index_columns){
		if($e = $expect('REFERENCES')) return $e;
		if($e = $identifier($coldesc['index_reference_table'])) return $e;
		if(match_token($tokens, '.')){
			$coldesc['index_reference_database'] = $coldesc['index_reference_table'];
			if($e = $identifier($coldesc['index_reference_table'])) return $e;
			$coldesc['index_reference_table_quoted'] = '`'.$coldesc['index_reference_database'].'`.`'.$coldesc['index_reference_table'].'`';
		} else {
			$coldesc['index_reference_table_quoted'] = '`'.$coldesc['index_reference_table'].'`';
		}
		if($e = $index_columns($coldesc['index_reference_columns'])) return $e;
	};

	$optional_index_name = function(&$value) use (&$tokens, $identifier){
		$token = current($tokens);
		if($token != '(' && strtoupper($token) != 'USING'){
			if($e = $identifier($value)) return $e;
		}
	};

	$optional_reference_option = function(&$coldesc, $key, $action) use (&$tokens){
		if(match_token($tokens, 'ON') && match_token($tokens, $action)){
			if($token = match_token($tokens, ['RESTRICT','CASCADE','SET','NO'])){
				if($token == 'SET'){
					if($token = match_token($tokens, ['NULL','DEFAULT'])){
						$coldesc[$key] = 'SET '.$token;
						return;
					}
				} elseif($token == 'NO'){
					if(match_token($tokens, 'ACTION')){
						$coldesc[$key] = 'NO ACTION';
						return;
					}
				} elseif(!empty($token)) {
					$coldesc[$key] = $token;
					return;
				}
			}
			return $fail("expected: ON $action {RESTRICT | CASCADE | SET NULL | SET DEFAULT | NO ACTION}");
		}
	};

	$last_column = '#FIRST';
	$column = function() use (&$tokens, &$desc, $expect, $identifier, $index_columns, $index_type, $index_reference,
			$optional_index_name, $optional_reference_option, $data_type, $nullity, $fail, $pop, $type_stringy, &$last_column){
		$coldesc = [];
		$is_unique = null;
		if($token = match_token($tokens, ['INDEX','KEY','UNIQUE','PRIMARY','FULLTEXT','FOREIGN','CONSTRAINT'])){
			if($token == 'CONSTRAINT'){
				if($e = $identifier($coldesc['constraint'])) return $e;
				$token = match_token($tokens, ['INDEX','KEY','UNIQUE','PRIMARY','FULLTEXT','FOREIGN']);
				if($token === false){
					return $fail('expected INDEX definition');
				}
			}
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
			} elseif($token == 'FOREIGN'){
				if($e = $expect('KEY')) return $e;
				$coldesc['index_type'] = 'foreign';
			} else {
				$coldesc['index_type'] = '';
			}
			if($token != 'PRIMARY'){
				$optional_index_name($coldesc['name']);
			}
			if($e = $index_type($coldesc)) return $e;
			if($e = $index_columns($coldesc['index_columns'])) return $e;
			if($token == 'FOREIGN'){
				if($e = $index_reference($coldesc)) return $e;
				if($e = $optional_reference_option($coldesc, 'index_on_delete','DELETE')) return $e;
				if($e = $optional_reference_option($coldesc, 'index_on_update','UPDATE')) return $e;
			}
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

// private; shouldn't be used outside this namespace
function statement_grant($stmt){
	$priv_type_tokens = ['ALL', 'ALTER', 'ALTER', 'CREATE', 'DELETE', 'DROP', 'EVENT', 'EXECUTE', 'FILE', 'GRANT', 'INDEX', 'INSERT', 'LOCK', 'PROCESS', 'PROXY', 'REFERENCES', 'RELOAD', 'REPLICATION', 'SELECT', 'SHOW', 'SHUTDOWN', 'SUPER', 'TRIGGER', 'UPDATE', 'USAGE'];
	$priv_type_sec_tokens = ['ALL' => ['PRIVILEGES'], 'ALTER' => ['ROUTINE'], 'CREATE' => ['ROUTINE', 'TABLESPACE', 'TEMPORARY', 'USER', 'VIEW'], 'GRANT' => ['OPTION'], 'LOCK' => ['TABLES'], 'REPLICATION' => ['CLIENT', 'SLAVE'], 'SHOW' => ['DATABASES', 'VIEW']];
	$priv_type_ter_tokens = ['CREATE TEMPORARY' => ['TABLES']];
	$desc = ['type' => 'not grant/revoke', 'key' => 'unknown_'.rand()];
	$tokens = preg_split('/(\([^\)]*\))|(\'[^\']+\')|(@)|(`[^`]+`)|(\.)|[\s,]+/', $stmt, 0, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);
	
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
			$columns = preg_split('/[\(\)\s,`]+/', current($tokens), 0, PREG_SPLIT_NO_EMPTY);
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

// private; shouldn't be used outside this namespace
function statement_user($stmt){
	$tokens = preg_split('/(\'[^\']*\')|(`[^`]+`)|([.,()=@])|[\s]+/', $stmt, 0, PREG_SPLIT_NO_EMPTY | PREG_SPLIT_DELIM_CAPTURE);

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
	$name = function(&$value) use ($pop, $fail){
		$token = $pop();
		if($token[0] == '`'){
			$len = strlen($token);
			if($token[$len-1] == '`'){
				$value = "'".substr($token,1,-1)."'";
				return false;
			}
		} elseif($token[0] == "'"){
			$len = strlen($token);
			if($token[$len-1] == "'"){
				$value = $token;
				return false;
			}
		} elseif($token[0] == '"'){
			$len = strlen($token);
			if($token[$len-1] == '"'){
				$value = "'".substr($token,1,-1)."'";
				return false;
			}
		} elseif(preg_match('/^[0-9a-zA-Z$_]+$/', $token)) {
			$value = "'".$token."'";
			return false;
		}
		return $fail('expected: valid name [ '.$token.' ]');
	};
	$label = function(&$value) use ($pop, $fail){
		$token = $pop();
		if(preg_match('/^\'[^\']+\'$/', $token)) {
			$value = $token;
			return false;
		}
		if(preg_match('/^[0-9a-zA-Z$_]+$/', $token)) {
			$value = $token;
			return false;
		}
		return $fail('expected: valid label [ '.$token.' ]');
	};
	$string = function(&$value) use ($pop, $fail){
		$token = $pop();
		if(preg_match('/^\'[^\']+\'$/', $token)) {
			$value = substr($token,1,-1);
			return false;
		}
		return $fail('expected: valid string [ '.$token.' ]');
	};

	$desc = ['type' => 'user', 'statement' => $stmt, 'key' => 'user_'.rand()];

	if($e = $phrase('CREATE USER')) return $e;

	if(match_token($tokens, 'IF')){
		if($e = $phrase('NOT EXISTS')) return $e;
		$desc['if not exists'] = true;
	} else {
		$desc['if not exists'] = false;
	}

	if($e = $name($desc['username'])) return $e;
	if(match_token($tokens, '@')){
		if($e = $name($desc['host'])) return $e;
	} else {
		$desc['host'] = "'%'";
	}
	$desc['user'] = $desc['username'].'@'.$desc['host'];

	if(match_token($tokens, 'IDENTIFIED')){
		if(match_token($tokens, 'WITH')){
			if($e = $label($desc['auth_plugin'])) return $e;
			if($token = match_token($tokens, ['AS','BY'])){
				$desc['auth_hashed'] = $token == 'AS';
				if($e = $string($desc['auth_string'])) return $e;
			}
		} elseif(match_token($tokens, 'BY')) {
			$desc['auth_hashed'] = (bool) match_token($tokens,'PASSWORD');
			if($e = $string($desc['auth_string'])) return $e;
		}
	}

	$desc['tls'] = 'NONE';
	if(match_token($tokens, 'REQUIRE')){
		$tls_option = match_token($tokens, ['NONE','SSL','X509','CIPHER','ISSUER','SUBJECT']);
		if($tls_option === false){
			return $fail("Expected TLS option");
		} elseif($tls_option != 'NONE') {
			if(in_array($tls_option, ['CIPHER','ISSUER','SUBJECT'])){
				$tls_option = [$tls_option,null];
				if($e = $string($tls_option[1])) return $e;
				$desc['tls'] = [$tls_option];
				while((match_token($tokens,'AND') || true)
					&& ($tls_option = match_token($tokens, ['CIPHER','ISSUER','SUBJECT']))
				){
					$tls_option = [$tls_option,null];
					if($e = $string($tls_option[1])) return $e;
					$desc['tls'][] = $tls_option;
				}
			} else {
				$desc['tls'] = $tls_option;
			}
		}
	}

	if(match_token($tokens,'WITH')){
		$resource_tokens = [
			'MAX_QUERIES_PER_HOUR',
			'MAX_UPDATES_PER_HOUR',
			'MAX_CONNECTIONS_PER_HOUR',
			'MAX_USER_CONNECTIONS',
			'MAX_STATEMENT_TIME'
		];
		while($resource = match_token($tokens, $resource_tokens)){
			$desc['resource'][] = [$resource,$pop()];
		}
	}

	if(match_token($tokens,'ACCOUNT')){
		$account = match_token($tokens, ['LOCK','UNLOCK']);
		if($account){
			$desc['account_lock'] = $account;
		} else {
			return $fail("Expected ACCOUNT LOCK or ACCOUNT UNLOCK");
		}
	}

	return $desc;
}

// private; shouldn't be used outside this namespace
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
