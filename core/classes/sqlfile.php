<?php
require_once __DIR__.'/db.php';

class SQLFile {
	private $filename;
	public $exists;
	private $str_buffer = '';
	private $vars;
	public $error = '';
	private $cache = [];

	public static function get_all($file_ending = 'sql'){
		$cwd = getcwd();
		$schemas = [];
		if(defined('SCHEMAPATH')){
			$schemapath = strtok(SCHEMAPATH, ';');
		} else {
			$schemapath = strtok('.', ';');
		}
		while($schemapath){
			$schemadir = realpath(ROOTDIR . $schemapath);
			if(is_dir($schemadir)){
				chdir($schemadir);
				$glob = glob($schemadir."/*.sql");
				$schemas = array_merge($schemas, $glob);
				$subpath = 'mo_*';
				if(defined('SCHEMA_MODULE_SUBFOLDER') && !empty(SCHEMA_MODULE_SUBFOLDER)) $subpath .= '/'.SCHEMA_MODULE_SUBFOLDER;
				$glob = glob($schemadir."/$subpath/*.sql");
				$schemas = array_merge($schemas, $glob);
			} else {
				View::msg('error',"Directory '$schemapath' not found.");
			}
			$schemapath = strtok(';');
		}
		chdir($cwd);
		if(!empty($file_ending)){
			return array_filter($schemas, function($schema) use ($file_ending){
				return $file_ending == explode('.', $schema, 2)[1];
			});
		} else {
			return $schemas;
		}
	}

	public function __construct($fname, $vars = []) {
		if($fname[0] == '/'){
			$filepath = realpath($fname);
		} else {
			if(defined('SCHEMAPATH')){
				$schemapath = strtok(SCHEMAPATH, ';');
			} else {
				$schemapath = strtok('.', ';');
			}
			while($schemapath){
				$schemadir = realpath(ROOTDIR . $schemapath);
				$filepath = realpath($schemadir .'/'. $fname);
				if(is_file($filepath)) break;
				$schemapath = strtok(';');
			}
		}
		$this->filename = $filepath;
		$this->exists = isset($filepath) && is_file($filepath);
		$this->vars = $vars;
	}

	private function parse_line($line){
		$line = explode('--', $line, 2)[0];
		$line = explode('#', $line, 2)[0];
		$fragments = explode(';', $line, 2);
		if(!$fragments){
			return ['end' => false, 'str' => ''];
		}
		$count = count($fragments);
		$result = ['end' => $count == 2];
		if($count >= 1){
			$result['str'] = $fragments[0];
		}
		if($count == 2){
			$result['rest'] = $fragments[1];
		}
		return $result;
	}

	private function get_next_statement($stream){
		$statement = array();
		$result = $this->parse_line($this->str_buffer);
		$statement[] = $result['str'];
		while(!$result['end']){
			$line = fgets($stream);
			if(!$line){
				break;
			}
			$result = $this->parse_line(trim($line));
			if(!empty($result['str'])){
				$statement[] = $result['str'];
			}
		}
		if(isset($result['rest'])){
			$this->str_buffer = $result['rest'];
		}
		return implode(' ', $statement);
	}

	private function read_statements(){
		if(isset($this->statements)){
			return true;
		}
		if(!$this->exists){
			$this->error = "File does not exist: {$this->filename}";
			return false;
		}
		$statements = array();
		$stream = fopen($this->filename, 'r');
		$first_line = fgets($stream);
		if(substr($first_line, 0, 9) == '#!include'){
			$filenames = explode(',', substr($first_line, 10));
			foreach($filenames as $filename){
				$filename = trim($filename);
				if($filename[0] != '/'){
					$dir = dirname($this->filename);
					$filename = realpath($dir.'/'.$filename);
				}
				$file = new SQLFile($filename, $this->vars);
				if($file->exists){
					$statements = array_merge($statements, $file->get_all_stmts());
				}
			}
		} else {
			fseek($stream, 0);
		}
		while($stmt = $this->get_next_statement($stream)){
			foreach($this->vars as $search => $replace){
				$stmt = str_replace('['.$search.']', $replace, $stmt);
			}
			$statements[] = $stmt;
		}
		$this->statements = $statements;
		fclose($stream);
		return true;
	}

	private function execute_stmt_list($list){
		if(!$list){
			return false;
		}
		foreach($list as $stmt){
			$result = DB::sql($stmt);
			if(!$result){
				$result = false;
				break;
			}
		}
		return $result;
	}

	public function execute(){
		return $this->execute_stmt_list($this->get_all_stmts());
	}

	public function execute_tables_only(){
		return $this->execute_stmt_list($this->get_create_table_stmts());
	}

	public function get_all_stmts(){
		$result = $this->read_statements();
		if(!$result){ return false; }
		return $this->statements;
	}

	public function get_create_table_stmts(){
		if(isset($this->cache['create_table_stmts'])) return $this->cache['create_table_stmts'];
		$result = $this->read_statements();
		if(!$result){ return false; }
		$list = array();
		$table_pattern = '/^\s*CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+`?(.+?)`?\s*\(/';
		foreach($this->statements as $stmt){
			$matches = array();
			if(preg_match($table_pattern, $stmt, $matches)){
				$list[$matches[1]] = $stmt;
			}
		}
		$this->cache['create_table_stmts'] = $list;
		return $list;
	}

	public function get_col_defs($tablename){
		$list = $this->get_create_table_stmts();
		$table = $list[$tablename];
		$pbegin = '^\s*CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+`?.+?`?\s*\(';
		$pend = '\)[^;]*$';
		$pattern = "/$pbegin(.*)$pend/";
		$matches = array();
		preg_match($pattern, $table, $matches);
		return $this->parse_col_defs($matches[1]);
	}

	private function parse_key($tokens){
		$key = ['def' => implode(' ', $tokens)];
		$i = 0;
		if($tokens[$i] == 'CONSTRAINT'){
			$key['contraint'] = $tokens[$i+1];
			$i += 2;
		}
		if($tokens[$i] == 'PRIMARY'){
			$key['name'] = $tokens[$i];
			$i++;
		} elseif($tokens[$i] == 'UNIQUE'){
			$key['unique'] = true;
			$i++;
		}
		if($tokens[$i] == 'KEY'){
			$i++;
		} else {
			return false;
		}
		if(!isset($key['name']) && $tokens[$i][0] != '('){
			$key['name'] = str_replace('`','',$tokens[$i]);
			$i++;
		}
		if($tokens[$i][0] != '('){
			$key['type'] = $tokens[$i];
			$i++;
		}
		if($tokens[$i][0] == '('){
			$key['cols'] = explode(',',str_replace(['(', ')','`',' '], '', $tokens[$i]));
			$i++;
		}
		if(isset($tokens[$i])){
			$key['options'] = [];
		}
		while(isset($tokens[$i])){
			$key['options'][] = $tokens[$i];
		}
		return $key;
	}

	private function parse_col_defs($string){
		

		$token = '';
		$tokens = [];
		$defs = [
			'cols' => [],
			'keys' => [],
			'unknown' => []
		];
		$reading_colname = false;
		$colname = '';
		$parens = 0;

		$endtoken = function() use (&$token, &$tokens){
			if(!empty($token)){
				$tokens[] = $token;
				$token = '';
			}
		};
		$enddef = function() use ($endtoken, &$defs, &$colname, &$tokens){
			$endtoken();
			if(!empty($colname)){
				$defs['cols'][$colname] = implode(' ', $tokens);
			} elseif(in_array('KEY', $tokens) || in_array('INDEX', $tokens)){
				$key = $this->parse_key($tokens);
				if($key && $key['name']){
					$defs['keys'][$key['name']] = $key;
				} else {
					$defs['keys'][] = $tokens;
				}
			} else {
				$defs['unknown'][] = $tokens;
			}
			$colname = '';
			$tokens = [];
		};

		for($i = 0; $i < strlen($string); $i++){
			$c = $string[$i];
			switch($c){
				case ' ':
					if(!$parens){
						$endtoken();
						continue 2;
					}
					break;
				case ',':
					if(!$parens){
						$enddef();
						continue 2;
					}
					break;
				case '(':
					$parens++;
					break;
				case ')':
					$parens--;
					break;
				case '`':
					if(count($tokens) == 0){$reading_colname = !$reading_colname;}
					break;
			}
			if($reading_colname && $c != '`'){
				$colname .= $c;
			}
			$token .= $c;
		}
		$enddef();
		return $defs;
	}
	/*
	public function parse(){
		$result = $this->read_statements();
		if(!$result){ return false; }
		$structure = array();
		foreach($this->statements as $stmt){
			$element = $this->parse_statement($stmt);
			if($element['type'] == 'table'){
				$structure[$element['name']] = $element['contents'];
			}
		}
		View::msg('parse', $structure);
		return $structure;
	}

	private function parse_statement($stmt){
		$table_pattern = '/^\s*CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+`?(.+?)`?\s*\((.*)\)/';
		$matches = array();
		if(preg_match($table_pattern, $stmt, $matches)){
			return ['type' => 'table', 'name' => $matches[1], 'columns' => $matches[2]];
		}
		return ['type' => 'unknown'];
	}
	*/
	public static function parse_statement($stmt, $config = []){
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
				return self::parse_statement_table($stmt);
			case 'insert':
				return self::parse_statement_insert($stmt);
			case 'grant':
				return self::parse_statement_grant($stmt, $config);
			default:
				return ['type' => $type, 'statement' => $stmt];
		}
	}

	private static function parse_statement_table($stmt){
		return ['type' => 'table', 'statement' => $stmt, 'key' => 'table_'.rand()];
	}

	private static function parse_statement_insert($stmt){
		return ['type' => 'insert', 'statement' => $stmt, 'key' => 'insert_'.rand()];
	}

	private static function parse_statement_grant($stmt, $config){
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
				if(self::match_token($tokens, $t)){
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
		while($token = self::match_token($tokens, $priv_type_tokens)){
			if(in_array($token, array_keys($priv_type_sec_tokens)) && $t = self::match_token($tokens, $priv_type_sec_tokens[$token])){
				$token .= " $t";
				if(in_array($token, array_keys($priv_type_ter_tokens)) && $t = self::match_token($tokens, $priv_type_ter_tokens[$token])){
					$token .= " $t";
				}
			}
			$next_token = current($tokens);
			if($next_token[0] == '('){
				$columns = preg_split('/[\(\)\s,`]+/', current($tokens), null, PREG_SPLIT_NO_EMPTY);
				$priv_types[$token] = ['priv_type' => $token, 'column_list' => $columns];
				next($tokens);
			} else {
				$priv_types[$token] = $token;
			}
		}
		$desc['priv_types'] = $priv_types;

		if($e = $expect('ON')) return $e;

		if($object_type = self::match_token($tokens, ['TABLE','FUNCTION','PROCEDURE'])) $desc['object_type'] = $object_type;

		$desc['database'] = $pop();
		if($e = $expect('.')) return $e;
		$desc['table'] = $pop();

		if($desc['type'] == 'revoke'){
			if($e = $expect('FROM')) return $e;
		} else {
			if($e = $expect('TO')) return $e;
		}

		$desc['user'] = $pop();
		if($e = $expect('@')) return $e;
		$host = $pop();
		if(!isset($config['ignore_host']) || !$config['ignore_host']) $desc['user'] .= '@'.$host;
		

		$desc['key'] = $desc['type'].':'.$desc['user'].':'.$desc['database'].'.'.$desc['table'];
		//$desc['statement'] = $stmt;
		return $desc;
	}

	private static function match_token(&$tokens, $match, $casesensitive = false){
		$token = current($tokens);
		
		if(is_array($match)){
			foreach($match as $string){
				if($token == $string || $casesensitive && strtolower($token) == strtolower($string)){
					next($tokens);
					return $string;
				}
			}
			return false;
		} else {
			if($token == $match || $casesensitive && strtolower($token) == strtolower($match)){
				next($tokens);
				return $match;
			} else {
				return false;
			}
		}
	}
}
?>