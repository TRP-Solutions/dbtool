<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/

class Source {
	static public function from($sql, $name = null){
		if(is_a($sql,'Source')){
			$source = $sql;
		} elseif(is_string($sql)){
			$source = new Source($name ?? 'Raw String');
			$source->stmts = self::lines_to_statements(explode("\n",$sql));
		} elseif(is_array($sql) && array_reduce($sql, ['Source','is_lines'], true)){
			$source = new Source($name ?? 'Anonymous array of strings');
			$source->stmts = self::lines_to_statements($sql);
		} else {
			$source = new Source('error');
			$source->error = 'Input is not recognizable';
		}
		return $source;
	}

	static private function is_lines($carry, $item){
		return $carry && is_string($item);
	}

	static private function is_source_list($carry, $item){
		return $carry && is_a($item, 'Source');
	}

	static public function is_list_of($value){
		return is_array($value) && array_reduce($value, ['Source','is_source_list'], true);
	}

	public $error, $warnings = [];
	private $name, $stmts = [];

	private function __construct($name){
		$this->name = $name;
	}

	public function get_name(){
		return $this->name;
	}

	public function get_stmts(){
		return $this->stmts;
	}

	public function is_empty(){
		return empty($this->stmts);
	}

	static private function lines_to_statements($lines){
		$stmts = [];
		$statement = [];
		$i = 0;

		$line = $lines[$i];
		$i += 1;
		while(isset($line)){
			$line = trim($line);
			if(!empty($line)){
				$result = self::parse_line(trim($line));
				//echo json_encode($result).PHP_EOL;
				if(!empty($result['str'])){
					$statement[] = $result['str'];
				}
				if($result['end']){
					$stmts[] = implode(' ', $statement);
					$statement = [];
					if(!empty($result['rest'])){
						$line = $result['rest'];
						continue;
					}
				}
			}
			if(isset($lines[$i])){
				$line = $lines[$i];
				$i += 1;
			} else {
				$line = null;
			}
		}
		return $stmts;
	}

	static private function parse_line($line){
		$line = explode('--', $line, 2)[0];
		$line = explode('#', $line, 2)[0];

		$in_single = false;
		$in_double = false;
		$len = strlen($line);
		for($i = 0; $i < $len; $i++){
			if($line[$i] == '\'' && !$in_double) $in_single = !$in_single;
			elseif($line[$i] == '"' && !$in_single) $in_double = !$in_double;
			elseif($line[$i] == ';' && !$in_single && !$in_double) break;
		}
		$result = ['str'=>substr($line, 0, $i), 'rest'=>substr($line, $i+1), 'end'=>isset($line[$i]) && $line[$i] == ';'];
		return $result;
	}
}