<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/

declare(strict_types=1);
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
		$quote_delimiter = ['\'','"','`'];
		$quote_opener = null;
		$in_quote = false;
		$prev_char = null;
		$rest_is_comment = false;
		$len = strlen($line);
		for($i = 0; $i < $len; $i++){
			$char = $line[$i];
			if($in_quote){
				/*
					The quoting character can be escaped with backslash in
					strings, but not in identifiers. Including the quoting
					character in an identifier requires writing the character
					twice, which is odd, but doesn't interfere with this
					method of detecting whether a symbol is inside a quote.
				*/
				if($char === $quote_opener && ($char === '`' || $prev_char !== '\\')){
					$in_quote = false;
				}
			} elseif(in_array($char, $quote_delimiter)) {
				$in_quote = true;
				$quote_opener = $char;
			} elseif($char == ';'){
				break;
			} elseif($char == '#'){
				$rest_is_comment = true;
				break;
			} elseif($char == '-' && $prev_char == '-'){
				/*
					MySQL requires whitespace or a control character after the
					double dash to accept it as a comment. This is not enforced
					here, but might be a good addition in the future.
				*/
				$rest_is_comment = true;
				$i -= 1;
				break;
			}
			$prev_char = $char;
		}
		return [
			'str'=>substr($line, 0, $i),
			'rest'=>$rest_is_comment ? '' : substr($line, $i+1),
			'end'=>isset($line[$i]) && $line[$i] == ';'
		];
	}
}
