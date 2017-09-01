<?php
require_once __DIR__."/../core/classes/core.php";

$arguments = parse_argv();

if(in_array('h', $arguments['options']) || in_array('help', $arguments['options'])){
	help();
}

define('VERBOSE',in_array('v',$arguments['options']) || in_array('verbose', $arguments['options']));
define('EXECUTE',in_array('e',$arguments['options']) || in_array('execute', $arguments['options']));
session_start();

if(isset($arguments['configfile'])){
	$inputpath = getcwd().'/'.$arguments['configfile'];
	$path = realpath($inputpath);
	if(!$path){
		echo "Error: File not found: $inputpath\n";
		exit;
	}
	if(VERBOSE) echo "Configuration file: $path\n";
	$action = isset($arguments['parameters']['action']) ? $arguments['parameters']['action'] : null;
	$core = Core::run($path, $action, false);
	if(!isset($core['obj'])){
		fail("Invalid action: $core[action]",3);
	}
	if(VERBOSE) echo "Action: [$core[action]]\n";
	if($core['action']=='diff') show_diff($core['result']);
	elseif($core['action']=='permission') show_permission($core['result']);

	if(EXECUTE){
		$lines = $core['obj']->execute();
		if(VERBOSE){
			echo "Execution:\n";
			if($lines){
				$messages = DB::get_messages();
				if(empty($messages)){
					echo "$lines SQL lines executed without errors.\n";
				} else {
					echo "The following errors was found:\n";
					foreach($messages as $msg) echo "$msg[text]\n";
				}
			} else {
				echo "No SQL lines were executed.\n";
			}
			
		}
	}
	exit;
}

help();

function help(){
echo <<<'HELP'
Usage: php dbtool.php [OPTIONS] CONFIGFILE [OPTIONS]

Options:
  -h, --help             Displays this help text.

  -a, --action ACTION    Specify the action used, supported actions are 'diff' and 'permission'.
  -e, --execute          Runs the generated SQL to align the database with the provided schema.
  -v, --verbose          Writes verbose output while executing.


HELP;
exit;
}

function show_permission($result){
	$files = [];
	foreach($result['files'] as $file){
		if(empty($file['sql'])) continue;
		$files[] = $file;
	}
	$count = count($files);
	echo "Found differences in $count file(s):\n";
	foreach($files as $file){
		echo "\t".$file['title']."\n";
		if(VERBOSE){
			echo "\t\t";
			echo implode("\n\t\t",$file['sql'])."\n";
		}
	}
	if(VERBOSE){
		
	}
}

function show_diff($result){
	if(show_error($result)) return;

	$differences_found = false;
	if(!empty($result['drop_queries'])){
		$differences_found = true;
		$count = count($result['tables_in_database_only']);
		echo "Found $count table(s) in database only:\n\t";
		echo implode(', ',$result['tables_in_database_only'])."\n";
		if(VERBOSE){
			echo "The following drop queries will remove them:\n\t";
			echo implode("\n\t",$result['drop_queries'])."\n";
		}
		echo "\n";
	}
	
	if(!empty($result['create_queries'])){
		$differences_found = true;
		$count = count($result['tables_in_file_only']);
		echo "Found $count table(s) in file only:\n\t";
		echo implode(', ',$result['tables_in_file_only'])."\n";
		if(VERBOSE){
			echo "The following create queries will add them:\n";
			echo implode("\n",array_map('indent_text', $result['create_queries']))."\n";
		}
		echo "\n";
	}

	if(!empty($result['alter_queries'])){
		$differences_found = true;
		$array = show_nonempty_keys("table(s) with column differences", $result['intersection_columns']);
		$array = show_nonempty_keys("table(s) with key differences", $result['intersection_keys']);
		$array = show_nonempty_keys("table(s) with option differences", $result['intersection_options']);
		if(VERBOSE){
			echo "The following alter queries will align them:\n";
			foreach($result['alter_queries'] as $table => $queries){
				echo $table.":\n";
				echo implode("\n",array_map('indent_text', $queries))."\n";
			}
		}
	}

	if(!$differences_found){
		echo "No differences found.\n";
	}
}

function show_nonempty_keys($name, $array){
	$array = array_filter($array,function($e){return !empty($e);});
	$count = count($array);
	if($count == 0) return [];
	echo "Found $count $name:\n\t";
	echo implode(', ',array_keys($array))."\n\n";
	return $array;
}

function show_error($result){
	if($result['errno'] !== 0){
		echo "Error ($result[errno]): ";
		if(isset($result['error'])) echo $result['error'];
		else echo 'Unknown Error';
		echo "\n";
		if(isset($result['sqlerror'])) echo $result['sqlerror'];
		echo "\n";
		return true;
	}
	return false;
}

function indent_text($text){
	return "\t".implode("\n\t",explode("\n",$text));
}

function parse_argv(){
	global $argv;
	$arguments = ['options'=>[]];
	$index = 1;
	while(isset($argv[$index])){
		$arg = strtolower($argv[$index]);
		switch($arg){
			case '-a':
			case '--action':
				if(!isset($argv[$index+1]) || $argv[$index+1][0]=='-') fail("Error: missing parameter for flag [$arg]", 2);
				$arguments['parameters']['action'] = $argv[$index+1];
				$index++;
			break;
			default:
			if($arg[0]=='-'){
				if($arg[1]=='-'){
					$arguments['options'][] = substr($arg, 2);
				} else {
					$arguments['options'] += str_split(substr($arg, 1));
				}
			} else {
				$arguments['configfile'] = $argv[$index];
			}
		}
		$index++;
	}
	if(!empty($arguments['options'])){
		$arguments['options'] = array_unique($arguments['options']);
		sort($arguments['options']);
	}
	if(isset($argv[$index])){
		$arguments['configfile'] = $argv[$index];
		$index++;
	}
	return $arguments;
}

function fail($msg, $errno = 1){
	echo $msg."\n";
	exit($errno);
}
?>