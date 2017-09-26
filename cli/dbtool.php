<?php
require_once __DIR__."/../core/classes/core.php";

$arguments = parse_argv();

if(in_array('h', $arguments['options']) || in_array('help', $arguments['options'])){
	help();
}

define('VERBOSE',in_array('v',$arguments['options']) || in_array('verbose', $arguments['options']));
define('EXECUTE',in_array('e',$arguments['options']) || in_array('execute', $arguments['options']));
define('FORCE',in_array('f',$arguments['options']) || in_array('force', $arguments['options']));
define('REQUEST_PASSWORD',in_array('p',$arguments['options']) || in_array('password', $arguments['options']));
define('NO_ALTER',in_array('no-alter', $arguments['options']));
define('NO_CREATE',in_array('no-create', $arguments['options']));
define('NO_DROP',in_array('no-drop', $arguments['options']));
define('TEST_RUN',in_array('test', $arguments['options']));
session_start();

if(TEST_RUN) echo "Options:\n".json_encode($arguments['options'])."\n";

if(isset($arguments['configfile'])){
	$inputpath = getcwd().'/'.$arguments['configfile'];
	$path = realpath($inputpath);
	if(!$path){
		echo "Error: File not found: $inputpath\n";
		exit;
	}

	// set db username where the config can find it.
	if(isset($arguments['parameters']['username'])) $_GET['db_u'] = $arguments['parameters']['username'];
	if(REQUEST_PASSWORD) ask_for_password();
	if(VERBOSE) echo "Configuration file: $path\n";
	switch(Core::load($path)){
		case 'missing_username': fail("Connection Error: Missing username"); break;
		case 'missing_password': fail("Connection Error: Missing password"); break;
		case 'wrong_credentials': fail("Connection Error: Username or password incorrect"); break;
	}
	$action = isset($arguments['parameters']['action']) ? $arguments['parameters']['action'] : null;
	$core = Core::run($action);
	if(!isset($core['obj'])){
		fail("Invalid action: $core[action]",3);
	}
	if(VERBOSE) echo "Action: [$core[action]]\n";
	if($core['action']=='diff') show_diff($core['result']);
	elseif($core['action']=='permission') show_permission($core['result']);
	
	if(TEST_RUN) echo "[Test run, skipping execution]\n";
	elseif(EXECUTE && (FORCE || ask_continue())){
		$options = 0;
		if($core['action']=='diff'){
			if(!NO_ALTER) $options |= CoreDiff::ALTER;
			if(!NO_CREATE) $options |= CoreDiff::CREATE;
			if(!NO_DROP) $options |= CoreDiff::DROP;
		}
		$lines = $core['obj']->execute($options);
		if(VERBOSE){
			echo "Execution:\n";
			if($lines){
				$messages = DB::get_messages();
				if(empty($messages)){
					echo "$lines SQL lines executed without errors.\n";
				} else {
					echo "The following errors was encountered:\n";
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

General Options:
  -h, --help                 Displays this help text.

  -a, --action ACTION        Specify the action used, supported actions are 'diff' and 'permission'.
  -e, --execute              Run the generated SQL to align the database with the provided schema.
  -f, --force                Combined with -e: Run any SQL without asking first.
  -v, --verbose              Write extra descriptive output.
  -u, --username USERNAME    Use the given username when connecting to the database.
  -p, --password             Request password before connecting to the database.

  --test                     Run everything as usual, but without executing any SQL.

Diff Specific Options:
  --no-alter                 An executed diff will not include ALTER statements.
  --no-create                An executed diff will not include CREATE statements.
  --no-drop                  An executed diff will not include DROP statements.


HELP;
exit;
}

function ask_for_password(){
	$is_windows = strtoupper(substr(PHP_OS, 0, 3)) === 'WIN';
	if($is_windows){
		echo "Hiding input of password isn't supported on Windows\n";
		if(!ask_continue('Are you sure you want to write your password visibly?',false)) return;
	}
	echo "Password: ";
	if(!$is_windows) system('stty -echo');
	$pw = rtrim(fgets(STDIN));
	if(!$is_windows) system('stty echo');
	echo "\n";
	// write password somewhere config can find it.
	$_GET['db_p'] = $pw;
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
}

function show_diff($result){
	if(show_error($result)) return;

	$differences_found = false;
	if(!empty($result['drop_queries'])){
		$differences_found = true;
		$count = count($result['tables_in_database_only']);
		if(NO_DROP){
			if(VERBOSE) echo "Ignoring $count table(s) in database.\n";
		} else {
			echo "Found $count table(s) in database only:\n\t";
			echo implode(', ',$result['tables_in_database_only'])."\n";
			if(VERBOSE){
				echo "The following drop queries will remove them:\n\t";
				echo implode("\n\t",$result['drop_queries'])."\n";
			}
			echo "\n";
		}
	}
	
	if(!empty($result['create_queries'])){
		$differences_found = true;
		$count = count($result['tables_in_file_only']);
		if(NO_CREATE){
			if(VERBOSE) echo "Ignoring $count table(s) in file.\n";
		} else {
			echo "Found $count table(s) in file only:\n\t";
			echo implode(', ',$result['tables_in_file_only'])."\n";
			if(VERBOSE){
				echo "The following create queries will add them:\n";
				echo implode("\n",array_map('indent_text', $result['create_queries']))."\n";
			}
			echo "\n";
		}
	}

	if(!empty($result['alter_queries'])){
		$differences_found = true;
		if(NO_ALTER){
			if(VERBOSE){
				$columns = array_keys(array_filter($result['intersection_columns'],function($e){return !empty($e);}));
				$keys = array_keys(array_filter($result['intersection_keys'],function($e){return !empty($e);}));
				$options = array_keys(array_filter($result['intersection_options'],function($e){return !empty($e);}));
				$count = count(array_unique($columns+$keys+$options));
				echo "Ignoring differences in $count table(s).\n";
			}
			
		} else {
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
	}

	if(!$differences_found){
		echo "No differences found.\n";
		exit;
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

function ask_continue($msg = null, $default_yes = true){
	if(isset($msg)) echo $msg;
	else echo "Do you want to continue?";
	if($default_yes){
		echo " [Y/n]:";
	} else {
		echo " [y/N]:";
	}
	$line = trim(fgets(STDIN));
	if(empty($line)) return $default_yes;
	$line = strtolower($line);
	if($line=='y') return true;
	elseif($line=='n') return false;
	else {
		echo "Please answer 'y' or 'n'.\n";
		return ask_continue($msg, $default_yes);
	}
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
			case '-a': case '--action':
				read_arg('action',$arguments, $index);
				break;
			case '-u': case '--username':
				read_arg('username',$arguments, $index);
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

function read_arg($name, &$arguments, &$index){
	global $argv;
	if(!isset($argv[$index+1]) || $argv[$index+1][0]=='-') fail("Error: missing parameter for flag [$arg]", 2);
	$arguments['parameters']['action'] = $argv[$index+1];
	$index++;
}

function fail($msg, $errno = 1){
	echo $msg."\n";
	exit($errno);
}
?>