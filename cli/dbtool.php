<?php
require_once __DIR__."/../core/classes/core.php";

session_start();

define('OPTION_VOID',1);
define('OPTION_TAKES_VALUE',2);
$long_options = [
	'help'=>OPTION_VOID,
	'action'=>OPTION_TAKES_VALUE,
	'database'=>OPTION_TAKES_VALUE,
	'execute'=>OPTION_VOID,
	'force'=>OPTION_VOID,
	'password'=>OPTION_VOID | OPTION_TAKES_VALUE,
	'user'=>OPTION_TAKES_VALUE,
	'verbose'=>OPTION_VOID,
	'test'=>OPTION_VOID,
	'no-alter'=>OPTION_VOID,
	'no-create'=>OPTION_VOID,
	'no-drop'=>OPTION_VOID
];
$short_options = [
	'h'=>'help',
	'a'=>'action',
	'd'=>'database',
	'e'=>'execute',
	'f'=>'force',
	'p'=>'password',
	'u'=>'user',
	'v'=>'verbose'
];

$options = parse_options();

if(isset($options['help']) && $options['help']) help();

define('TEST_RUN',isset($options['test']) && $options['test']);

if(TEST_RUN) echo "Options:\n".json_encode($options)."\n";

if(isset($options['verbose'])
	&& $options['verbose']
	&& isset($options['configfile'])){
	echo "Configuration file: $options[configfile]\n";
}
if(isset($options['configfile'])){
	$config = load_config($options['configfile']);
	$configdir = dirname(realpath($options['configfile']));
} else {
	$config = [];
	$configdir = '.';
}

foreach($long_options as $name => $type){
	if(isset($options[$name])) $config[$name] = $options[$name];
	elseif(!isset($config[$name])) $config[$name] = false;
}

if($config['action'] != 'diff' && $config['action'] != 'permission') help(); //help exits
define('VERBOSE',$config['verbose']);

$config['db_username'] = $config['user']!==false ? $config['user'] : get_current_user();
$config['db_password'] = $config['password'] === true ? ask_for_password() : $config['password'] || '';

switch(Core::load_json($config, $configdir)){
	case 'wrong_credentials': fail("Connection Error: Username or password incorrect", 2); break;
}

$action = $config['action'] || null;
$core = Core::run($action);
if(!isset($core['obj'])){
	fail("Invalid action: $core[action]",3);
}
if(VERBOSE) echo "Action: [$core[action]]\n";
if($core['action']=='diff') show_diff($core['result']);
elseif($core['action']=='permission') show_permission($core['result']);

if(TEST_RUN) echo "[Test run, skipping execution]\n";
elseif($config['execute'] && ($config['force'] || ask_continue())){
	$options = 0;
	if($core['action']=='diff'){
		if(!$config['no-alter']) $options |= CoreDiff::ALTER;
		if(!$config['no-create']) $options |= CoreDiff::CREATE;
		if(!$config['no-drop']) $options |= CoreDiff::DROP;
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

function help(){
echo <<<'HELP'
Usage: php dbtool.php [OPTIONS] CONFIGFILE [OPTIONS]

General Options:
  -h, --help                           Displays this help text.

  -aACTION, --action=ACTION            Specify the action used, supported actions are 'diff' and 'permission'.
  -e, --execute                        Run the generated SQL to align the database with the provided schema.
  -f, --force                          Combined with -e: Run any SQL without asking first.
  -pPASSWORD, --password[=PASSWORD]    Use given password or if not set, request password before connecting to the database.
  -uUSERNAME, --user=USERNAME          Use the given username when connecting to the database.
  -v, --verbose                        Write extra descriptive output.

  --test                               Run everything as usual, but without executing any SQL.

Diff Specific Options:
  --no-alter                           An executed diff will not include ALTER statements.
  --no-create                          An executed diff will not include CREATE statements.
  --no-drop                            An executed diff will not include DROP statements.


HELP;
exit;
}

function load_config($path){
	$rpath = realpath($path);
	if($rpath===false) fail("Failed to load configfile: $path",65);
	$json = json_decode(file_get_contents($rpath),true);
	if(json_last_error()===JSON_ERROR_NONE) return $json;
	else fail("Error parsing configfile as JSON: ".json_last_error_msg(),66);
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
	return $pw;
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
	global $config;

	$differences_found = false;
	if(!empty($result['drop_queries'])){
		$differences_found = true;
		$count = count($result['tables_in_database_only']);
		if($config['no-drop']){
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
		if($config['no-create']){
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
		if($config['no-alter']){
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

function parse_options(){
	global $argv, $long_options, $short_options;
	$options = [];
	$index = 1;
	while(isset($argv[$index])){
		$option = $argv[$index];
		if($option[0]=='-'){
			if($option[1]=='-'){
				// long form option
				$opt = explode('=',substr($option,2),2);
				if(!isset($long_options[$opt[0]])) fail("Unknown Option ($opt[0])", 33);
				$type = $long_options[$opt[0]];
				if($type & ~OPTION_TAKES_VALUE && isset($opt[1])) fail("Option \"$opt[0]\" can't take a value",34);
				if($type & ~OPTION_VOID && !isset($opt[1])) fail("Option \"$opt[0]\" requires a value.",35);
				$options[$opt[0]] = isset($opt[1]) ? $opt[1] : true;
			} else {
				// short form option
				$i = 1;
				while(isset($option[$i])){
					if(!isset($short_options[$option[$i]])) fail("Unknown Flag ($option[$i])",36);
					$name = $short_options[$option[$i]];
					if(!isset($long_options[$name])) fail("Internal Error: Option \"$name\" ($option[$i]) not implemented correctly",1025);
					if($long_options[$name] & OPTION_TAKES_VALUE){
						// assume the rest of the string is the given value
						$value = substr($option,$i+1);
						if($value === false){
							if($long_options[$name] & OPTION_VOID) $value = true;
							else fail("Option \"$name\" ($option[$i]) requires a value.",37);
						}
						$i = strlen($option);
					} else {
						$value = true;
						$i++;
					}
					$options[$name] = $value;
				}
			}
		} else {
			if(isset($options['configfile'])) fail("Can't use more than one config file",38);
			$options['configfile'] = $option;
		}
		$index++;
	}
	return $options;
}

function fail($msg, $errno = 1){
	echo $msg."\n";
	exit($errno);
}
?>