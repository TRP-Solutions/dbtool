<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/
require_once __DIR__."/../lib/core.php";

function debug(...$msg){
	echo json_encode($msg,JSON_PRETTY_PRINT)."\n";
}

session_start();

define('OPTION_VOID',0);
define('OPTION_TAKES_VALUE',1);
define('OPTION_NOT_VOID',2); // never used without OPTION_TAKES_VALUE
define('OPTION_REQUIRES_VALUE',3); // 0b01 + 0b10, combined OPTION_TAKES_VALUE & OPTION_NOT_VOID
define('OPTION_REQUIRES_KEY_VALUE',4);
define('OPTION_LIST_VALUE',8);
$long_options = [
	'help'=>OPTION_VOID,
	'execute'=>OPTION_VOID,
	'force'=>OPTION_VOID,
	'password'=>OPTION_TAKES_VALUE,
	'user'=>OPTION_REQUIRES_VALUE,
	'verbose'=>OPTION_VOID,
	'test'=>OPTION_VOID,
	'database'=>OPTION_REQUIRES_VALUE,
	'config'=>OPTION_REQUIRES_VALUE,
	'variables'=>OPTION_REQUIRES_KEY_VALUE,
	'host'=>OPTION_REQUIRES_VALUE,
	'statement'=>OPTION_REQUIRES_VALUE | OPTION_LIST_VALUE
];
$short_options = [
	'h'=>'help',
	'e'=>'execute',
	'f'=>'force',
	'p'=>'password',
	'u'=>'user',
	'v'=>'verbose',
	'd'=>'database',
	'c'=>'config',
	'w'=>'variables',
	'h'=>'host',
	's'=>'statement'
];

$options = parse_options();

if(isset($options['help']) && $options['help']) help();

define('TEST_RUN',isset($options['test']) && $options['test']);

if(TEST_RUN) echo "Options:\n".json_encode($options)."\n";

if(isset($options['verbose'])
	&& $options['verbose']
	&& isset($options['config'])){
	echo "Configuration file: $options[config]\n";
}
if(isset($options['config'])){
	$config = load_config($options['config']);
	$configdir = dirname(realpath($options['config']));
} else {
	$config = [];
	$configdir = '.';
}

foreach($long_options as $name => $type){
	if(isset($options[$name])) $config[$name] = $options[$name];
	elseif(!isset($config[$name])) $config[$name] = false;
}
if(isset($options['files'])) $config['files'] = $options['files'];
if(empty($config['files']) && empty($config['batch'])) help(); //help exits
define('VERBOSE',$config['verbose']);
if(TEST_RUN) echo "Config:\n".json_encode($config)."\n";
if($config['user']===false){
	$config['user'] = get_current_user();
}
if($config['password'] === true) $config['password'] = ask_for_password();

run_config($config, $configdir);
exit;

function help(){
echo <<<'HELP'
Usage:
php dbtool.php [OPTIONS] SCHEMAFILE [SCHEMAFILE...]
php dbtool.php [OPTIONS] --config=CONFIGFILE

General Options:
  -h, --help                          Displays this help text.
  -cVALUE, --config=VALUE             Loads a config file.
  -dVALUE, --database=VALUE           An execution will use the given database,
                                       if a database isn't specified in the
                                       schemafile.
  -e, --execute                       Run the generated SQL to align the
                                       database with the provided schema.
  -f, --force                         Combined with -e: Run any SQL without
                                       asking first.
  -sVALUE, --statement=VALUE          A comma separated subset of
                                         ALTER,CREATE,DROP,GRANT,REVOKE
                                       Enabling a mode allows such statements.
  -p[VALUE], --password[=VALUE]       Use given password or if not set, request
                                       password before connecting to the
                                       database.
  -uVALUE, --user=VALUE               Use the given username when connecting to
                                       the database.
  -v, --verbose                       Write extra descriptive output.
  -wKEY=VALUE, --variables KEY=VALUE  Define a variable to be inserted in the
                                       schema.
  --host=VALUE                        The database host to connect to.
  --test                              Run everything as usual, but without
                                      executing any SQL.

HELP;
exit;
}

function load_config($path){
	list($json,$error) = Core::load_file($path);
	if($error == 'invalid_path') fail("Failed to load configfile: $path",65);
	elseif(isset($error)) fail("Error parsing configfile as JSON: $error",66);
	return $json;
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

function run_config($config, $configdir){
	list($objs, $error) = Core::load_and_run($config, $configdir);
	if($error == 'login_error') fail("Connection Error: Username or password incorrect", 2);

	$successful_results = [];
	$error_printed = false;
	$num = 0;
	foreach($objs as $obj){
		$num++;
		$batch_name = count($objs) > 1 ? "batch #$num" : 'config';

		if(!empty($obj->warnings)){
			foreach($obj->warnings as $warning){
				echo "Warning in $batch_name: $warning\n";
			}
		}
		if(!empty($obj->error)){
			echo "Error in $batch_name: $obj->error\n";
			$error_printed = true;
			continue;
		}
		$result = $obj->get_result();
		if(!show_error($result)) $successful_results[] = [$result,$obj,Config::get_instance(),$num];
		else $error_printed = true;
	}

	if($error_printed && !empty($successful_results)) echo str_repeat('=', 30)."\nProceeding with non-erroring batches.\n\n";

	foreach($successful_results as $pair){
		list($result,$obj,$batch_config,$batch_number) = $pair;
		Config::set_instance($batch_config);
		$batch_msg = "Batch $batch_number";
		$db = Config::get('database');
		if(!empty($db)){
			$batch_msg .= ", using database `$db`";
		}
		echo box($batch_msg);
		$changes = show_result($result);
		$database = Config::get('database');
		if(TEST_RUN) echo "[Test run, skipping execution]\n";
		elseif($changes && $config['execute'] && ($config['force'] || ask_continue("Do you want to execute changes".(empty($database)?'?':" on database `$database`?")))){
			$lines = $obj->execute();
			if(VERBOSE){
				echo "Execution:\n";
				if($lines){
					$messages = DB::get_messages();
					if(empty($messages)){
						$line_count = count($lines);
						echo "$line_count SQL lines executed without errors.\n";
					} else {
						echo "The following errors was encountered:\n";
						foreach($messages as $msg) echo "$msg[text]\n";
					}
				} else {
					echo "No SQL lines were executed.\n";
				}
			}
		}
	}
}

function show_result($result){
	global $config;

	$intersection_tables = [];
	$file_tables = [];
	$db_only_tables = [];
	$drop_queries = [];
	$create_users = [];
	$alter_users = [];
	$drop_users = [];

	foreach($result as $table){
		if(empty($table['sql'])) continue;
		if($table['type'] == 'intersection') $intersection_tables[] = $table;
		elseif($table['type'] == 'file_only') $file_tables[] = $table;
		elseif($table['type'] == 'database_only') $intersection_tables[] = $table;
		elseif($table['type'] == 'drop'){
			$db_only_tables[] = $table['name'];
			$drop_queries = array_merge($drop_queries,$table['sql']);
		} elseif($table['type'] == 'create_user'){
			$create_users[] = $table;
		} elseif($table['type'] == 'alter_user'){
			$alter_users[] = $table;
		} elseif($table['type'] == 'drop_user'){
			$drop_users[] = $table;
		}
	}

	$no_user_drop = show_result_tablelist($drop_users, 'user(s) in database','drop queries will remove them');
	$no_user_create = show_result_tablelist($create_users, 'user(s) in file(s) only','create queries will add them');
	$no_user_alter = show_result_tablelist($alter_users, 'user(s) with differences','queries will align them');
	$no_db = show_result_part($db_only_tables, $drop_queries, 'table(s) in database', 'drop queries will remove them');
	$no_file = show_result_tablelist($file_tables, 'table(s) in file(s) only', 'create queries will add them', ['Format','prettify_create_table']);
	$no_intersect = show_result_tablelist($intersection_tables, 'table(s) with differences', 'queries will align them');

	if($no_user_create && $no_user_alter && $no_user_drop && $no_db && $no_file && $no_intersect){
		echo "No differences found.\n\n";
		return false;
	}
	return true;
}

function show_result_tablelist($tables, $descriptor, $sql_text, $sql_format = null){
	$tablenames = array_map(function($t){return $t['name'];}, $tables);
	$sql = array_map(function($t){return $t['sql'];}, $tables);
	if(!empty($sql)){
		$sql = array_merge(...$sql);
		if(isset($sql_format)) $sql = array_map($sql_format, $sql);
	}
	return show_result_part($tablenames, $sql, $descriptor, $sql_text);
}

function show_result_part($tablenames, $sql, $descriptor, $sql_text){
	if(empty($tablenames)) return true;

	$count = count($tablenames);
	echo "Found $count $descriptor:\n\t";
	echo implode(', ',$tablenames)."\n\n";
	if(VERBOSE){
		echo "The following $sql_text:\n\t";
		echo implode("\n\t",explode("\n",implode("\n",$sql)))."\n\n";
	}
}

function show_error($result){
	$has_errors = false;
	foreach($result as $entry){
		if($entry['type'] != 'error'){
			continue;
			$has_errors = true;
			echo "Error ($e[errno]): ";
			if(isset($entry['error'])) echo $entry['error'];
			else {
				echo "Unknown Error\n";
				debug_print_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS);
			}
			echo "\n";
			if(isset($entry['sqlerror']))echo $entry['sqlerror']."\n";
		}
	}
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

function box($msg){
	if(!VERBOSE) return '# '.$msg." #\n";
	$len = strlen($msg);
	$line = '#'.str_repeat('-',$len)."#\n";
	return $line.'|'.$msg."|\n".$line;
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
				if(!($type & OPTION_TAKES_VALUE) && isset($opt[1])) fail("Option \"$opt[0]\" can't take a value",34);
				if($type & OPTION_NOT_VOID && !isset($opt[1])) fail("Option \"$opt[0]\" requires a value.",35);
				if($type & OPTION_REQUIRES_KEY_VALUE){
					if(!isset($argv[$index+1])) fail("Option \"$opt[0]\" requires a key=value pair.",36);
					$pair = explode('=',$argv[$index+1]);
					if($pair[0][0]=='-') fail("Option \"$opt[0]\": Key must not start with a dash. Is \"$pair[0]\" a separate option?",37);
					if(!isset($pair[1])) fail("Option \"$opt[0]\" requires key \"$pair[0]\" to have a value.",38);
					if(!isset($options[$opt[0]])) $options[$opt[0]] = [];
					$options[$opt[0]][$pair[0]] = $pair[1];
					$index++;
				} elseif($type & OPTION_LIST_VALUE){
					$options[$opt[0]] = isset($opt[1]) ? explode(',',$opt[1]) : [];
				} else {
					$options[$opt[0]] = isset($opt[1]) ? $opt[1] : true;
				}
			} else {
				// short form option
				$i = 1;
				while(isset($option[$i])){
					if(!isset($short_options[$option[$i]])) fail("Unknown Flag ($option[$i])",39);
					$name = $short_options[$option[$i]];
					if(!isset($long_options[$name])) fail("Internal Error: Option \"$name\" ($option[$i]) not implemented correctly",1025);
					if($long_options[$name] & (OPTION_TAKES_VALUE | OPTION_REQUIRES_KEY_VALUE)){
						// assume the rest of the string is the given value
						$value = substr($option,$i+1);
						if($long_options[$name] & OPTION_REQUIRES_KEY_VALUE){
							if($value === false) fail("Option \"$name\" ($option[$i]) requires a key=value pair.",40);
							$pair = explode('=',$value);
							if(!isset($pair[1])) fail("Option \"$name\" ($option[$i]) requires key \"$pair[0]\" to have a value.",41);
							$value = isset($option[$name]) && is_array($options[$name]) ? array_merge($options[$name],[$pair[0]=>$pair[1]]) : [$pair[0]=>$pair[1]];
						} elseif($value === false || $value === ''){
							if($long_options[$name] & ~OPTION_NOT_VOID) $value = $long_options[$name] & OPTION_LIST_VALUE ? [] : true;
							else fail("Option \"$name\" ($option[$i]) requires a value.",42);
						} elseif($long_options[$name] & OPTION_LIST_VALUE){
							$value = explode(',',$value);
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
			if(!isset($options['files'])) $options['files'] = [];
			$options['files'][] = $option;
		}
		$index++;
	}
	return $options;
}

function fail($msg, $errno = 1){
	echo $msg."\n";
	exit($errno);
}
