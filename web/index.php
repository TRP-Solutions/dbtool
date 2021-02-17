<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/
require_once "../lib/core.php";
require_once "../lib/format.php";
require_once "lib/heal-document/lib/HealHTML.php";
if(file_exists("config.php")){
	require_once "config.php";
}
require_once "page.php";


function debug(...$data){
	header('Content-type:application/json');
	echo json_encode($data);
	exit;
}

session_start();

if(isset($_POST['dbusername']) && isset($_POST['dbpassword'])){
	$_SESSION['dbusername'] = $_POST['dbusername'];
	$_SESSION['dbpassword'] = $_POST['dbpassword'];
}
if(isset($_GET['dbdisconnect'])){
	unset($_SESSION['dbusername']);
	unset($_SESSION['dbpassword']);
} elseif(isset($_GET['dbconnect'])){
	Page::login();
	abort();
} else if(!isset($_SESSION['dbusername']) && !isset($_SESSION['dbpassword']) && defined('DEFAULT_USER') && defined('DEFAULT_PASS')){
	$_SESSION['dbusername'] = DEFAULT_USER;
	$_SESSION['dbpassword'] = DEFAULT_PASS;
}

if(!defined('SCHEMAPATH')) define('SCHEMAPATH','.');
$schemapaths = explode(':',SCHEMAPATH);
$files = [];
foreach($schemapaths as $path){
	$path = realpath($path);
	$files = array_merge($files,glob($path.'/*.json'));
}
$name_to_file = array_combine(array_map('basename', $files),$files);
$actions = [];
foreach($name_to_file as $filename => $file){
	$config = json_decode(file_get_contents($file), true);
	$actions[$filename] = isset($config['name']) ? $config['name']." [$filename]" : $filename;
}

Page::config_select($actions, isset($_GET['config'])?$_GET['config']:'');

if(isset($_GET['config']) && isset($name_to_file[$_GET['config']])){
	$path = realpath($name_to_file[$_GET['config']]);
	list($config,$error) = Core::load_file($path);
	if(isset($error)){
		Page::error('Loading error: '.$error);
		abort();
	}
	load_and_run($config, dirname($path));
}

Page::flush();

function abort(){
	Page::flush();
	exit;
}

function load_and_run($config, $basedir){
	prepare_login($config);
	list($objs,$error) = Core::load_and_run($config, $basedir);
	if($error == 'login_error'){
		$msg = "Connection Error: Could not connect to database as \"$config[user]\"";
		$msg .= !isset($config['password']) ? ' with no password.' : ' using a password.';
		$msgs = [$msg];
		if(isset($config['overwritten_user'])){
			$msgs[] = "The configuration suggests the username \"$config[overwritten_user]\".";
		}
		if(isset($config['overwritten_password'])){
			$msgs[] = "You have supplied a different password than what is specified in the configuration. Disconnecting from the database (in the navigation bar) might help.";
		}
		Page::error(...$msgs);
		abort();
	}
	if(isset($_POST['execute_part'])){
		$execute = explode(':',$_POST['execute_part']);
		if(count($execute) == 3) $execute_batch = $execute[0];
	}
	$content_emmitted = false;
	foreach($objs as $obj){
		if(isset($execute_batch) && $execute_batch != $obj->batch_number) continue;
		$content_emmitted = display_result($obj) || $content_emmitted;
	}
	if(!$content_emmitted){
		Page::card(...blank("No differences", true));
	}
	$dbmsg = DB::get_messages();
	if(!empty($dbmsg)){
		$msgs = array_map(function($msg){return $msg['text'];}, $dbmsg);
		Page::itemize($msgs, 'DB Messages', true);
	}
}

function prepare_login(&$json){
	if(isset($_SESSION['dbusername'])){
		$json['overwritten_user'] = !isset($json['user']) || $json['user'] == $_SESSION['dbusername'] ? null : $json['user'];
		$json['overwritten_password'] = !isset($json['password']) || $json['password'] == $_SESSION['dbpassword'] ? null : $json['password'];
		$json['user'] = $_SESSION['dbusername'];
		$json['password'] = empty($_SESSION['dbpassword']) ? null : $_SESSION['dbpassword'];
	} else {
		Page::login(isset($json['user'])?$json['user']:'');
		abort();
	}
}
/*
function cache_result($obj){
	$batch_display_number = $obj->batch_number + 1;

	$cache = ['batch_display_number'=>$obj->batch_number + 1];
	if(!empty($obj->warnings)){
		foreach($obj->warnings as $warning){
			$cache['warnings'][] = $warning;
		}
	}

	if($obj->error){
		$cache['error'] = $obj->error;
		return $cache;
	}

	$cache['result'] = $obj->get_result();
	
	if(isset($cache['result']['error'])){
		return $cache;
	}

	if(isset($_POST['execute_part'])){
		$execute = explode(':',$_POST['execute_part']);
		if(count($execute) != 3){
			$display_title();
			Page::error("Invalid <execute_part> value");
			return true;
		}
		if($execute[1]=='table'){
			$executed_sql = $obj->execute_table($execute[2]);
			$is_executed = true;
		} elseif($execute[1]=='sql' && $execute[2]=='drop'){
			$executed_sql = $obj->execute_drop();
			$is_executed = true;
		} elseif($execute[1]=='sql' && $execute[2]=='create_database'){
			$executed_sql = $obj->execute_create_database();
			$is_executed = true;
		}
	} elseif(isset($_POST['execute'])){
		$executed_sql = $obj->execute();
		$is_executed = true;
	} else {
		$schemas = Config::get('schema');
		if(isset($schemas)){
			if(is_string($schemas)) $schemas = [$schemas];
			$schemas = array_map(function($s){return preg_replace("|^[./]+|",'',$s);}, $schemas);
			$db = Config::get('database');
			$title = empty($db) ? "Files" : "Comparing database `$db` with files";
			$display_title();
			Page::itemize($schemas, $title);
		}
	}
	
}
*/
function display_result($obj){
	$batch_display_number = $obj->batch_number + 1;

	if(!empty($obj->warnings)){
		foreach($obj->warnings as $warning){
			Page::error("Batch $batch_display_number: ".$warning);
		}
	}

	if($obj->error){
		Page::error("Error in batch $batch_display_number: $obj->error");
		return true;
	}

	$result = $obj->get_result();

	if(isset($result['error'])){
		$msg = "Error ($result[errno]): ".$result['error'].(isset($result['sqlerror']) ? ' '.$result['sqlerror'] : '');
		Page::error($msg);
		return true;
	}

	$title_is_emitted = false;
	$display_title = function() use (&$title_is_emitted, $batch_display_number){
		if(!$title_is_emitted){
			Page::card(['title'=>'Batch '.$batch_display_number]);
			$title_is_emitted = true;
		}
	};

	$is_executed = false;
	if(isset($_POST['execute_part'])){
		$execute = explode(':',$_POST['execute_part']);
		if(count($execute) != 3){
			$display_title();
			Page::error("Invalid <execute_part> value");
			return true;
		}
		if($execute[1]=='table'){
			$executed_sql = $obj->execute_table($execute[2]);
			$is_executed = true;
		} elseif($execute[1]=='sql' && $execute[2]=='drop'){
			$executed_sql = $obj->execute_drop();
			$is_executed = true;
		} elseif($execute[1]=='sql' && $execute[2]=='create_database'){
			$executed_sql = $obj->execute_create_database();
			$is_executed = true;
		}
	} elseif(isset($_POST['execute'])){
		$executed_sql = $obj->execute();
		$is_executed = true;
	} else {
		$schemas = Config::get('schema');
		if(isset($schemas)){
			if(is_string($schemas)) $schemas = [$schemas];
			$schemas = array_map(function($s){return preg_replace("|^[./]+|",'',$s);}, $schemas);
			$db = Config::get('database');
			$title = empty($db) ? "Files" : "Comparing database `$db` with files";
			$display_title();
			Page::itemize($schemas, $title);
		}
	}
	
	if($is_executed){
		$db = Config::get('database');
		if(!empty($executed_sql)){
			$cards = [[
				'title'=>"Executed ".count($executed_sql)." SQL statements".(empty($db) ? '' : " on database `$db`"),
				'title_class'=>'text-light bg-success',
				'sql'=>$executed_sql
			]];
		} elseif($title_is_emitted) {
			$cards = blank("No SQL executed");
		} else {
			$cards = [];
		}
	} else {
		$cards = Format::diff_to_display($result);
		if(empty($cards)){
			if($title_is_emitted){
				$cards = blank("No differences");
			} else {
				$cards = [];
			}
		} else {
			$db = Config::get('database');
			foreach($cards as &$card){
				if(isset($card['id'])) $card['execute_button'] = ['batch'=>$obj->batch_number,'id'=>$card['id']];
			}
			$display_title();
			Page::execute_button();
		}
	}
	if(!empty($cards)){
		$display_title();
		Page::card(...$cards);
		return true;
	}
	return false;
}

function blank($msg, $exclude_context = false){
	if($exclude_context){
		return [[
			'title_class'=>'alert-success',
			'title'=>$msg
		]];
	}
	$db = Config::get('database');
	if(!empty($db)){
		return [[
			'title_class'=>'alert-success',
			'title'=>"$msg in database `$db`"
		]];
	} else {
		return [[
			'title_class'=>'alert-success',
			'title'=>"$msg in files:",
			'display'=>[
				['list'=>Config::get('files')]
			]
		]];
	}
}
