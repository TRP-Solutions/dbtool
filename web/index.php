<?php
require_once "../core/classes/core.php";
require_once "../core/classes/format.php";
require_once "lib/heal-document/HealHTML.php";
require_once "config.php";
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
}

if(isset($_GET['dbconnect'])){
	Page::login();
	abort();
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
		Page::error($error);
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
	foreach($objs as $obj){
		display_result($obj);
	}
}

function prepare_login(&$json){
	if(isset($_SESSION['dbusername'])){
		$json['overwritten_user'] = !isset($json['user']) || $json['user'] == $_SESSION['dbusername'] ? null : $json['user'];
		$json['overwritten_password'] = !isset($json['password']) || $json['password'] == $_SESSION['dbpassword'] ? null : $json['password'];
		$json['user'] = $_SESSION['dbusername'];
		$json['password'] = empty($_SESSION['dbpassword']) ? null : $_SESSION['dbpassword'];
	} else {
		Page::login($json['user']);
		abort();
	}
}

function display_result($obj){
	$result = $obj->get_result();

	if(isset($result['error'])){
		$msg = "Error ($result[errno]): ".$result['error'].(isset($result['sqlerror']) ? ' '.$result['sqlerror'] : '');
		Page::error($msg);
		return;
	}

	$is_executed = false;
	if(isset($_POST['execute'])){
		$executed_sql = $obj->execute();
		$is_executed = true;
	} else {
		$schemas = Config::get('schema');
		if(isset($schemas)){
			if(is_string($schemas)) $schemas = [$schemas];
			$schemas = array_map(function($s){return preg_replace("|^[./]+|",'',$s);}, $schemas);
			$db = Config::get('database');
			$title = empty($db) ? "Files" : "Comparing database `$db` with files";
			Page::itemize($schemas, $title);
		}
	}
	
	$dbmsg = DB::get_messages();
	if(!empty($dbmsg)){
		$msgs = array_map(function($msg){return $msg['text'];}, $dbmsg);
		Page::itemize($msgs, 'DB Messages');
	}
	if($is_executed){
		$db = Config::get('database');
		if(!empty($executed_sql)){
			$cards = [[
				'title'=>"Executed ".count($executed_sql)." SQL statements".(empty($db) ? '' : " on database `$db`"),
				'title_class'=>'text-light bg-success',
				'sql'=>$executed_sql
			]];
		} else {
			$cards = blank("No SQL executed");
		}
	} else {
		$cards = Format::diff_to_display($result);
		if(empty($cards)){
			$cards = blank("No differences");
		} else {
			Page::execute_button();
		}
	}
	Page::card(...$cards);
}

function blank($msg){
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
?>