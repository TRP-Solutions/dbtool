<?php
require_once "../core/classes/core.php";
require_once "lib/heal-document/HealHTML.php";
require_once "classes/html.php";
require_once "classes/diffview.php";
require_once "config.php";

session_start();

if(isset($_POST['dbusername']) && isset($_POST['dbpassword'])){
	$_SESSION['dbusername'] = $_POST['dbusername'];
	$_SESSION['dbpassword'] = $_POST['dbpassword'];
}
if(isset($_GET['dbdisconnect'])){
	unset($_SESSION['dbusername']);
	unset($_SESSION['dbpassword']);
}


$output = new HealHTML();
list($head,$body) = $output->html('DBTool');
$head->css('lib/bootstrap-4.1.0-dist/bootstrap.css');

$nav = $body
	->el('header',['class'=>'navbar navbar-dark bg-dark mb-3'])
	->el('div',['class'=>'container']);
$nav->el('a',['class'=>'navbar-brand','href'=>'.'])->te('DBTool');
function navlink($href, $text, $active = false) {
	global $navbar, $nav;
	if(!isset($navbar)) $navbar = $nav->el('ul',['class'=>'navbar-nav']);
	$navbar->el('a',['class'=>'nav-item nav-link'.($active?' active':''),'href'=>$href])->te($text);
}
if(!isset($_SESSION['dbusername'])) {
	navlink('?dbconnect','Connect to Database', isset($_GET['dbconnect']));
} else {
	$username = htmlentities($_SESSION['dbusername']);
	$nav->el('div',['class'=>'navbar-text'])->te("DB User: [$username]");
	navlink('?dbdisconnect','Disconnect from Database');
}

$header = $body->el('div',['class'=>'container']);
$main = $body->el('div',['class'=>'container']);

if(isset($_GET['dbconnect'])){
	form_connect($main);
} else {
	if(!defined('SCHEMAPATH')) define('SCHEMAPATH','.');
	$schemapaths = explode(':',SCHEMAPATH);
	$files = [];
	foreach($schemapaths as $path){
		$path = realpath($path);
		$files = array_merge($files,glob($path.'/*.json'));
	}
	$filenames = array_map('basename', $files);
	$name_to_file = array_combine($filenames,$files);
	$actions = [];
	foreach($name_to_file as $filename => $file){
		$config = json_decode(file_get_contents($file), true);
		$actions[$filename] = isset($config['name']) ? $config['name']." [$filename]" : $filename;
	}

	$form = $header->form('.')->at(['class'=>'mb-3']);
	$form->label('Config file','configselect');
	$group = $form->el('div',['class'=>'input-group']);
	$group->el('span',['class'=>'input-group-btn'])->el('button',['class'=>'btn btn-primary','onclick'=>'form.submit()'])->te('Submit');
	$select = $group->select('config')->at(['class'=>'form-control','id'=>'configselect']);
	$select->options($actions,isset($_GET['config'])?$_GET['config']:'');

	if(isset($_GET['config']) && isset($name_to_file[$_GET['config']])){
		make_body($main,$header,$name_to_file[$_GET['config']]);
	}
}

echo $output;

function make_body($body,$header,$path){
	$confdiv = $body->el('div');
	$path = realpath($path);
	$basedir = dirname($path);
	list($json,$error) = Core::load_file($path);
	if(isset($_SESSION['dbusername'])){
		$overwritten_user = !isset($json['user']) || $json['user'] == $_SESSION['dbusername'] ? null : $json['user'];
		$overwritten_password = !isset($json['password']) || $json['password'] == $_SESSION['dbpassword'] ? null : $json['password'];
		$json['user'] = $_SESSION['dbusername'];
		$json['password'] = empty($_SESSION['dbpassword']) ? null : $_SESSION['dbpassword'];
	}
	$login_error = Core::load_json($json, $basedir);
	if(isset($login_error)){
		if(!isset($_SESSION['dbusername'])){
			form_connect($confdiv, $json['user']);
		} else {
			$msg = $confdiv->el('div',['class'=>'alert alert-danger','role'=>'alert']);
			$msg->te("Connection Error: Could not connect to database as ");
			$msg->el('em')->te($json['user']);
			$msg->te(!isset($json['password'])?' with no password.':' using a password.');
			if(isset($overwritten_user)){
				$submsg = $confdiv->el('div',['class'=>'alert alert-info','role'=>'alert']);
				$submsg->te(" The configuration suggests the username ")->el('em')->te($overwritten_user);
				$submsg->te('.');
			}
			if(isset($overwritten_password)){
				$submsg = $confdiv->el('div',['class'=>'alert alert-info','role'=>'alert']);
				$submsg->te(" You have supplied a different password than what is specified in the configuration. Disconnecting from the database (in the navigation bar) might help.");
			}
		}
		return;
	}
	$result = Core::run();

	if(isset($result['result']['error'])){
		$res = $result['result'];
		$confdiv->p("Error ($res[errno]): ".$res['error'].(isset($res['sqlerror']) ? ' '.$res['sqlerror'] : ''));
	} else {
		if(isset($_GET['execute'])){
			$result['obj']->execute();
			$result['action'] .= '_execute';
		}

		$schemas = Config::get('schema');
		if(isset($schemas)){
			if(is_string($schemas)) $schemas = [$schemas];
			$schemas = array_map(function($s){return preg_replace("|^[./]+|",'',$s);}, $schemas);
			HTML::itemize($body, $schemas, 'Schemas');
		}
		
		$dbmsg = DB::get_messages();
		if(!empty($dbmsg)){
			$msgs = array_map(function($msg){return $msg['text'];}, $dbmsg);
			HTML::itemize($body, $msgs, 'DB Messages');
		}
		if($result['action']=='permission'){
			$cols = ['location' => 'Location', 'priv_types' => 'Priv Types', 'database' => 'Database', 'table' => 'Table', 'user' => 'User'];
			foreach($result['result']['files'] as $file){
				if(!empty($file['data'])){
					HTML::table($body, $file['data'], $cols, $file['title']);
				}
			}
			$header->el('a',['href'=>'.?config='.$_GET['config'].'&execute','class'=>'btn btn-warning mb-3'])->te('Execute');
		} elseif($result['action']=='permission_execute'){
			$card = $body->el('div',['class'=>'card']);
			$alert = $card->el('div',['class'=>'card-header text-light bg-success h3']);
			$lines = 0;
			$pre = $card->el('pre',['class'=>'card-body text-light bg-dark m-0']);
			foreach($result['result']['files'] as $file){
				foreach($file['sql'] as $sql){
					$pre->te($sql."\n");
					$lines++;
				}
			}
			$alert->te("Executed $lines SQL statements");
		} elseif($result['action']=='diff'){
			$diffs_found = DiffView::build($body, $result['result']);
			if($diffs_found) $header->el('a',['href'=>'.?config='.$_GET['config'].'&execute','class'=>'btn btn-warning mb-3'])->te('Execute');
		} elseif($result['action']=='diff_execute'){
			$body->p('The following SQL was executed:');
			DiffView::build($body, $result['result'], true);
		} else {
			$body->el('pre')->te(json_encode($result));
		}
		$result = json_encode($result);
		$body->el('script')->te("console.log($result)");
	}
}

function form_connect($parent, $suggested_username = null){
	$params = [];
	foreach($_GET as $key => $value){
		if($key == 'dbconnect') continue;
		$params[$key] = $value;
	}
	$url = empty($params) ? '.' : '?'.http_build_query($params);
	$form = $parent->form($url, 'post')->at(['class'=>'mb-3']);
	$group = $form->el('div',['class'=>'form-group']);
	$group->label('Database Username','dbusername');
	$group->input('dbusername', $suggested_username)->at(['class'=>'form-control']);
	$group = $form->el('div',['class'=>'form-group']);
	$group->label('Database Password','dbpassword');
	$group->password('dbpassword')->at(['class'=>'form-control']);
	$form->el('button',['class'=>'btn btn-primary','type'=>'submit'])->te('Connect');
}

?>