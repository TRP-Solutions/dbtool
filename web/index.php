<?php
require_once "../core/classes/core.php";
require_once "lib/heal-document/HealHTML.php";
require_once "classes/html.php";
require_once "classes/diffview.php";


$output = new HealHTML();
list($head,$body) = $output->html('DBTool');
$head->css('css/main.css');
$head->el('script',['src'=>'js/tabs.js']);

$body->at('class','flex col');

$header = $body->el('header');

$files = array_map(function($path){
	return basename(dirname($path)).'/'.basename($path);
}, glob('../../configfiles/*.json'));
$files = array_map(function($path){
	return basename($path);
}, array_combine($files,$files));
$form = $header->form('index.php');
$form->label('Config file: ')
	->select('config')
	->options($files,isset($_GET['config'])?$_GET['config']:'');
$form->submit();

$confdiv = $body->el('div');

if(isset($_GET['config'])){
	$path = realpath(__DIR__.'/../../'.$_GET['config']);
	$basedir = dirname($path);
	$result = Core::run($path);

	if(isset($_GET['execute'])){
		$result['obj']->execute();
		$result['action'] .= '_execute';
	}

	$confdiv->p('Connected as: '.DB::get_username());
	$confdiv->p('Schemas:');
	$schemas = Config::get('schema');
	if(is_string($schemas)) $schemas = [$schemas];
	$schemas = array_map(function($s)use($basedir){return realpath($basedir.'/'.$s);}, $schemas);
	HTML::itemize($confdiv, $schemas);
	$dbmsg = DB::get_messages();
	if(!empty($dbmsg)){
		$confdiv->p('DB messages:');
		HTML::itemize($confdiv, array_map(function($msg){return $msg['text'];}, $dbmsg));
	}
	if($result['action']=='permission'){
		$cols = ['location' => 'Location', 'priv_types' => 'Priv Types', 'database' => 'Database', 'table' => 'Table', 'user' => 'User'];
		foreach($result['result']['files'] as $file){
			if(!empty($file['data'])){
				$body->el('h1')->te($file['title']);
				HTML::table($body, $file['data'], $cols);
			}
		}
		$js = 'window.location="?config='.$_GET['config'].'&execute"';
		$header->el('button',['onclick'=>$js])->te('Execute');
	} elseif($result['action']=='permission_execute'){
		$h1 = $body->el('h1');
		$lines = 0;
		foreach($result['result']['files'] as $file){
			foreach($file['sql'] as $sql){
				$body->el('pre')->te($sql);
				$lines++;
			}
		}
		$h1->te("Executed $lines lines of SQL");
	} elseif($result['action']=='diff'){
		DiffView::build($body, $result['result']);

		$js = 'window.location="?config='.$_GET['config'].'&execute"';
		$header->el('button',['onclick'=>$js])->te('Execute');
	} elseif($result['action']=='diff_execute'){
		$body->p('The following SQL was executed:');
		DiffView::build($body, $result['result']);
	} else {
		$body->el('pre')->te(json_encode($result));
	}
	$result = json_encode($result);
	$body->el('script')->te("console.log($result)");
}

echo $output;

?>