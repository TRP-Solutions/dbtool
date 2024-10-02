<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/
class Page {
	private static $output, $body, $navbar, $control, $control_buttons, $important, $main;
	private static $execute_button_made = false;

	private static function init(){
		if(!isset(self::$output)){
			self::$output = new HealHTML();
			list($head,self::$body) = self::$output->html('DBTool');
			$head->css('lib/bootstrap-4.1.0-dist/bootstrap.css');

			$nav = self::$body
				->el('header',['class'=>'navbar navbar-dark bg-dark mb-3'])
				->el('div',['class'=>'container']);
			$nav->el('a',['class'=>'navbar-brand','href'=>'.'])->te('DBTool');
			self::$navbar = $nav->el('ul',['class'=>'navbar-nav']);

			if(!isset($_SESSION['dbusername'])) {
				self::navlink('?dbconnect','Connect to Database', isset($_GET['dbconnect']));
			} else {
				$username = htmlentities($_SESSION['dbusername']);
				$nav->el('div',['class'=>'navbar-text'])->te("DB User: [$username]");
				self::navlink('?dbdisconnect','Disconnect from Database');
			}

			self::$control = self::$body->el('div',['class'=>'container mb-3']);
			self::$important = self::$body->el('div',['class'=>'container']);
			self::$main = self::$body->el('div',['class'=>'container']);
		}
	}

	private static function navlink($href, $text, $active = false) {
		self::$navbar->el('a',['class'=>'nav-item nav-link'.($active?' active':''),'href'=>$href])->te($text);
	}

	public static function login($suggested_username = null){
		self::init();
		$params = [];
		foreach($_GET as $key => $value){
			if($key == 'dbconnect') continue;
			$params[$key] = $value;
		}
		$url = empty($params) ? '.' : '?'.http_build_query($params);
		$form = self::$important->form($url, 'post')->at(['class'=>'mb-3']);
		$group = $form->el('div',['class'=>'form-group']);
		$group->label('Database Username','dbusername');
		$group->input('dbusername', $suggested_username)->at(['class'=>'form-control']);
		$group = $form->el('div',['class'=>'form-group']);
		$group->label('Database Password','dbpassword');
		$group->password('dbpassword')->at(['class'=>'form-control']);
		$form->el('button',['class'=>'btn btn-primary','type'=>'submit'])->te('Connect');
	}

	public static function error($message, ...$submessages){
		self::init();
		self::error_message(self::$important, $message);
		foreach($submessages as $submessage){
			self::error_message(self::$important, $submessage, 'alert-info');
		}
	}

	public static function config_select($actions, $selected = ''){
		self::init();
		$form = self::$control->form('.')->at(['class'=>'mb-3','id'=>'config_form']);
		$form->label('Config file','configselect');
		$select = $form->select('config')->at(['class'=>'form-control','id'=>'configselect','onchange'=>'form.submit()']);
		$select->options($actions,$selected);
		if(!isset(self::$control_buttons)) self::$control_buttons = self::$control->el('div',['class'=>'btn-group']);
		self::$control_buttons->el('button',['class'=>'btn btn-primary','onclick'=>'config_form.submit()'])->te('Submit');
	}

	public static function execute_button(){
		if(!self::$execute_button_made){
			self::$execute_button_made = true;
			self::init();
			$form = self::$control->form('.?config='.$_GET['config'],'POST')->at(['id'=>'execute_form']);
			$form->hidden('execute','');
			if(!isset(self::$control_buttons)) self::$control_buttons = self::$control->el('div',['class'=>'btn-group']);
			$js = 'if(confirm("Are you sure you want to execute this SQL?")){execute_form.submit()}else{return false}';
			self::$control_buttons->el('button',['onclick'=>$js,'class'=>'btn btn-danger'])->te('Execute');
		}
	}

	public static function card(...$cards){
		self::init();
		foreach($cards as $data){
			if(!empty($data['errors'])){
				foreach($data['errors'] as $error){
					self::error_message(self::$main, $error);
				}
				continue;
			}
			$card = self::$main->el('div',['class'=>'card mb-3']);
			if(isset($data['title'])){
				$header = $card->el('div',['class'=>'card-header']);
				if(isset($data['id']) && $data['id']=='error') $header->at(['class'=>'text-white bg-danger'], HEAL_ATTR_APPEND);
				$header->el('h2',['class'=>'card-title mb-0'])->te($data['title']);
				if(isset($data['title_class'])) $header->at(['class'=>$data['title_class']], HEAL_ATTR_APPEND);
				if(isset($data['subtitle'])){
					$header->el('h6',['class'=>'card-subtitle mt-2'])->te($data['subtitle']);
				}
			}
			if(isset($data['display'])) foreach($data['display'] as $display){
				if(isset($display['table'])){
					self::table($card, $display['table'], $display['title'], ['key','type']);
				}
				if(isset($display['list'])){
					$list = $card->el('ul',['class'=>'list-group list-group-flush']);
					foreach($display['list'] as $item){
						$list->el('li',['class'=>'list-group-item'])->te($item, HEAL_TEXT_NL2BR);
					}
				}
			}
			if(isset($data['sql'])){
				$pre = $card->el('pre',['class'=>'card-body text-light bg-dark mt-3 mb-0 rounded-bottom']);
				foreach($data['sql'] as $sql){
					$pre->te($sql."\n");
				}
			}
			if(isset($data['execute_button'])){
				$batch = $data['execute_button']['batch'];
				$id = $data['execute_button']['id'];
				$form = $card->form('.?config='.$_GET['config'],'POST');
				$form->hidden('execute_part',"$batch:$id");
				$form->el('button',['onclick'=>'this.parentElement.submit()','class'=>'btn btn-danger mt-3'])->te('Execute this part');
			}
		}
	}

	private static function table($parent, $data, $title = null, $ignore_columns = []){
		$columns = [];
		foreach($data as $row){
			foreach($row['data'] as $key => $value){
				if(!in_array($key, $ignore_columns)){
					$columns[$key] = ucwords(str_replace('_', ' ', $key));
				}
			}
		}
		$table = $parent->el('table',['class'=>'table m-0']);
		if(isset($title)){
			$table->el('tr',['class'=>'thead-light'])->el('th',['class'=>'h4','colspan'=>count($columns)])->te($title);
		}
		$headrow = $table->el('tr',['class'=>'thead-light']);
		
		foreach($columns as $title){
			$headrow->el('th')->te($title);
		}
		$columns = array_keys($columns);
		foreach($data as $row){
			$tr = $table->el('tr');
			if(isset($row['class'])){
				$tr->at(['class'=>$row['class']]);
			}
			foreach($columns as $key){
				$cell = $tr->el('td');
				if(isset($row['data'][$key])) $cell->te($row['data'][$key]);
			}
		}
		return $table;
	}

	private static function error_message($parent, $message, $class = 'alert-danger'){
		$parent->el('pre',['class'=>"alert $class",'role'=>'alert'])->te($message, HEAL_TEXT_NL2BR);
	}

	public static function itemize($array, $header = null, $important = false){
		if(!isset($array)) return;
		if(is_string($array)) $array = [$array];
		self::init();
		$parent = $important ? self::$important : self::$main;
		$list = $parent->el('ul',['class'=>'list-group mb-3']);
		if(isset($header)) $list->el('li',['class'=>'list-group-item list-group-item-info'])->te($header);
		foreach($array as $item){
			$list->el('li',['class'=>'list-group-item'])->te($item);
		}
	}

	public static function flush(){
		self::init();
		echo self::$output;
	}
}
