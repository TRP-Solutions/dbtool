<?php

class HTML {
	public static $alt_elem;

	public static function table($parent, $data, $columns = []){
		$table = $parent->el('table');
		$headrow = $table->el('tr');
		if(empty($columns)){
			foreach($data as $row){
				foreach(array_keys($row['data']) as $key){
					if(!isset($columns[$key])){
						$columns[$key] = ucwords(str_replace('_', ' ', $key));
					}
				}
			}
		}
		foreach($columns as $title){
			$headrow->el('th')->te($title);
		}
		$columns = array_keys($columns);
		foreach($data as $row){
			$tr = $table->el('tr');
			if(isset($row['class'])){
				$tr->at('class',$row['class']);
			}
			foreach($columns as $key){
				$cell = $tr->el('td');
				if(isset($row['data'][$key])) $cell->te($row['data'][$key]);
			}
		}
		return $table;
	}

	public static function head($parent, $name, $headname, $text){
		$root = $parent->el($name);
		$head = $root->el($headname);
		$head->at('class','header');
		$head->te($text);
		return $root;
	}

	public static function itemize($parent, $array){
		if(!isset($array)) return;
		if(is_string($array)) $array = [$array];
		foreach($array as $item){
			$parent->el('li')->te($item);
		}
	}

	public static function tab_head($parent, $name, $id, $headname, $text, $default_tab, $bparent_name = ''){
		$head = HTML::head($parent, $name, $headname, $text);
		$head->at('id',$id);
		if(!empty($bparent_name)){
			$bparent = $head->el($bparent_name);
			self::$alt_elem = $bparent;
		} else {
			$bparent = $head;
		}
		$button = $bparent->el('button');
		$button->at('class','toggle_tabs');
		$button->at('data-prefix','Show');
		$button->te('Show '.$default_tab);
		$bparent->el('script')->te("init_tabs('$id')");
		return $head;
	}

	public static function tab($parent, $name, $tabname, $selected = false){
		$tab = $parent->el($name);
		$tab->at('data-tab-name',$tabname);
		$class = 'tab';
		if($selected){
			$class .= ' selected';
		}
		$tab->at('class',$class);
		return $tab;
	}
}
?>