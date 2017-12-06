<?php

class HTML {
	public static $alt_elem;

	public static function table($parent, $data, $columns = [], $title = null){
		if(empty($columns)){
			foreach($data as $row){
				foreach(array_keys($row['data']) as $key){
					if(!isset($columns[$key])){
						$columns[$key] = ucwords(str_replace('_', ' ', $key));
					}
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

	public static function itemize($parent, $array, $header = null){
		if(!isset($array)) return;
		if(is_string($array)) $array = [$array];
		$list = $parent->el('ul',['class'=>'list-group mb-3']);
		if(isset($header)) $list->el('li',['class'=>'list-group-item list-group-item-info'])->te($header);
		foreach($array as $item){
			$list->el('li',['class'=>'list-group-item'])->te($item);
		}
	}

}
?>