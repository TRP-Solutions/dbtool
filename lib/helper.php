<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/

namespace Helper;

function sqlfiles($configdir){
	$files = \Config::get('files');
	$vars = \Config::get('variables');
	$sqlfiles = [];
	$errors = [];
	foreach($files as $file){
		$path = realpath($file[0]=='/' ? $file : $configdir.'/'.$file);
		if($path===false || !is_file($path)){
			$errors[] = "Can't read file: ".$file;
			continue;
		}
		$sqlfiles[] = \Source::from(implode("\n",read_lines($path, $vars)), $path);
	}
	return [$sqlfiles, $errors];
}

function include_files($line, $vars, $dir){
	if(substr($line, 0, 9) == '#!include'){
		$filenames = explode(',', substr($line, 10));
		$lines = [];
		foreach($filenames as $filename){
			$filename = trim($filename);
			if($filename[0] != '/'){
				
				$filename = realpath($dir.'/'.$filename);
			}
			$file_lines = file($filename, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
			if(!empty($file_lines)){
				$lines = array_merge($lines, $file_lines);
			}
		}
		return $lines;
	} else {
		return false;
	}
}

function replace_vars($lines, $vars){
	if(!is_array($vars)) return $lines;
	foreach($lines as &$line){
		foreach($vars as $search => $replace){
			$line = str_replace('['.$search.']', $replace, $line);
		}
	}
	return $lines;
}

function read_lines($filename, $vars){
	$file_lines = file($filename, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
	$included = include_files($file_lines[0], $vars, dirname($filename));
	if($included !== false){
		array_shift($file_lines);
		$file_lines = array_merge($included,$file_lines);
	}
	return replace_vars($file_lines, $vars);
}

function split_batches($json){
	if(!isset($json['batch']) || !is_array($json['batch'])){
		return [$json];
	} else {
		$batches = [];
		foreach($json['batch'] as $action){
			$batches[] = array_merge($json, $action);
		}
		return $batches;
	}
}

function share_known_tables($objs){
	$known_tables = [];
	// collect list of known tables
	foreach($objs as $obj){
		$result = $obj->get_result();
		// assumes ->get_result loads the relevant config instance
		$config = \Config::get_instance();
		$db = $config->read('database');
		$tables = array_keys($result['tables']);
		if(!isset($known_tables[$db])){
			$known_tables[$db] = $tables;
		} else {
			$known_tables[$db] = array_merge($known_tables[$db],$tables);
		}
	}
	foreach($known_tables as &$table){
		$table = array_unique($table);
	}
	// remove known tables from drop queries
	foreach($objs as $obj){
		$result = $obj->get_result();
		// assumes ->get_result loads the relevant config instance
		$config = \Config::get_instance();
		$db = $config->read('database');
		$result['db_only_tables'] = array_diff($result['db_only_tables'], $known_tables[$db]);
		$drop = [];
		foreach($result['db_only_tables'] as $key){
			if(isset($result['drop_queries'][$key])){
				$drop[$key] = $result['drop_queries'][$key];
			}
		}
		$result['drop_queries'] = $drop;
	}
}
