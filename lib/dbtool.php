<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/
require_once __DIR__.'/config.php';
require_once __DIR__.'/tablediff.php';
require_once __DIR__.'/source.php';
require_once __DIR__.'/db.php';

class DBTool {
	private static $batch_counter = 0;

	public static function load($sources, $options){
		if(!is_a($options, 'Config')){
			Config::load($options);
			$options = Config::get_instance();
		}
		DB::login();
		if(!DB::$isloggedin){
			return self::error("Not logged in");
		}
		if(!Source::is_list_of($sources)){
			if(!is_a($sources, 'Source')){
				$sources = Source::from($sources, $options->get('source'));
			}
			$sources = [$sources];
		}

		return new DBTool($sources, $options);
	}

	public static function error($msg){
		throw new Exception($msg);
	}

	public $error = null, $warnings = [], $batch_number;
	private $result, $config, $executed_sql = [];

	protected function __construct($sources, $config){
		$this->batch_number = self::$batch_counter++;
		$this->config = $config;
		foreach($sources as $source){
			if(!empty($source->warnings)){
				$this->warnings = $source->warnings;
			}
			if($source->error){
				$this->error = $source->error;
			}
		}
		$this->result = Tablediff::run($sources);
	}

	public function execute(){
		$this->pre_execute();
		foreach($this->result as $entry){
			if($entry['type'] == 'intersection'
				|| $entry['type'] == 'file_only'
				|| $entry['type'] == 'database_only'
				|| $entry['type'] == 'drop'
			){
				foreach($entry['sql'] as $sql){
					DB::sql($sql);
					$this->executed_sql[] = $sql;
				}
			}
		}
		return $this->executed_sql;
	}

	public function execute_table($tablename){
		$this->pre_execute();
		foreach($this->result as $entry){
			if(
				($entry['type'] == 'intersection' || $entry['type'] == 'file_only' || $entry['type'] == 'database_only')
				&& $entry['name'] == $tablename
			){
				foreach($entry['sql'] as $sql){
					DB::sql($sql);
					$this->executed_sql[] = $sql;
				}
			}
		}
		return $this->executed_sql;
	}

	public function execute_drop(){
		$this->pre_execute();
		foreach($this->result as $entry){
			if($entry['type'] != 'drop'){
				continue;
			}
			DB::sql($entry['sql']);
			$this->executed_sql[] = $entry['sql'];
		}
		return $this->executed_sql;
	}

	private function pre_execute(){
		Config::set_instance($this->config);
		foreach($this->result as $entry){
			if($entry['type'] == 'create_database'){
				DB::sql($entry['sql']);
				$this->executed_sql[] = $entry['sql'];
			}
		}
		DB::use_configured();
	}

	public function get_result(){
		Config::set_instance($this->config);
		return $this->result;
	}
}