<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/
require_once __DIR__.'/config.php';
require_once __DIR__.'/tablediff.php';
require_once __DIR__.'/source.php';

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
		foreach($this->result['tables'] as $table){
			foreach($table['sql'] as $sql){
				DB::sql($sql);
				$this->executed_sql[] = $sql;
			}
		}
		foreach($this->result['drop_queries'] as $sql){
			DB::sql($sql);
			$this->executed_sql[] = $sql;
		}
		return $this->executed_sql;
	}

	public function execute_table($tablename){
		$this->pre_execute();
		if(isset($this->result['tables'][$tablename])){
			foreach($this->result['tables'][$tablename]['sql'] as $sql){
				DB::sql($sql);
				$this->executed_sql[] = $sql;
			}
		}
		return $this->executed_sql;
	}

	public function execute_drop(){
		$this->pre_execute();
		foreach($this->result['drop_queries'] as $sql){
			DB::sql($sql);
			$this->executed_sql[] = $sql;
		}
		return $this->executed_sql;
	}

	private function pre_execute(){
		Config::set_instance($this->config);
		if(isset($this->result['create_database'])){
			DB::sql($this->result['create_database']);
			$this->executed_sql[] = $this->result['create_database'];
		}
		DB::use_configured();
	}

	public function get_result(){
		Config::set_instance($this->config);
		return $this->result;
	}
}