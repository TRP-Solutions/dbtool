<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/

declare(strict_types=1);
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
				$sources = Source::from($sources, $options->read('source'));
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
		$source_empty = true;
		foreach($sources as $source){
			if(!empty($source->warnings)){
				$this->warnings = $source->warnings;
			}
			if($source->error){
				$this->error = $source->error;
			}
			if(!$source->is_empty()){
				$source_empty = false;
			}
		}
		if($source_empty){
			$this->error = "Sources are empty";
			$this->result = [];
		} else {
			$this->result = Tablediff::run($sources);
		}
	}

	public function execute(){
		$this->pre_execute();
		foreach($this->result as $entry){
			if($entry['type'] == 'intersection'
				|| $entry['type'] == 'file_only'
				|| $entry['type'] == 'database_only'
				|| $entry['type'] == 'drop'
				|| $entry['type'] == 'create_user'
				|| $entry['type'] == 'alter_user'
				|| $entry['type'] == 'drop_user'
			){
				$this->execute_entry($entry);
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
				$this->execute_entry($entry);
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
			$this->execute_entry($entry);
		}
		return $this->executed_sql;
	}

	public function execute_drop_user($username){
		$this->pre_execute();
		foreach($this->result as $entry){
			if($entry['type'] != 'drop_user' || $entry['name'] != $username){
				continue;
			}
			$this->execute_entry($entry);
			break;
		}
		return $this->executed_sql;
	}

	public function execute_create_user($username){
		$this->pre_execute();
		foreach($this->result as $entry){
			if($entry['type'] != 'create_user' || $entry['name'] != $username){
				continue;
			}
			$this->execute_entry($entry);
			break;
		}
		return $this->executed_sql;
	}

	public function execute_alter_user($username){
		$this->pre_execute();
		foreach($this->result as $entry){
			if($entry['type'] != 'alter_user' || $entry['name'] != $username){
				continue;
			}
			$this->execute_entry($entry);
			break;
		}
		return $this->executed_sql;
	}

	public function execute_create_database(){
		$this->pre_execute();
		return $this->executed_sql;
	}

	private function execute_entry($entry){
		foreach($entry['sql'] as $sql){
			$result = DB::sql($sql);
			if($result !== false) $this->executed_sql[] = $sql;
		}
	}

	private function pre_execute(){
		Config::set_instance($this->config);
		foreach($this->result as $entry){
			if($entry['type'] != 'create_database'){
				continue;
			}
			foreach($entry['sql'] as $sql){
				DB::sql($sql);
				$this->executed_sql[] = $sql;
			}
		}
		DB::use_configured();
	}

	public function get_result(){
		Config::set_instance($this->config);
		return $this->result;
	}
}
