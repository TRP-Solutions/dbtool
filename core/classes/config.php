<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/
class Config {
	private static $instance;
	const ALLOWED_KEYS = ['host','user','password','variables','action','files','database','allow_unknown_permissions','allow_unknown_users'];
	const FORCE_ARRAY = ['files'];
	const STORE_IN_SESSION = [];

	public static function load($json){
		if(is_string($json)) $json = json_decode($json);
		if(!is_array($json)) return;
		self::$instance = new Config($json);
	}

	public static function get_instance(){
		if(!isset(self::$instance)) self::$instance = new Config([]);
		return self::$instance;
	}

	public static function set_instance($instance){
		if(is_a($instance, 'Config')){
			self::$instance = $instance;
		}
	}

	public static function get($key){
		$instance = self::get_instance();
		return $instance->read($key);
	}

	public static function debug(){
		debug(self::$instance->json);
	}

	private $json;

	private function __construct($json){
		foreach(self::STORE_IN_SESSION as $key){
			if(isset($json[$key])) $_SESSION['config_'.$key] = $json[$key];
			elseif(isset($_SESSION['config_'.$key])) $json[$key] = $_SESSION['config_'.$key];
		}
		$this->json = $json;
	}

	public function read($key){
		if(isset($this->json[$key]) && in_array($key, self::ALLOWED_KEYS)){
			return !is_array($this->json[$key]) && in_array($key, self::FORCE_ARRAY) ? [$this->json[$key]] : $this->json[$key];
		} elseif(in_array($key, self::FORCE_ARRAY)){
			return [];
		}
	}
}

?>