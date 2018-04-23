<?php
class Config {
	private static $instance;
	const ALLOWED_KEYS = ['host','user','password','variables','action','schema','database'];
	const STORE_IN_SESSION = [];

	public static function load($json){
		if(is_string($json)) $json = json_decode($json);
		if(!is_array($json)) return;
		self::$instance = new Config($json);
	}

	private static function get_instance(){
		if(!isset(self::$instance)) self::$instance = new Config([]);
		return self::$instance;
	}

	public static function get($key){
		$instance = self::get_instance();
		if(isset($instance->json[$key]) && in_array($key, self::ALLOWED_KEYS)){
			return $instance->json[$key];
		}
	}

	private $json;

	private function __construct($json){
		foreach(self::STORE_IN_SESSION as $key){
			if(isset($json[$key])) $_SESSION['config_'.$key] = $json[$key];
			elseif(isset($_SESSION['config_'.$key])) $json[$key] = $_SESSION['config_'.$key];
		}
		$this->json = $json;
	}
}

?>