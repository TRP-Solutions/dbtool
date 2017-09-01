<?php
class Config {
	private static $instance;
	const ALLOWED_KEYS = ['db_username','db_password','variables','action','schema'];
	const STORE_IN_SESSION = ['db_username','db_password'];
	const READ_FROM_INPUT = ['db_u'=>'db_username','db_p'=>'db_password'];

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
		foreach(self::READ_FROM_INPUT as $inputkey => $key){
			if(!isset($json[$key])){
				if(isset($_POST[$inputkey])) $json[$key] = $_POST[$inputkey];
				if(isset($_GET[$inputkey])) $json[$key] = $_GET[$inputkey];
			}
		}
		foreach(self::STORE_IN_SESSION as $key){
			if(isset($json[$key])) $_SESSION['config_'.$key] = $json[$key];
			elseif(isset($_SESSION['config_'.$key])) $json[$key] = $_SESSION['config_'.$key];
		}
		$this->json = $json;
	}
}

?>