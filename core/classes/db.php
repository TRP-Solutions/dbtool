<?php
require_once __DIR__.'/config.php';
require_once __DIR__.'/core.php';

class DB {
	private static $instance;
	public static $isloggedin = false;
	private static $messages = [];
	private static $db_in_use;

	public static function login(){
		if(!isset(self::$instance)){
			self::$instance = new DB();
		}
		return self::$isloggedin;
	}

	public static function logout(){
		session_destroy();
		session_start();
		self::$isloggedin = false;
	}

	public static function get(){
		if(!isset(self::$instance)){
			self::$instance = new DB();
		}
		return self::$instance->mysqli;
	}

	public static function get_dbs(){
		$sql = "SHOW DATABASES WHERE `Database` != 'information_schema' AND `Database` != 'mysql' AND `Database` != 'performance_schema' AND `Database` != 'phpmyadmin' AND SUBSTR(`Database` FROM 1 FOR 9) != 'diff_php_'";
		$result = self::sql($sql);
		$databases = [];
		foreach($result as $r){
			$databases[] = $r['Database'];
		}
		return $databases;
	}

	public static function escape($string){
		return self::get()->escape_string($string);
	}

	public static function sql($sql){
		if(is_array($sql)){
			return array_map(['DB','sql'], $sql);
		}
		$mysqli = self::get();
		$result = $mysqli->query($sql);
		if(!$result){
			self::msg('error', 'SQL error: '.$mysqli->error.' - Query: '.json_encode($sql));
		}
		return $result;
	}

	public static function prepare($stmt){
		$mysqli = self::get();
		$result = $mysqli->prepare($stmt);
		if(!$result){
			self::msg('error', 'SQL error: '.$mysqli->error);
		}
		return $result;
	}

	public static function use_configured(){
		$db = Config::get('database');
		if((!isset($db_in_use) || $this->$db_in_use != $db) && !empty($db)){
			$dbname = self::escape($db);
			self::sql("USE `$dbname`;");
		}
	}

	public static function get_messages(){
		return self::$messages;
	}

	private static function msg($class, $text) {
		self::$messages[] = ['class'=>$class,'text'=>$text];
	}

	private $mysqli;

	private function __construct(){
		$host = Config::get('host');
		if(!isset($host)) $host = 'localhost';
		$username = Config::get('user');
		$password = Config::get('password');
		$database = Config::get('database');
		if(!isset($database)) $database = '';
		if(isset($username) && isset($password)){
			$error_level = error_reporting(0);
			$mysqli = new mysqli($host, $username, $password, $database);
			error_reporting($error_level);
			
			if(!$mysqli->connect_error){
				self::$isloggedin = true;
			}
			$this->mysqli = $mysqli;
		}
	}
}?>
