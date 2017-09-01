<?php
require_once __DIR__.'/config.php';
require_once __DIR__.'/core.php';

class DB {
	private static $instance;
	public static $isloggedin = false;
	private static $username;
	private static $database;
	private static $messages = [];

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

	public static function get_username(){
		if(isset($_SESSION['db_u'])){
			return $_SESSION['db_u'];
		}
		return false;
	}

	public static function get_messages(){
		return self::$messages;
	}

	private static function msg($class, $text) {
		self::$messages[] = ['class'=>$class,'text'=>$text];
	}

	private $mysqli;

	private function __construct(){
		$username = Config::get('db_username');
		$password = Config::get('db_password');
		if(isset($username) && isset($password)){
			if(defined('DBHOST')){
				$mysqli = new mysqli(DBHOST, $username, $password);
			} else {
				$mysqli = new mysqli('localhost', $username, $password);
			}
			if($mysqli->connect_error){
				Core::msg('error', "Failed to connect to MySQL: {$mysqli->connect_error}");
			} else {
				self::$isloggedin = true;
				$_SESSION['db_u'] = $username;
				$_SESSION['db_p'] = $password;
			}
			$this->mysqli = $mysqli;
		}
	}
}?>