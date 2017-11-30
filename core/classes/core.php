<?php
require_once __DIR__.'/config.php';
require_once __DIR__.'/permissiondiff.php';
require_once __DIR__.'/diff.php';
require_once __DIR__.'/sqlfile.php';

class CoreDiff extends Core {
	const ALTER = 0b100;
	const CREATE = 0b010;
	const DROP = 0b001;

	private $dbname;
	protected function __construct(){
		parent::__construct();
		$this->dbname = Config::get('database');
		if(empty($this->schemas)) $this->error = 'missing_schema';
		elseif(empty($this->dbname)) $this->error = 'missing_dbname';
		else {
			$vars = Config::get('variables');
			if(!isset($vars)) $vars = [];
			$diff = new Diff();
			$this->result = $diff->diff_multi($this->dbname, $this->schemas, $vars);
		}
	}

	public function execute($options = 0b111){
		$dbname = DB::escape($this->dbname);
		DB::sql("USE $dbname");
		$lines = 0;
		foreach($this->result['files'] as $file){
			if($options & self::ALTER){
				$lines += count($this->file['alter_queries']);
				foreach($this->file['alter_queries'] as $sql) DB::sql($sql);
			}
			if($options & self::CREATE){
				$lines += count($this->file['create_queries']);
				foreach($this->file['create_queries'] as $sql) DB::sql($sql);
			}
			if($options & self::DROP){
				$lines += count($this->file['drop_queries']);
				foreach($this->file['drop_queries'] as $sql) DB::sql($sql);
			}
		}
		return $lines;
	}
}

class CorePermission extends Core {
	protected function __construct(){
		parent::__construct();
		$vars = Config::get('variables');
		$permission = new PermissionDiff($vars['PRIM'],$vars['SECO'],[],$vars);
		$this->result = $permission->diff_files($this->schemas);
	}

	public function execute($options = 0){
		$lines = 0;
		foreach($this->result['files'] as $file){
			$lines += count($file['sql']);
			foreach($file['sql'] as $sql) DB::sql($sql);
		}
		return $lines;
	}
}

abstract class Core {
	private static $configdir = '';
	public static function load_file($path){
		$path = realpath($path);
		if($path){
			$json = json_decode(file_get_contents($path),true);
			if(json_last_error()===JSON_ERROR_NONE) return [$json,null];
			return [null,json_last_error_msg()];
		}
		return [null,'invalid_path'];
	}

	public static function load_json($json, $configdir = '.'){
		self::$configdir = realpath($configdir);
		Config::load($json);
		DB::login();
		if(!DB::$isloggedin) return 'wrong_credentials';
	}

	public static function run($action = null){
		if(empty($action)) $action = Config::get('action');
		switch($action){
			case 'diff':
				$obj = new CoreDiff();
				break;
			case 'permission':
				$obj = new CorePermission();
				break;
			default:
				$obj = null;
		}
		$result = isset($obj) ? $obj->get_result() : false;
		return ['action'=>$action,'result'=>$result,'obj'=>$obj];
	}

	public $error = null;
	protected $schemas = [];
	protected function __construct(){
		DB::login();
		$this->schemas = self::get_schemalist();
	}

	abstract public function execute($options = 0);

	protected $result;
	public function get_result(){
		return $this->result;
	}

	private static function get_schemalist(){
		$schema = Config::get('schema');
		if(is_string($schema)){
			return [realpath(self::$configdir.'/'.$schema)];
		} elseif(is_array($schema)) {
			$dir = self::$configdir;
			return array_map(function($schema) use ($dir){
				return realpath($dir.'/'.$schema);
			},$schema);
		} else {
			return [$schema];
		}
	}
}
?>