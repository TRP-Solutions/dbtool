<?php
require_once __DIR__.'/config.php';
require_once __DIR__.'/permissiondiff.php';
require_once __DIR__.'/diff.php';
require_once __DIR__.'/sqlfile.php';

class Core {
	const ALTER = 0b100;
	const CREATE = 0b010;
	const DROP = 0b001;

	private static $configdir;

	public static function load_file($path){
		$path = realpath($path);
		if($path){
			$json = json_decode(file_get_contents($path),true);
			if(json_last_error()===JSON_ERROR_NONE) return [$json,null];
			return [null,json_last_error_msg()];
		}
		return [null,'invalid_path'];
	}

	public static function load_and_run($json, $configdir = null){
		if(isset($configdir)){
			self::$configdir = realpath($configdir);
		} elseif(!isset(self::$configdir)){
			self::$configdir = realpath('.');
		}

		Config::load($json);
		DB::login();
		if(!DB::$isloggedin){
			return [[], 'login_error'];
		}

		$objs = [];
		if(!empty($json['batch']) && is_array($json['batch'])){
			foreach($json['batch'] as $action){
				Config::load(array_merge($json, $action));
				$objs[] = new Core();
			}
		} else {
			$objs[] = new Core();
		}

		return [$objs, null];
	}

	public $error = null;
	private $result, $config;
	protected function __construct(){
		$this->config = Config::get_instance();
		DB::login();
		$sqlfiles = self::sqlfiles();
		if(empty($sqlfiles)) $this->error = 'missing_schema';
		$diff = new Diff($sqlfiles);
		$permission = new PermissionDiff($sqlfiles);
		$result = $diff->run();
		$this->result = $permission->run($result);
	}

	public function execute($options = 0b111){
		Config::set_instance($this->config);
		$executed_sql = [];
		DB::use_configured();
		foreach($this->result['tables'] as $table){
			if($table['type']=='intersection' && $options & self::ALTER
				|| $table['type']=='file_only' && $options & self::CREATE){
				foreach($table['sql'] as $sql){
					DB::sql($sql);
					$executed_sql[] = $sql;
				}
			}
		}
		if($options & self::DROP && !empty($result['drop_queries'])){
			foreach($file['drop_queries'] as $sql){
				DB::sql($sql);
				$executed_sql[] = $sql;
			}
		}
		return $executed_sql;
	}

	public function get_result(){
		Config::set_instance($this->config);
		return $this->result;
	}

	private static function sqlfiles(){
		$files = Config::get('files');
		$vars = Config::get('variables');
		$sqlfiles = [];
		foreach($files as $file){
			$path = $file[0]=='/' ? $file : self::$configdir.'/'.$file;
			$sqlfile = new SQLFile($path, $vars);
			if($sqlfile->exists) $sqlfiles[] = $sqlfile;
		}
		return $sqlfiles;
	}
}
?>