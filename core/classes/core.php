<?php
require_once __DIR__.'/config.php';
require_once __DIR__.'/permissiondiff.php';
require_once __DIR__.'/diff.php';
require_once __DIR__.'/sqlfile.php';

class Core {
	const ALTER = 0b100;
	const CREATE = 0b010;
	const DROP = 0b001;

	private static $configdir, $batch_counter = 0;

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
		
		$known_tables = [];
		foreach($objs as $obj){
			$db = $obj->config->read('database');
			$result = $obj->get_result();
			$tables = array_keys($result['tables']);
			if(!isset($known_tables[$db])){
				$known_tables[$db] = $tables;
			} else {
				$known_tables[$db] = array_merge($known_tables[$db],$tables);
			}
		}
		foreach($known_tables as &$table){
			$table = array_unique($table);
		}
		foreach($objs as $obj){
			$db = $obj->config->read('database');
			$obj->result['db_only_tables'] = array_diff($obj->result['db_only_tables'], $known_tables[$db]);
			$drop = [];
			foreach($obj->result['db_only_tables'] as $key){
				if(isset($obj->result['drop_queries'][$key])){
					$drop[$key] = $obj->result['drop_queries'][$key];
				}
			}
			$obj->result['drop_queries'] = $drop;
		}

		return [$objs, null];
	}

	public $error = null, $batch_number;
	private $result, $config, $executed_sql = [];
	protected function __construct(){
		$this->batch_number = self::$batch_counter++;
		$this->config = Config::get_instance();
		DB::login();
		$sqlfiles = self::sqlfiles();
		if(empty($sqlfiles)) $this->error = 'No files found';
		$diff = new Diff($sqlfiles);
		$permission = new PermissionDiff($sqlfiles);
		$result = $diff->run();
		$this->result = $permission->run($result);
	}

	public function execute($options = 0b111){
		Config::set_instance($this->config);
		$this->exec_create_database();
		DB::use_configured();
		foreach($this->result['tables'] as $table){
			$this->exec_alter_create($table,$options);
		}
		$this->exec_drop($options);
		return $this->executed_sql;
	}

	public function execute_table($tablename, $options = 0b111){
		Config::set_instance($this->config);
		$this->exec_create_database();
		DB::use_configured();
		if(isset($this->result['tables'][$tablename])){
			$this->exec_alter_create($this->result['tables'][$tablename], $options);
		}
		return $this->executed_sql;
	}

	public function execute_drop($options = 0b111){
		Config::set_instance($this->config);
		DB::use_configured();
		$this->exec_drop($options);
		return $this->executed_sql;
	}

	public function execute_create_database(){
		Config::set_instance($this->config);
		$this->exec_create_database();
		return $this->executed_sql;
	}

	private function exec_create_database(){
		if(isset($this->result['create_database'])){
			DB::sql($this->result['create_database']);
			$this->executed_sql[] = $this->result['create_database'];
		}
	}

	private function exec_alter_create($table, $options){
		if($table['type']=='intersection' && $options & self::ALTER
			|| $table['type']=='file_only' && $options & self::CREATE
			|| $table['type']=='database_only' && $options & self::DROP){
			foreach($table['sql'] as $sql){
				DB::sql($sql);
				$this->executed_sql[] = $sql;
			}
		}
	}

	private function exec_drop($options){
		if(!empty($this->result['drop_queries']) && $options & self::DROP){
			foreach($this->result['drop_queries'] as $sql){
				DB::sql($sql);
				$this->executed_sql[] = $sql;
			}
		}
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
			$path = realpath($file[0]=='/' ? $file : self::$configdir.'/'.$file);
			if($path===false) continue;
			if(is_dir($path)){
				foreach(glob($path.'/[^_]*.sql') as $filepath){
					$sqlfile = new SQLFile($filepath, $vars);
					if($sqlfile->exists) $sqlfiles[] = $sqlfile;
				}
			} else {
				$sqlfile = new SQLFile($path, $vars);
				if($sqlfile->exists) $sqlfiles[] = $sqlfile;
			}
		}
		return $sqlfiles;
	}
}
?>
