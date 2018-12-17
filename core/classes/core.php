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
		DB::use_configured();
		foreach($this->result['tables'] as $table){
			$this->exec_alter_create($table,$options);
		}
		$this->exec_drop($options);
		return $this->executed_sql;
	}

	public function execute_table($tablename, $options = 0b111){
		Config::set_instance($this->config);
		DB::use_configured();
		foreach($this->result['tables'] as $table){
			if($table['name']==$tablename){
				$this->exec_alter_create($table, $options);
				break;
			}
		}
		if(isset($this->result['tables'][$tablename])){
			
		}
		return $this->executed_sql;
	}

	public function execute_drop($options = 0b111){
		Config::set_instance($this->config);
		DB::use_configured();
		$this->exec_drop($options);
		return $this->executed_sql;
	}

	private function exec_alter_create($table, $options){
		if($table['type']=='intersection' && $options & self::ALTER
			|| $table['type']=='file_only' && $options & self::CREATE){
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
			$path = $file[0]=='/' ? $file : self::$configdir.'/'.$file;
			$sqlfile = new SQLFile($path, $vars);
			if($sqlfile->exists) $sqlfiles[] = $sqlfile;
		}
		return $sqlfiles;
	}
}
?>