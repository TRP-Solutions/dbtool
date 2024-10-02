<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/

declare(strict_types=1);
require_once __DIR__.'/config.php';
require_once __DIR__.'/db.php';
require_once __DIR__.'/dbtool.php';
require_once __DIR__.'/helper.php';

class Core {
	public static function load_file($path){
		$path = realpath($path);
		if($path){
			$json = json_decode(file_get_contents($path),true);
			if(json_last_error()===JSON_ERROR_NONE) return [$json,null];
			return [null,json_last_error_msg()];
		}
		return [null,'invalid_path'];
	}

	public static $warnings = [];

	public static function load_and_run($json, $configdir = null){
		Config::load($json);
		DB::login();
		if(!DB::$isloggedin){
			return [[], 'login_error'];
		}

		$configdir = realpath($configdir ?? '.');

		$batches = \Helper\split_batches($json);
		$objs = [];
		foreach($batches as $batch){
			Config::load($batch);
			list($sqlfiles, $warnings) = \Helper\sqlfiles($configdir);
			self::$warnings = array_merge(self::$warnings,$warnings);
			$objs[] = DBTool::load($sqlfiles, Config::get_instance());
		}

		\Helper\share_known_tables($objs);

		return [$objs, null];
	}
}
