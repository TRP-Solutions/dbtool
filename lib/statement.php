<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/

declare(strict_types=1);
class Statement implements jsonSerializable {
	public static function modify_column($database, $table, $column_diff){
		// Definitiondiff::generate_alter_queries
		return new self(StatementType::ModifyColumn, $database, $table, column_diff:$column_diff);
	}

	public static function add_column($database, $table, $column_diff){
		// Definitiondiff::generate_alter_queries
		return new self(StatementType::AddColumn, $database, $table, column_diff:$column_diff);
	}

	public static function drop_column($database, $table, $column_name, $column_diff){
		// Definitiondiff::generate_alter_queries
		return new self(StatementType::DropColumn, $database, $table, column_name:$column_name, column_diff:$column_diff);
	}

	public static function drop_table($database, $table){
		// Definitiondiff->get_drop
		return new self(StatementType::DropTable, $database, $table);
	}

	private StatementGuard $guard_state = StatementGuard::Unknown;
	public ?string $guard_warning = null;
	private string $table_identifier;
	private bool $use_guards = false;

	private function __construct(
		private StatementType $type,
		private ?string $database,
		private string $table,
		private $column_name = null,
		private $column_diff = null
	){
		$this->table_identifier = isset($database) ? "`$database`.`$table`" : `$table`;
		$this->use_guards = !\Config::get('ignore-dataloss');
		if(!$this->use_guards || $type == StatementType::AddColumn || $this->is_safe_modify()){
			$this->guard_state = StatementGuard::Safe;
		}
	}

	private function is_safe_modify(){
		return $this->type == StatementType::ModifyColumn
			&& $this->type_conversion_is_safe()
			&& empty($this->removed_enum_values());
	}

	public function execute(mysqli $mysqli): mysqli_result|bool{
		if($this->is_safe($mysqli)){
			return $mysqli->query($this->toSQL());
		} else {
			return false;
		}
	}

	public function is_safe(mysqli $mysqli): bool {
		return match($this->guard_state){
			StatementGuard::Safe => true,
			StatementGuard::Unsafe => false,
			StatementGuard::Unknown => $this->release_guard($mysqli)
		};
	}

	private function release_guard(mysqli $mysqli): bool {
		$guard = $this->build_guard_condition();
		$query = $mysqli->query($guard);
		if($mysqli->errno || $query === false || $query->num_rows == 0){
			throw new \Exception('Error while testing guard condition.');
		}
		$result = $query->fetch_assoc();
		if($result['count'] == 0){
			$this->guard_state = StatementGuard::Safe;
			return true;
		}

		if($result['count'] == 1) {
			$this->guard_warning = "1 affected row found";
		} else {
			$this->guard_warning = $result['count']." affected rows found";
		}
		$this->guard_state = StatementGuard::Unsafe;
		return false;
	}

	public function __toString(){
		if($this->guard_state == StatementGuard::Unknown || !empty($this->guard_warning)){
			return "-- [WARNING] -- ".$this->toSQL();
		} else {
			return $this->toSQL();
		}
	}

	private function toSQL(){
		return match($this->type){
			StatementType::ModifyColumn => "ALTER TABLE $this->table_identifier MODIFY COLUMN ".$this->column_definition().';',
			StatementType::AddColumn => "ALTER TABLE $this->table_identifier ADD COLUMN ".$this->column_definition().';',
			StatementType::DropColumn => "ALTER TABLE $this->table_identifier DROP COLUMN `$this->column_name`;",
			StatementType::DropTable => "DROP TABLE $this->table_identifier",
		};
	}

	private function column_definition(){
		$t2 = $this->t2();
		
		$definition = Format::column_A_to_definition($t2);
		
		if(isset($t2['after']) && $t2['after'] != $this->t1('after',true)){
			$definition .= $t2['after'] == '#FIRST' ? " FIRST": " AFTER `$t2[after]`";
		}

		return $definition;
	}

	private function build_guard_condition(){
		if($this->type == StatementType::DropTable){
			return "SELECT count(*) as count FROM $this->table_identifier";
		} elseif($this->type == StatementType::DropColumn){
			return $this->count_non_default_rows($this->column_name);
		} elseif($this->type == StatementType::ModifyColumn){
			$column_name = $this->t1('colname');
			$t1 = $this->t1('data_type_obj');
			$t2 = $this->t2('data_type_obj');
			if($t1->is_lossless($t2)){
				return $this->count_non_default_rows($column_name);
			} else {
				$removed_values = $t1->values_diff($t2);
				if(!empty($removed_values)){
					$removed_values = implode(',',$removed_values);
					return "SELECT count(*) as count FROM $this->table_identifier WHERE `$column_name` in ($removed_values)";
				}
			}
		}
		return '';
	}

	private function type_conversion_is_safe(){
		$t1 = $this->t1('data_type_obj');
		$t2 = $this->t2('data_type_obj');
		return $t1->is_lossless($t2);
	}

	private function removed_enum_values(){
		$t1 = $this->t1('data_type_obj');
		if(is_a($t1,'DataTypeEnum')){
			$t2 = $this->t2('data_type_obj');
			return $t1->values_diff($t2);
		} else {
			return [];
		}
	}

	private function count_non_default_rows($column_name){
		if(!empty($this->t1('default'))) {
			return "SELECT count(*) as count FROM $this->table_identifier WHERE IF(DEFAULT(`$column_name`) IS NULL, `$column_name` IS NOT NULL, `$column_name` != DEFAULT(`$column_name`))";
		}
		$default_value = $this->implicit_default();
		if(isset($default_value)){
			return "SELECT count(*) as count FROM $this->table_identifier WHERE `$column_name` != $default_value";
		}
		return "SELECT count(*) as count FROM $this->table_identifier";
	}

	private function implicit_default(){
		$type = $this->t1('data_type_obj');
		if($type->is_numeric()){
			return "0";
		} elseif($type->is_text()) {
			return "''";
		} elseif($type == \SQLType::Date){
			return "'0000-00-00'";
		} elseif($type == \SQLType::Time){
			return "'00:00.00'";
		} elseif($type == \SQLType::Datetime || $type == \SQLType::Timestamp){
			return "'0000-00-00 00:00.00'";
		} elseif($type == \SQLType::Year){
			return "'00:00.00'";
		}
	}

	private function t1($key = null, $allow_null = false){
		if(!$allow_null && !isset($this->column_diff['t1'])){
			throw new \Exception("Invalid column diff");
		}

		if($key == 'data_type_obj') return $this->column_diff['t1']['data_type']->datatype;
		return isset($key) ? $this->column_diff['t1'][$key] ?? null : $this->column_diff['t1'] ?? null;
	}

	private function t2($key = null, $allow_null = false){
		if(!$allow_null && !isset($this->column_diff['t2'])){
			throw new \Exception("Invalid column diff");
		}

		if($key == 'data_type_obj') return $this->column_diff['t2']['data_type']->datatype;
		return isset($key) ? $this->column_diff['t2'][$key] ?? null : $this->column_diff['t2'] ?? null;
	}

	public function jsonSerialize(): mixed {
		return (string) $this;
	}
}

enum StatementType {
	case ModifyColumn;
	case AddColumn;
	case DropColumn;
	case DropTable;
}

enum StatementGuard {
	case Unknown;
	case Safe;
	case Unsafe;
}
