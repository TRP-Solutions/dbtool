<?php
/*
DBTool is licensed under the Apache License 2.0 license
https://github.com/TRP-Solutions/dbtool/blob/master/LICENSE
*/

declare(strict_types=1);

class DataTypeNameProxy implements jsonSerializable {
	public function __construct(public readonly DataType $datatype){}
	public function __toString(){
		return $this->datatype->normalized_name();
	}

	public function jsonSerialize(): mixed {
		return (string) $this;
	}
}

class DataType implements ArrayAccess, jsonSerializable {
	public static $real_is_double = true;

	protected const SIZE = null;
	public readonly int $size;

	public static function from(SQLType $type): self {
		if($type->is_integer()){
			return new DataTypeInteger($type);
		} elseif($type->is_fixed_point()){
			return new DataTypeFixedPoint($type);
		} elseif($type->is_floating_point()){
			return new DataTypeFloatingPoint($type);
		} elseif($type->is_text() || $type->is_binary()){
			if($type->size_required() || $type->size_optional()){
				return new DataTypeStringSized($type);
			} else {
				return new DataTypeString($type);
			}
		} elseif($type->is_enum()){
			return new DataTypeEnum($type);
		} elseif($type->has_seconds()){
			return new DataTypeTime($type);
		} elseif($type->size_optional()){
			return new DataTypeSized($type);
		}
		return new self($type);
	}

	protected function __construct(public readonly SQLType $type){}

	public function offsetExists(mixed $offset): bool {
		return property_exists($this, $offset) && isset($this->$offset) || $offset == static::SIZE && isset($this->size);
	}
	public function offsetGet(mixed $offset): mixed {
		if($offset == 'name'){
			return $this->normalized_name();
		} elseif($offset == static::SIZE){
			return $this->size ?? null;
		} elseif(property_exists($this, $offset)) {
			return $this->$offset ?? null;
		} else {
			throw new \Exception("invalid offset on {$this->type->value} (".static::class."): '$offset'");
		}
	}
	public function offsetSet(mixed $offset, mixed $value): void {
		if($offset == static::SIZE || $offset == 'size'){
			$this->size = intval($value);
		} else {
			throw new \Exception("invalid offset on {$this->type->value} (".static::class."): '$offset'");
		}
	}
	public function offsetUnset(mixed $offset): void {

	}

	public function __call($name, $arguments){
		if(method_exists($this->type, $name)){
			return $this->type->$name(...$arguments);
		}
	}

	public function __toString(){
		return $this->normalized_name().$this->parameter_string();
	}

	public function string_with_attribute(){
		$attr = $this->attributes();
		if(!empty($attr)){
			return $this.' '.implode(' ',$attr);
		} else {
			return (string) $this;
		}
	}

	protected function parameter_string(){
		$size = $this->size ?? $this->default_size();
		return isset($size) ? '('.$size.')' : '';
	}

	protected function attributes(){
		return [];
	}

	public function to_array(){
		$array = get_object_vars($this);
		if(static::SIZE !== null && isset($array['size'])){
			$array[static::SIZE] = $array['size'];
			unset($array['size']);
		}
		$array['type'] = $this->type->normalized_type();
		return $array;
	}

	public function jsonSerialize(): mixed {
		return $this->to_array();
	}

	public function is_lossless(DataType $other): bool {
		return $this->normalized_type() == $other->normalized_type();
	}
}

class DataTypeString extends DataType {
	protected readonly bool $binary;
	public readonly string $character_set;
	public readonly string $collate;

	
	public function offsetGet(mixed $offset): mixed {
		if($offset == 'character set'){
			return $this->character_set ?? null;
		} else {
			return parent::offsetGet($offset);
		}
	}
	public function offsetSet(mixed $offset, mixed $value): void {
		match($offset){
			'character set' => $this->character_set = $value,
			'collate' => $this->collate = $value,
			default => parent::offsetSet($offset, $value)
		};
	}

	public function offsetExists(mixed $offset): bool {
		return $offset == 'character set' && isset($this->character_set) || parent::offsetExists($offset);
	}

	public function is_lossless(DataType $other): bool {
		return is_a($other, 'DataTypeString')
			&& $this->byte_size() <= $other->byte_size();
	}

	protected function byte_size(): int {
		return 2 ** $this->storage_power() - 1;
	}

	protected function attributes(){
		$attr = parent::attributes();
		if(isset($this->character_set)){
			$attr[] = 'CHARACTER SET '.$this->character_set;
		}
		if(isset($this->collate)){
			$attr[] = 'COLLATE '.$this->collate;
		}
		return $attr;
	}
}

class DataTypeStringSized extends DataTypeString {
	protected const SIZE = 'char_max_length';

	protected function byte_size(): int {
		if(!isset($this->size)) parent::byte_size();
		$max_bytes_per_char = $this->is_text() ? 4 : 1;
		return $this->size * $max_bytes_per_char;
	}
}

class DataTypeNumeric extends DataType {
	public readonly bool $signed;
	public readonly bool $zerofill;
	protected readonly int $bitwidth;
	protected $debug = false;

	public function offsetSet(mixed $offset, mixed $value): void {
		if($offset == 'signed'){
			$this->signed   = (bool) $value;
		} elseif($offset == 'unsigned'){
			$this->signed   = !$value;
		} elseif($offset == 'zerofill'){
			$this->zerofill = (bool) $value;
		} else {
			parent::offsetSet($offset, $value);
		}
	}

	public function offsetGet(mixed $offset): mixed {
		if($offset == 'unsigned'){
			return !$this->signed;
		} else {
			return parent::offsetGet($offset);
		}
	}

	public function offsetExists(mixed $offset): bool {
		return $offset == 'unsigned' || parent::offsetExists($offset);
	}
	
	public function is_lossless(DataType $other): bool {
		return (
			is_a($other, static::class)
			&& isset($this->signed)
			&& isset($other->signed)
			// signed -> unsigned is not lossless
			&& !($this->signed && !$other->signed)
		);
	}

	protected function attributes(){
		$attr = parent::attributes();
		if(!($this->signed ?? true)){
			$attr[] = 'UNSIGNED';
		}
		if($this->zerofill ?? false){
			$attr[] = 'ZEROFILL';
		}
		return $attr;
	}
}

class DataTypeInteger extends DataTypeNumeric {
	protected const SIZE = 'display_width';

	public function is_lossless(DataType $other): bool {
		return (
			parent::is_lossless($other)
			&& $this->storage_bytes() <= $other->storage_bytes()
			&& !( // note ! outside parenthesis
				(!$this->signed && $other->signed)
				// unsigned -> signed requires larger target
				&& $this->storage_bytes() < $other->storage_bytes()
			)
		);
	}
}

class DataTypeNumericPoint extends DataTypeNumeric {
	protected const SIZE = 'precision';
	public readonly string $decimals;

	public function offsetSet(mixed $offset, mixed $value): void {
		match($offset){
			'decimals' => $this->decimals = $value,
			default => parent::offsetSet($offset, $value)
		};
	}

	protected function parameter_string(){
		if(isset($this->decimals)){
			return '('.($this->size??$this->default_size()).', '.($this->decimals??0).')';
		} else {
			return parent::parameter_string();
		}
	}
}

class DataTypeFloatingPoint extends DataTypeNumericPoint {
	public function is_lossless(DataType $other): bool {
		return (
			parent::is_lossless($other)
			&& ($this->normalized_type() == SQLType::Float || $other->normalized_type() != SQLType::Float)
		);
	}
}

class DataTypeFixedPoint extends DataTypeNumericPoint {
	public function is_lossless(DataType $other): bool {
		return parent::is_lossless($other) && $this->is_fixed_point() && $other->is_fixed_point();
	}

	public function offsetGet(mixed $offset): mixed {
		if($offset == 'decimals'){
			return $this->decimals ?? 0;
		} else {
			return parent::offsetGet($offset);
		}
	}

	public function offsetExists(mixed $offset): bool {
		return match($offset){
			'decimals' => true,
			default => parent::offsetExists($offset)
		};
	}

	protected function parameter_string(){
		return '('.($this->size??$this->default_size()).', '.($this->decimals ?? 0).')';
	}
}

class DataTypeEnum extends DataTypeString {
	public readonly array $values;

	public function offsetSet(mixed $offset, mixed $value): void {
		match($offset){
			'values' => $this->values = $value,
			default => parent::offsetSet($offset, $value)
		};
	}

	protected function parameter_string(){
		return isset($this->values) ? "(".implode(",",$this->values).")" : '()';
	}

	public function is_lossless(DataType $other): bool {
		// parenthesis for ease of reading
		return (
			is_a($other, self::class) // lossless conversion only possible to this type
			&& !($this->type == SQLType::Set && $other->type == SQLType::Enum) // Set can't convert losslessly to Enum
			&& !($this->type == SQLType::Enum && $other->type == SQLType::Set && count($this->values) > 64) // Set can't have more than 64 members
			&& empty(array_diff($this->values, $other->values)) // find possible values in this enum, that aren't in the other enum
		);
	}

	public function values_diff(DataType $other): ?array {
		if(!is_a($other, self::class)) return null;
		return array_diff($this->values, $other->values);
	}
}

class DataTypeTime extends DataType {
	protected const SIZE = 'fsp'; // Fractional Seconds Precision

	public function is_lossless(DataType $other): bool {
		return $this->normalized_type() == $other->normalized_type();
	}
}

class DataTypeSized extends DataType {
	protected const SIZE = 'length';

	public function is_lossless(DataType $other): bool {
		return $this->normalized_type() == $other->normalized_type()
			&& ($this->size ?? 0) <= ($other->size ?? 0);
	}
}

enum SQLType: string {
	public static function known_types(){
		return array_map(fn($case) => $case->value, self::cases());
	}

	/*** Integer types ***/
	case Int = 'INT';
	case Integer = 'INTEGER';
	case Tinyint = 'TINYINT';
	case Smallint = 'SMALLINT';
	case Mediumint = 'MEDIUMINT';
	case Bigint = 'BIGINT';
	case Bool = 'BOOL';
	case Boolean = 'BOOLEAN';
	public function is_integer(){
		return $this == self::Int
			|| $this == self::Integer
			|| $this == self::Tinyint
			|| $this == self::Smallint
			|| $this == self::Mediumint
			|| $this == self::Bigint
			|| $this == self::Bool
			|| $this == self::Boolean;
	}

	/*** Fixed point types ***/
	case Decimal = 'DECIMAL';
	case Numeric = 'NUMERIC';

	public function is_fixed_point(){
		return $this == self::Decimal
			|| $this == self::Numeric;
	}

	/*** Floating point types ***/
	case Float = 'FLOAT';
	case Real = 'REAL';
	case Double = 'DOUBLE';

	public function is_floating_point(){
		return $this == self::Float
			|| $this == self::Real
			|| $this == self::Double;
	}

	/*** Text types ***/
	case Varchar = 'VARCHAR';
	case Char = 'CHAR';
	case Text = 'TEXT';
	case Tinytext = 'TINYTEXT';
	case Mediumtext = 'MEDIUMTEXT';
	case Longtext = 'LONGTEXT';

	public function is_text(){
		return $this == self::Varchar
			|| $this == self::Char
			|| $this == self::Text
			|| $this == self::Tinytext
			|| $this == self::Mediumtext
			|| $this == self::Longtext;
	}

	/*** Binary string types ***/
	case Varbinary = 'VARBINARY';
	case Binary = 'BINARY';
	case Blob = 'BLOB';
	case Tinyblob = 'TINYBLOB';
	case Mediumblob = 'MEDIUMBLOB';
	case Longblob = 'LONGBLOB';
	
	public function is_binary(){
		return $this == self::Varbinary
			|| $this == self::Binary
			|| $this == self::Blob
			|| $this == self::Tinyblob
			|| $this == self::Mediumblob
			|| $this == self::Longblob;
	}

	/*** Enum types ***/
	case Enum = 'ENUM';
	case Set = 'SET';

	public function is_enum(){
		return $this == self::Enum
			|| $this == self::Set;
	}

	/*** Date/Time types ***/
	case Timestamp = 'TIMESTAMP';
	case Time = 'TIME';
	case Datetime = 'DATETIME';
	case Date = 'DATE';
	case Year = 'YEAR';

	public function has_seconds(){
		return $this == self::Timestamp
			|| $this == self::Time
			|| $this == self::Datetime;
	}

	public function can_default_current(){
		return $this == self::Timestamp
			|| $this == self::Datetime;
	}

	/*** Other types ***/
	case Bit = 'BIT';
	case Json = 'JSON';
	case Uuid = 'UUID';

	/*** End of types ***/

	public function normalized_type(){
		if($this == self::Bool || $this == self::Boolean) return self::Tinyint;
		if($this == self::Integer) return self::Int;
		if($this == self::Numeric) return self::Decimal;
		if($this == self::Real) return DataType::$real_is_double ? self::Double : self::Float;
		return $this;
	}

	public function normalized_name(){
		return $this->normalized_type()->value;
	}

	public function size_optional(){
		return $this->is_numeric()
			|| $this == self::Bit
			|| $this == self::Binary
			|| $this == self::Char;
	}

	public function size_required(){
		return $this == self::Varbinary
			|| $this == self::Varchar;
	}

	public function size_name(){
		if($this->is_integer()) return 'display_width';
		if($this->is_floating_point()) return 'precision';
		if($this->is_fixed_point()) return 'precision';
		if($this->is_string()) return 'char_max_length';
		return 'length';
	}

	public function is_string(){
		return $this->is_text() || $this->is_binary() || $this->is_enum();
	}

	public function accepts_string_literal(){
		return $this->is_string()
			|| $this == self::Timestamp
			|| $this == self::Time
			|| $this == self::Datetime
			|| $this == self::Date
			|| $this == self::Year;
	}

	public function is_numeric(){
		return $this->is_integer() || $this->is_fixed_point() || $this->is_floating_point();
	}

	public function default_size(){
		return match($this){
			self::Bool, self::Boolean => 1,
			self::Tinyint => 4,
			self::Smallint => 6,
			self::Mediumint => 9,
			self::Int, self::Integer => 11,
			self::Bigint => 20,
			self::Decimal, self::Numeric => 10,
			self::Char => 1,
			self::Binary => 1,
			self::Bit => 1,
			default => null
		};
	}

	public function is_nullable_by_default(){
		return $this != self::Timestamp;
	}

	public function zero_value(){
		return match($this){
			self::Timestamp => "'0000-00-00 00:00:00'",
			self::Uuid => "'00000000-0000-0000-0000-000000000000'",
			default => null
		};
	}

	public function storage_bytes(){
		return match($this){
			self::Tinyint => 1,
			self::Smallint => 2,
			self::Mediumint => 3,
			self::Int, self::Integer => 4,
			self::Bigint => 8,
			default => 0
		};
	}

	public function storage_power(){
		// type can store up to 2^N bytes of data
		return match($this){
			// Tinytext and Tinyblob holds up to 2^8-1 bytes
			// Length marker is 1 byte = 16 bits, one byte used by 0x00 string terminator
			self::Tinytext,self::Tinyblob => 8,
			// Text and Blob holds up to 2^16-1 bytes
			// Length marker is 2 bytes = 16 bits, one byte used by 0x00 string terminator
			self::Text,self::Blob => 16,
			// Mediumtext and Mediumblob holds up to 2^24-1 bytes
			// Length marker is 3 bytes = 24 bits, one byte used by 0x00 string terminator
			self::Mediumtext,self::Mediumblob => 24,
			// Longtext and Longblob holds up to 2^32-1 bytes
			// Length marker is 4 bytes = 32 bits, one byte used by 0x00 string terminator
			self::Longtext,self::Longblob => 32,
			// Binary holds up to 2^8-1 bytes
			self::Binary => 8,
			// Char holds up to 2^8-1 characters = 2^10-4 bytes
			// Characters can be up to 4 bytes depending on character set
			self::Char => 10,
			// Varbinary holds up to 2^16-1 bytes
			self::Varbinary => 16,
			// Varchar holds up to 2^16-1 characters = 2^18-4 bytes
			// Characters can be up to 4 bytes depending on character set
			self::Varchar => 18,
			default => 0
		};
	}
}
