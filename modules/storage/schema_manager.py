"""
数据模式管理器 (Schema Manager)
负责数据模式的定义、验证和管理
"""

import polars as pl
from typing import Dict, Any, Optional, List, Union
from pathlib import Path
import json
from datetime import datetime

# 导入统一的schema定义
from modules.data_schema import (
    ALL_TABLE_SCHEMAS,
    get_table_schema,
    get_table_columns,
    get_table_primary_key,
    get_table_indexes,
    get_table_partition_by
)


class SchemaManager:
    """数据模式管理器"""

    def __init__(self, schema_path: Union[str, Path] = "config/schemas"):
        self.schema_path = Path(schema_path)
        self.schema_path.mkdir(exist_ok=True)
        self.schemas: Dict[str, Dict[str, Any]] = {}

        # 加载现有模式
        self._load_schemas()

    def define_schema(self, table_name: str, schema_definition: Dict[str, Any]):
        """定义数据表模式"""
        schema = {
            'table_name': table_name,
            'columns': schema_definition.get('columns', {}),
            'primary_key': schema_definition.get('primary_key', []),
            'indexes': schema_definition.get('indexes', []),
            'constraints': schema_definition.get('constraints', {}),
            'partition_by': schema_definition.get('partition_by', []),
            'created_at': datetime.now().isoformat(),
            'version': schema_definition.get('version', '1.0.0'),
        }

        self.schemas[table_name] = schema
        self._save_schema(table_name, schema)

    def get_schema(self, table_name: str) -> Dict[str, Any]:
        """获取数据表模式"""
        if table_name in ALL_TABLE_SCHEMAS:
            schema = ALL_TABLE_SCHEMAS[table_name]
            return {
                'table_name': schema.name,
                'columns': get_table_columns(table_name),
                'primary_key': schema.primary_key,
                'indexes': schema.indexes,
                'partition_by': schema.partition_by,
                'description': schema.description,
                'version': '1.0.0',
                'created_at': datetime.now().isoformat()
            }
        elif table_name in self.schemas:
            return self.schemas[table_name]
        else:
            raise ValueError(f"Schema for table '{table_name}' not found")

    def validate_dataframe(self, df: pl.DataFrame, table_name: str) -> List[str]:
        """验证DataFrame是否符合模式定义"""
        if table_name not in self.schemas:
            return [f"Schema for table '{table_name}' not defined"]

        schema = self.schemas[table_name]
        errors = []

        # 检查必需的列
        required_columns = set(schema['columns'].keys())
        actual_columns = set(df.columns)

        missing_columns = required_columns - actual_columns
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")

        extra_columns = actual_columns - required_columns
        if extra_columns:
            errors.append(f"Extra columns not in schema: {extra_columns}")

        # 检查数据类型
        for col in actual_columns & required_columns:
            expected_type = schema['columns'][col].get('type')
            actual_type = str(df[col].dtype)

            if expected_type and not self._types_compatible(expected_type, actual_type):
                errors.append(f"Column '{col}' type mismatch: expected {expected_type}, got {actual_type}")

        return errors

    def _types_compatible(self, expected: str, actual: str) -> bool:
        """检查数据类型是否兼容"""
        # 简化类型兼容性检查
        type_mapping = {
            'int64': ['int64', 'int32', 'int16', 'int8', 'Int64', 'Int32', 'Int16', 'Int8'],
            'float64': ['float64', 'float32', 'Float64', 'Float32'],
            'string': ['str', 'object', 'String', 'Utf8'],
            'bool': ['bool', 'boolean', 'Boolean'],
            'date': ['date', 'datetime', 'Date', 'Datetime'],
            'datetime': ['datetime', 'date', 'Datetime', 'Date'],
        }

        compatible_types = type_mapping.get(expected.lower(), [expected.lower()])
        return actual in compatible_types or actual.lower() in [t.lower() for t in compatible_types]

    def _load_schemas(self):
        """加载所有模式定义"""
        for schema_file in self.schema_path.glob("*.json"):
            table_name = schema_file.stem
            with open(schema_file, 'r', encoding='utf-8') as f:
                self.schemas[table_name] = json.load(f)

    def _save_schema(self, table_name: str, schema: Dict[str, Any]):
        """保存模式定义"""
        schema_file = self.schema_path / f"{table_name}.json"
        with open(schema_file, 'w', encoding='utf-8') as f:
            json.dump(schema, f, indent=2, ensure_ascii=False)

    def list_schemas(self) -> List[str]:
        """列出所有定义的模式"""
        return list(self.schemas.keys())

    def update_schema_version(self, table_name: str, new_version: str):
        """更新模式版本"""
        if table_name not in self.schemas:
            raise ValueError(f"Schema for table '{table_name}' not found")

        self.schemas[table_name]['version'] = new_version
        self.schemas[table_name]['updated_at'] = datetime.now().isoformat()
        self._save_schema(table_name, self.schemas[table_name])

    def get_schema_version(self, table_name: str) -> str:
        """获取模式版本"""
        if table_name not in self.schemas:
            raise ValueError(f"Schema for table '{table_name}' not found")
        return self.schemas[table_name].get('version', '1.0.0')


# =============================================================================
# 兼容性函数 (Compatibility Functions)
# =============================================================================

def get_instruments_schema() -> Dict[str, Any]:
    """获取仪器schema（兼容旧接口）"""
    return {
        'columns': get_table_columns('instruments'),
        'primary_key': get_table_primary_key('instruments'),
        'indexes': get_table_indexes('instruments'),
        'partition_by': get_table_partition_by('instruments')
    }


def get_factors_schema() -> Dict[str, Any]:
    """获取因子schema（兼容旧接口）"""
    return {
        'columns': get_table_columns('factors'),
        'primary_key': get_table_primary_key('factors'),
        'indexes': get_table_indexes('factors'),
        'partition_by': get_table_partition_by('factors')
    }


def get_valuation_schema() -> Dict[str, Any]:
    """获取估值schema（兼容旧接口）"""
    return {
        'columns': get_table_columns('valuation'),
        'primary_key': get_table_primary_key('valuation'),
        'indexes': get_table_indexes('valuation'),
        'partition_by': get_table_partition_by('valuation')
    }


# 全局模式管理器实例
schema_manager = SchemaManager()