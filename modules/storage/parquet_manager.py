"""
Parquet存储管理器 (Parquet Manager)
负责Parquet文件的存储和管理
"""

import polars as pl
from typing import Dict, Any, Optional, Union, List
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import json


class ParquetManager:
    """Parquet存储管理器"""

    def __init__(self, base_path: Union[str, Path] = "data"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(exist_ok=True)

        # 创建子目录结构
        self.raw_data_path = self.base_path / "raw"
        self.processed_data_path = self.base_path / "processed"
        self.metadata_path = self.base_path / "metadata"

        for path in [self.raw_data_path, self.processed_data_path, self.metadata_path]:
            path.mkdir(exist_ok=True)

    def save_parquet(self,
                    df: pl.DataFrame,
                    table_name: str,
                    partition_cols: Optional[List[str]] = None,
                    data_type: str = "processed",
                    **kwargs):
        """保存DataFrame为Parquet格式"""
        # 根据数据类型选择存储路径
        if data_type == "raw":
            base_path = self.raw_data_path
        elif data_type == "processed":
            base_path = self.processed_data_path
        else:
            base_path = self.base_path

        table_path = base_path / f"{table_name}.parquet"

        # 设置默认的Parquet选项
        parquet_options = {
            'compression': 'snappy',
            'row_group_size': 100000,
        }
        parquet_options.update(kwargs)

        if partition_cols:
            # 分区保存 - 使用pyarrow进行分区
            table = pa.Table.from_pandas(df.to_pandas())
            pq.write_to_dataset(
                table,
                root_path=str(table_path),
                partition_cols=partition_cols,
                **parquet_options
            )
        else:
            # 普通保存
            df.write_parquet(table_path, **parquet_options)

        # 保存元数据
        self._save_metadata(table_name, df, partition_cols, data_type)

    def load_parquet(self, table_name: str, data_type: str = "processed") -> pl.DataFrame:
        """加载Parquet文件"""
        # 根据数据类型选择存储路径
        if data_type == "raw":
            base_path = self.raw_data_path
        elif data_type == "processed":
            base_path = self.processed_data_path
        else:
            base_path = self.base_path

        table_path = base_path / f"{table_name}.parquet"
        return pl.read_parquet(table_path)

    def list_tables(self, data_type: str = "processed") -> List[str]:
        """列出所有可用的表"""
        # 根据数据类型选择存储路径
        if data_type == "raw":
            base_path = self.raw_data_path
        elif data_type == "processed":
            base_path = self.processed_data_path
        else:
            base_path = self.base_path

        return [p.stem for p in base_path.glob("*.parquet")]

    def get_table_info(self, table_name: str, data_type: str = "processed") -> Dict[str, Any]:
        """获取表的信息"""
        # 根据数据类型选择存储路径
        if data_type == "raw":
            base_path = self.raw_data_path
        elif data_type == "processed":
            base_path = self.processed_data_path
        else:
            base_path = self.base_path

        table_path = base_path / f"{table_name}.parquet"

        if not table_path.exists():
            raise FileNotFoundError(f"Table {table_name} not found")

        # 使用PyArrow获取Parquet文件元数据
        parquet_file = None
        try:
            parquet_file = pq.ParquetFile(table_path)
            schema = parquet_file.schema_arrow
            num_columns = len(schema.names)
            columns = schema.names
            column_types = {field.name: str(field.type) for field in schema}
            num_rows = parquet_file.metadata.num_rows
        except Exception as e:
            print(f"获取Parquet元数据失败: {e}")
            # 回退到DataFrame方法
            df_temp = pl.read_parquet(table_path)
            num_columns = len(df_temp.columns)
            columns = df_temp.columns
            column_types = {col: str(dtype) for col, dtype in zip(df_temp.columns, df_temp.dtypes)}
            num_rows = len(df_temp)

        return {
            'table_name': table_name,
            'num_rows': num_rows,
            'num_columns': num_columns,
            'columns': columns,
            'column_types': column_types,
            'file_size': table_path.stat().st_size if table_path.is_file() else 'Directory',
            'data_type': data_type,
            'created_at': datetime.now().isoformat(),
        }

    def _save_metadata(self, table_name: str, df: pl.DataFrame,
                      partition_cols: Optional[List[str]], data_type: str):
        """保存表的元数据"""
        metadata = {
            'table_name': table_name,
            'num_rows': len(df),
            'num_columns': len(df.columns),
            'columns': df.columns,
            'dtypes': {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
            'partition_cols': partition_cols or [],
            'data_type': data_type,
            'created_at': datetime.now().isoformat(),
            'file_size': None,  # 将在文件创建后更新
        }

        metadata_file = self.metadata_path / f"{table_name}_metadata.json"
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

    def get_metadata(self, table_name: str) -> Dict[str, Any]:
        """获取表的元数据"""
        metadata_file = self.metadata_path / f"{table_name}_metadata.json"
        if metadata_file.exists():
            with open(metadata_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            raise FileNotFoundError(f"Metadata for table {table_name} not found")

    def optimize_table(self, table_name: str, data_type: str = "processed"):
        """优化表存储"""
        # 重新压缩表
        df = self.load_parquet(table_name, data_type)
        self.save_parquet(df, table_name, data_type=data_type,
                         compression='zstd', row_group_size=50000)

    def cleanup_old_data(self, table_name: str, days: int = 30, data_type: str = "processed"):
        """清理旧数据"""
        # 根据数据类型选择存储路径
        if data_type == "raw":
            base_path = self.raw_data_path
        elif data_type == "processed":
            base_path = self.processed_data_path
        else:
            base_path = self.base_path

        table_path = base_path / f"{table_name}.parquet"
        cutoff_date = datetime.now() - timedelta(days=days)

        if table_path.exists():
            # 检查文件修改时间
            mtime = datetime.fromtimestamp(table_path.stat().st_mtime)
            if mtime < cutoff_date:
                table_path.unlink()
                # 清理对应的元数据文件
                metadata_file = self.metadata_path / f"{table_name}_metadata.json"
                if metadata_file.exists():
                    metadata_file.unlink()
                print(f"已清理旧数据文件: {table_name}")

    def migrate_json_to_parquet(self, json_file: Union[str, Path],
                               table_name: str, data_type: str = "raw"):
        """迁移JSON数据到Parquet格式"""
        json_path = Path(json_file)

        if not json_path.exists():
            raise FileNotFoundError(f"JSON file {json_file} not found")

        # 读取JSON数据
        with open(json_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)

        # 转换为Polars DataFrame
        if isinstance(json_data, list):
            df = pl.DataFrame(json_data)
        elif isinstance(json_data, dict):
            # 如果是字典格式，转换为DataFrame
            if all(isinstance(v, list) for v in json_data.values()):
                # 字典的每个值都是列表
                df = pl.DataFrame(json_data)
            else:
                # 单个对象的字典
                df = pl.DataFrame([json_data])
        else:
            raise ValueError(f"Unsupported JSON format in {json_file}")

        # 保存为Parquet
        self.save_parquet(df, table_name, data_type=data_type)
        print(f"已迁移 {json_file} 到 Parquet: {table_name}")


# 全局Parquet管理器实例
parquet_manager = ParquetManager()