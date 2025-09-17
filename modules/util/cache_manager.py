#!/usr/bin/env python3
"""
缓存管理工具 - 用于查看、修改和优化缓存文件
支持JSON和Pickle格式的缓存文件操作
"""

import argparse
from datetime import datetime
import json
import os
import pickle
import sys
from typing import Any

# 类型别名
StockData = dict[str, Any]
StocksDict = dict[str, StockData]


class CacheManager:
    """缓存文件管理器"""

    def __init__(self, cache_dir: str = "data"):
        self.cache_dir = cache_dir
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

    def detect_format(self, filepath: str) -> str:
        """检测文件格式"""
        try:
            with open(filepath, "rb") as f:
                # 尝试读取前几个字节判断格式
                header = f.read(10)
                f.seek(0)

                # Pickle文件通常以协议字节开头
                if len(header) > 0 and header[0:1] in [
                    b"\x80",
                    b"\x81",
                    b"\x82",
                    b"\x83",
                    b"\x84",
                ]:
                    return "pickle"
                return "json"
        except OSError as e:
            print(f"⚠️ 无法检测文件格式 {filepath}: {e}")
            return "unknown"

    def load_cache(self, filename: str) -> dict[str, Any] | None:
        """加载缓存文件"""
        filepath = os.path.join(self.cache_dir, filename)

        if not os.path.exists(filepath):
            print(f"❌ 文件不存在: {filepath}")
            return None

        file_format = self.detect_format(filepath)
        print(f"📁 检测到文件格式: {file_format}")

        try:
            # 首先尝试JSON格式(更安全)
            try:
                with open(filepath, encoding="utf-8") as f:
                    data = json.load(f)
                print(f"✅ 缓存文件加载成功: {filename}")
                return data
            except json.JSONDecodeError:
                # 如果不是JSON,尝试安全的pickle格式
                if file_format == "pickle":
                    import hashlib

                    # 计算文件哈希用于完整性检查
                    with open(filepath, "rb") as f:
                        file_content = f.read()
                        file_hash = hashlib.sha256(file_content).hexdigest()

                        # 检查文件大小(防止DOS攻击)
                        if len(file_content) > 100 * 1024 * 1024:  # 100MB限制
                            raise ValueError("Cache file too large for safe loading")

                        # 使用BytesIO进行安全的反序列化
                        from io import BytesIO

                        data: dict[str, Any] = pickle.load(BytesIO(file_content))

                    print(f"✅ 缓存文件加载成功: {filename} (hash: {file_hash[:8]})")
                    return data
                raise ValueError(f"Unsupported file format: {file_format}")

        except Exception as e:
            print(f"❌ 加载失败: {e}")
            return None

    def load_cache_with_validation(
        self, filename: str, expected_date: str, data_type: str
    ) -> dict[str, Any] | None:
        """加载缓存文件并验证数据有效性"""
        filepath = os.path.join(self.cache_dir, filename)

        if not os.path.exists(filepath):
            return None

        try:
            # 加载缓存数据
            cached_data = self.load_cache(filename)
            if cached_data is None:
                return None

            # 验证缓存数据的有效性
            # cached_data已经是Dict[str, Any]类型,无需isinstance检查
            if not cached_data:
                print(f"❌ {data_type}缓存数据为空")
                return None

            # 检查是否有必要的字段
            if "data" not in cached_data:
                print(f"❌ {data_type}缓存缺少data字段")
                return None

            # 检查日期是否匹配
            cache_date = cached_data.get("date")
            if cache_date != expected_date:
                print(f"⚠️ {data_type}缓存日期不匹配: {cache_date} vs {expected_date}")
                return None

            print(f"✅ {data_type}缓存验证通过")
            return cached_data

        except Exception as e:
            print(f"❌ {data_type}缓存验证失败: {e}")
            return None

    def save_cache(
        self, data: dict[str, Any], filename: str, use_pickle: bool = True
    ) -> bool:
        """保存缓存文件"""
        filepath = os.path.join(self.cache_dir, filename)

        try:
            # 原子写入
            temp_filepath = filepath + ".tmp"
            with open(
                temp_filepath,
                "wb" if use_pickle else "w",
                encoding=None if use_pickle else "utf-8",
            ) as f:
                if use_pickle:
                    pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
                else:
                    json.dump(
                        data, f, ensure_ascii=False, separators=(",", ":"), default=str
                    )

            # 原子移动
            if os.path.exists(filepath):
                os.remove(filepath)
            os.rename(temp_filepath, filepath)

            print(
                f"💾 缓存文件保存成功: {filename} (格式: {'pickle' if use_pickle else 'json'})"
            )
            return True

        except Exception as e:
            print(f"❌ 保存失败: {e}")
            return False

    def save_cache_data(
        self, data: Any, filename: str, data_type: str, date: str
    ) -> bool:
        """保存数据到缓存文件,包含元数据"""
        try:
            # 准备缓存数据结构
            cache_data: dict[str, Any] = {
                "data": data,
                "date": date,
                "data_type": data_type,
                "created_at": datetime.now().isoformat(),
                "version": "1.0",
            }

            # 使用现有的save_cache方法保存
            return self.save_cache(cache_data, filename, use_pickle=False)

        except Exception as e:
            print(f"❌ 保存{data_type}缓存数据失败: {e}")
            return False

    def show_cache_info(self, data: dict[str, Any]) -> None:
        """显示缓存文件信息"""
        if not data:
            print("❌ 缓存数据为空")
            return

        print("\n📊 缓存文件信息:")

        # 基本信息
        if "trading_date" in data:
            print(f"   📅 交易日期: {data['trading_date']}")
        if "fetch_time" in data:
            print(f"   🕐 获取时间: {data['fetch_time']}")

        # 股票信息
        stocks_data = data.get("stocks")
        if stocks_data and isinstance(stocks_data, dict):
            stocks_dict: StocksDict = stocks_data
            stock_count = len(stocks_dict)
            print(f"   📈 股票数量: {stock_count}")

            if stock_count > 0:
                # 显示前几个股票
                sample_stocks: list[str] = list(stocks_dict.keys())[:5]
                print(f"   📋 示例股票: {', '.join(sample_stocks)}")

                # 计算数据点统计
                total_data_points: int = 0
                for _, stock_data in stocks_dict.items():
                    if "data_points" in stock_data:
                        total_data_points += stock_data["data_points"]

                if total_data_points > 0:
                    print(f"   📊 总数据点数: {total_data_points}")
                    print(f"   📊 平均每股数据点: {total_data_points // stock_count}")

        # 文件大小估算
        data_size = sys.getsizeof(data)
        print(f"   💾 内存占用: {data_size / 1024 / 1024:.2f} MB")

    def convert_format(self, filename: str, target_format: str) -> bool:
        """转换缓存文件格式"""
        print(f"🔄 转换文件格式: {filename} -> {target_format}")

        # 加载数据
        data = self.load_cache(filename)
        if data is None:
            return False

        # 确定新文件名
        if target_format == "pickle":
            new_filename = filename.replace(".json", ".pkl")
        else:
            new_filename = filename.replace(".pkl", ".json")

        # 保存为新格式
        use_pickle = target_format == "pickle"
        return self.save_cache(data, new_filename, use_pickle)

    def optimize_cache(self, filename: str) -> bool:
        """优化缓存文件(转换为最优格式)"""
        print(f"⚡ 优化缓存文件: {filename}")

        data = self.load_cache(filename)
        if data is None:
            return False

        # 根据数据量选择格式
        stock_count = (
            len(data.get("stocks", {})) if isinstance(data.get("stocks"), dict) else 0
        )

        if stock_count > 1000:
            print(f"   📊 大数据集({stock_count}股票)，使用pickle格式优化")
            use_pickle = True
            new_filename = filename.replace(".json", ".pkl")
        else:
            print(f"   📊 小数据集({stock_count}股票)，使用紧凑JSON格式")
            use_pickle = False
            new_filename = filename

        return self.save_cache(data, new_filename, use_pickle)

    def search_stocks(self, data: dict[str, Any], pattern: str) -> None:
        """搜索股票"""
        if not data or "stocks" not in data:
            print("❌ 无股票数据")
            return

        stocks_data = data.get("stocks")
        if not stocks_data or not isinstance(stocks_data, dict):
            print("❌ 股票数据格式错误")
            return

        stocks_dict: StocksDict = stocks_data
        matches: list[tuple[str, StockData]] = []
        for stock_code, stock_data in stocks_dict.items():
            if pattern.lower() in stock_code.lower():
                matches.append((stock_code, stock_data))

        if matches:
            print(f"\n🔍 找到 {len(matches)} 只匹配的股票:")
            for stock_code, stock_data in matches[:10]:  # 最多显示10个
                data_points = stock_data.get("data_points", 0)
                print(f"   📈 {stock_code}: {data_points} 个数据点")
        else:
            print(f"❌ 未找到匹配的股票: {pattern}")


def main() -> None:
    parser = argparse.ArgumentParser(description="缓存管理工具")
    parser.add_argument(
        "action", choices=["info", "convert", "optimize", "search"], help="操作类型"
    )
    parser.add_argument("filename", help="缓存文件名")
    parser.add_argument(
        "--format", choices=["json", "pickle"], help="转换目标格式 (用于convert操作)"
    )
    parser.add_argument("--pattern", help="搜索模式 (用于search操作)")
    parser.add_argument("--cache-dir", default="data", help="缓存目录")

    args = parser.parse_args()

    manager = CacheManager(args.cache_dir)

    if args.action == "info":
        data = manager.load_cache(args.filename)
        if data:
            manager.show_cache_info(data)

    elif args.action == "convert":
        if not args.format:
            print("❌ 请指定目标格式 --format json 或 --format pickle")
            return
        manager.convert_format(args.filename, args.format)

    elif args.action == "optimize":
        manager.optimize_cache(args.filename)

    elif args.action == "search":
        if not args.pattern:
            print("❌ 请指定搜索模式 --pattern <pattern>")
            return
        data = manager.load_cache(args.filename)
        if data:
            manager.search_stocks(data, args.pattern)


if __name__ == "__main__":
    main()
