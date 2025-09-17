import sys
sys.path.append('.')

from modules.data_schema import DATA_FETCH_CONFIG

price_config = DATA_FETCH_CONFIG['price_data']
print('前复权配置验证:')
print(f'  字段: {price_config["fields"]}')
print(f'  频率: {price_config["frequency"]}')
print(f'  复权方式: {price_config["adjust_type"]}')

# 验证复权方式是否为前复权
if price_config["adjust_type"] == "pre":
    print('✅ 配置正确：使用前复权数据')
else:
    print('❌ 配置错误：不是前复权数据')