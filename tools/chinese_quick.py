#!/usr/bin/env python3
"""
快速中文处理脚本
"""
import sys
import chardet
import jieba
from opencc import OpenCC

def main():
    if len(sys.argv) < 2:
        print("快速中文处理工具")
        print("用法: python chinese_quick.py <文本>")
        return
    
    text = " ".join(sys.argv[1:])
    
    print(f"原文: {text}")
    print(f"分词: {'/'.join(jieba.cut(text))}")
    
    # 繁简转换
    cc_s2t = OpenCC('s2t')
    cc_t2s = OpenCC('t2s')
    print(f"简→繁: {cc_s2t.convert(text)}")
    print(f"繁→简: {cc_t2s.convert(text)}")

if __name__ == "__main__":
    main()
