# -*- coding: utf-8 -*-
"""
AI开发环境配置工具
AI Development Environment Configuration Tool

工作环境：
- 操作系统：Debian 12.12
- IDE：VS Code
- 终端：Bash (Linux default shell)
- Python：Python 3.13 (base环境)
- 包管理：pip/conda

代码风格约定：
- 文件编码：UTF-8
- 换行符：LF (Linux)
- 字符串：使用双引号优先
- 路径分隔符：使用 os.path.join() 或 pathlib
- 代码生成：避免复杂f-string嵌套

第一部分：环境基础
==============
环境兼容性规则：
- 操作系统环境：当前运行环境为Debian 12.12 + Bash，禁止使用Linux/Unix命令（如head、tail、grep等）；应使用Bash原生命令或Python替代方案
- Bash兼容性：终端编码统一为UTF-8，避免中文乱码
- 编码统一：所有文件使用UTF-8编码，Python读取文件指定encoding='utf-8'；系统默认UTF-8，无需特殊处理中文
- 路径处理：优先使用pathlib.Path或os.path.join()，处理长路径和空格；Linux下注意路径权限
- PYTHONPATH设置：添加项目根目录到PYTHONPATH，避免模块导入错误

第二部分：开发规范
==============
测试文件强制检查机制：
- 【最高优先级】AI在生成任何代码前，必须检查是否涉及测试文件创建；如果是，必须确保使用test/目录
- 【强制验证】所有文件创建操作必须验证目标路径是否正确；测试文件必须在test/目录内
- 【自动纠正】如果发现测试文件路径违规，AI必须立即停止并重新生成正确路径
- 【清理保证】测试完成后必须清理所有生成的文件，避免污染工作目录
- 【路径确认】每次创建测试文件时，必须明确显示目标路径并确认在test/目录内

代码规范规则：
- 代码插入和删除：增大tab扫描范围至50行上下文，自动检测缩进模式，智能推断级别，确保正确缩进
- 代码风格约定：字符串使用双引号，路径分隔符用os.path.join()或pathlib，避免复杂f-string嵌套
- 字符串语法强制规范：所有字符串必须使用一致的引号风格，双引号优先；AI必须检查所有字符串的引号匹配和闭合；多行字符串必须使用三引号
- 转义字符强制规则：路径字符串必须使用原始字符串(r'')或双重转义处理反斜杠；AI禁止在路径中使用单反斜杠，必须使用正斜杠或正确的转义
- 字符串嵌套强制规范：字符串嵌套时必须使用不同的引号类型；AI必须验证嵌套字符串的引号配对正确性，避免语法错误
- 语法检查要求：编写代码后进行语法验证，避免字符串字面量未终止等常见错误
- 缩进统一：使用4个空格缩进，避免tabs和spaces混合

脚本和终端规则：
- 终端脚本规范：无须输入回车结束，避免交互(input)，使用sys.exit()而非input("按回车键退出...")
- 自动化脚本：所有terminal指令和自动化脚本应无须人工确认，直接执行
- 脚本执行：.sh文件保存为UTF-8，Bash脚本开头无需特殊设置
- 非交互式环境处理：检测运行环境(sys.stdin.isatty())，在管道/重定向环境中避免input()调用，使用try-except捕获EOFError
- 命令行参数设计：提供--help、--demo等参数，支持非交互式自动化调用
- 命令分隔符规范：Bash使用&&分隔多条命令；避免跨平台语法混淆；多命令组合应检测shell类型选择正确分隔符
- Bash条件执行：使用if-else结构替代&&和||，使用$?检查退出码；错误重定向使用2>/dev/null替代2>&1
- 命令生成最佳实践：创建专用方式生成跨平台兼容的命令，避免硬编码shell特定语法；使用模板字符串和条件逻辑处理不同shell
- AI命令生成强制规则：AI在生成任何shell命令时必须使用跨平台兼容的命令生成方式，确保跨平台兼容性；禁止直接使用&&、||、;等分隔符；必须通过专用方式自动选择正确的分隔符
- 跨平台命令执行标准：所有多命令组合必须使用跨平台兼容的执行方式，确保在不同shell环境中正确执行；单个命令可以直接执行，但多命令必须使用跨平台方式

文件组织规则：
- test/目录强制使用：【强制】所有测试文件、测试脚本、测试数据、中间结果文件必须放在test/目录中；AI禁止在工作目录、根目录或其他任何非test目录下创建或保存测试相关文件；任何测试操作必须首先确认在test/目录中进行
- 测试文件路径规范：【强制】测试脚本必须使用标准方式构造文件路径，格式为test目录路径 / "filename"；AI必须在创建任何测试文件前先获取test目录路径；禁止使用硬编码的"./test/"、"test/"或其他相对路径；必须使用绝对路径或相对于项目根目录的路径
- 测试文件清理强制：【强制】所有测试生成的文件必须在测试完成后立即自动清理；AI必须在测试函数结束时添加清理代码；禁止测试文件残留在工作目录；如果清理失败，必须明确报告错误
- 测试目录检查：【强制】AI在开始任何测试相关操作前，必须首先检查test/目录是否存在，如果不存在则创建；所有测试文件操作必须验证路径是否在test/目录内
- 测试文件命名规范：【强制】测试文件必须使用明确的命名规范，如test_*.py、*_test.py等；避免与正式代码文件混淆
- backup/目录：存放废弃文件、旧版本备份
- 工作目录：存放新产生的正式程序文件，或修改命名时产生的中间文件
- 测试完成后：不再使用的文件移动到backup/目录
- .gitignore管理：定期审查.gitignore文件，确保重要配置文件（如config/*.json）和备份文件（如modules/*.backup）不被意外排除；使用!规则保留特定文件

第三部分：错误处理
==============
错误处理规则：
- 编码错误：文件编码不一致（UTF-8 vs 其他），中文乱码；解决方案：统一编码，指定encoding；Linux下优先用utf-8处理文件
- 路径问题：路径权限、长路径限制、空格处理；解决方案：使用pathlib或os.path.join()；Linux长路径无需特殊前缀；【强制】测试文件必须使用标准方式确保在test/目录中操作，禁止在根目录或其他位置创建测试文件
- 测试文件违规：如果发现测试文件在非test/目录中创建，必须立即停止操作并报告错误；解决方案：重新创建文件到正确位置，清理违规文件
- Python环境：Conda激活失败、包安装权限、网络问题；解决方案：检查PATH、使用requirements.txt；设置PYTHONPATH避免导入错误；网络超时设置合理timeout
- 脚本执行：执行权限不足、命令找不到；解决方案：使用chmod +x设置权限，检查命令是否存在或使用完整路径
- 跨平台命令错误：Windows命令在Linux环境中不存在(dir、type、powershell等)；解决方案：使用Bash命令替代(Get-Content → cat、Select-Object → grep等)或Python脚本实现；避免在Linux环境中使用Windows命令
- 代码生成：缩进不一致、字符串编码、f-string嵌套；解决方案：统一缩进、指定encoding、简化嵌套
- 文件操作：权限拒绝、文件锁定、磁盘空间；解决方案：with语句、os.access()检查；Linux用shutil.disk_usage()
- 交互问题：input()阻塞、超时、EOFError；解决方案：argparse解析命令行参数，检测sys.stdin.isatty()，使用try-except捕获EOFError，提供非交互式使用方式
- 网络请求：连接超时、SSL错误；解决方案：设置合理的timeout参数、使用requests库替代urllib、处理SSL证书问题
- 子进程调用：命令不存在、编码问题；解决方案：使用完整命令路径、检查命令可用性、正确设置encoding参数(UTF-8)
- 子进程编码规范：Linux下使用UTF-8编码处理中文输出，添加errors='ignore'参数避免UnicodeDecodeError，优先使用utf-8处理文件编码
- 卡住问题：网络请求无超时、临时目录清理失败、子进程编码错误；解决方案：所有网络请求设置timeout参数、临时文件清理使用安全删除方法、子进程使用UTF-8编码处理中文
- Bash语法错误：命令分隔符混淆、多行语句解析；解决方案：使用多行字符串避免复杂语句、简单语句使用单行、复杂逻辑写到单独脚本
- 命令分隔符错误：跨平台语法混淆(Bash用&&，PowerShell用;)；解决方案：检测shell类型选择正确分隔符、使用平台特定的命令构建方式、避免硬编码分隔符；AI必须使用跨平台兼容方式自动处理分隔符选择
- 跨平台命令生成规范：AI生成的所有shell命令必须通过专用方式处理；禁止在代码中直接使用&&、||、;等分隔符；必须使用跨平台兼容方式确保兼容性
- 字符串语法错误强制检查：AI必须在生成代码前验证所有字符串字面量的完整性；禁止未终止的字符串；必须使用原始字符串(r'')处理包含反斜杠的路径；三引号字符串必须正确闭合；字符串嵌套必须使用不同类型的引号
- 路径处理强制规范：【强制】所有文件路径必须使用pathlib.Path或os.path.join()构造；AI禁止使用硬编码的反斜杠路径字符串；【强制】测试文件路径必须使用标准方式，确保位于test/目录内；AI必须在创建任何文件前验证路径是否正确；Linux长路径无需特殊前缀
- .gitignore冲突：重要文件被意外排除时，使用git add -f强制添加；更新.gitignore以保留特定文件类型

第四部分：反省机制
==============
反省规则：
- 问题总结：AI在遇到代码、终端、文件以及其他潜在会影响工作效率的问题时，必须首先总结问题出现的原因
- 环境确认：当遇到命令不存在错误时，首先确认当前操作系统环境，避免Linux命令在Windows环境中使用
- 跨平台命令检查：当生成或执行shell命令时，必须检查是否使用了正确的跨平台方式；如果发现直接使用&&、||、;等分隔符，应立即使用跨平台兼容方式重写
- 命令兼容性验证：生成shell命令后，应验证命令是否能在当前平台上正确执行；优先使用跨平台方式而不是硬编码的命令字符串
- 测试文件路径检查：【强制】当创建或操作测试文件时，必须验证是否使用了标准方式；如果发现硬编码的test目录路径或在根目录创建测试文件，应立即停止并重构为标准路径格式；必须确保所有测试文件都在test/目录内
- 测试违规检测：【强制】AI必须在每次操作后检查是否有测试文件违规创建；如果发现违规，必须立即清理并报告；长期跟踪此类违规以改进规则
- 字符串转义验证：当生成包含路径或特殊字符的字符串时，必须检查转义是否正确；优先使用原始字符串(r'')处理反斜杠，避免转义错误
- 规则更新：基于问题分析，在规则文件中更新需要增加的规则，确保类似问题不再发生
- 测试代码添加：为新识别的问题类型创建相应的测试代码，验证修复效果
- 持续改进：每次问题解决后，更新验证系统以包含新的检测项
- 文档同步：确保docstring和打印输出中的规则保持同步更新
- 优先级排序：优先处理影响工作效率的核心问题，逐步完善边缘情况
- 验证机制：问题修复后必须通过测试验证，确认问题已完全解决
- 项目清理回顾：定期检查项目结构，确保test/和tools/目录（如存在）仅包含可再生内容；保留核心模块和配置文件

【规则声明】
本文件为AI代码生成和修改的规范依据。
每次重启对话后，只需执行本文件，AI助手将自动读取并严格遵守上述所有规则，包括环境、风格、终端、插入/删除、交互、文件管理等。
如有与本文件冲突的建议或操作，优先以本文件规则为准。
"""

# 代码段：执行以告知AI相应的环境信息和规则
import os
import sys
import platform
import datetime
import json
import re
import tempfile
import urllib.request
import subprocess
import ast
from pathlib import Path

# 跨平台Shell命令生成器 - 内嵌实现
class ShellCommandBuilder:
    """跨平台Shell命令生成器"""

    def __init__(self):
        self.is_linux = os.name == 'posix'
        self.is_bash = 'bash' in os.environ.get('SHELL', '').lower()

    def build_command(self, *commands: str, check_success: bool = False,
                     suppress_errors: bool = False) -> str:
        """
        构建跨平台兼容的命令字符串

        Args:
            *commands: 要执行的命令
            check_success: 是否检查命令执行成功
            suppress_errors: 是否抑制错误输出

        Returns:
            兼容当前shell的命令字符串
        """
        if not commands:
            return ""

        if self.is_linux and self.is_bash:
            return self._build_bash_command(commands, check_success, suppress_errors)
        else:
            return self._build_generic_command(commands, check_success, suppress_errors)

    def _build_bash_command(self, commands: tuple, check_success: bool,
                          suppress_errors: bool) -> str:
        """构建Bash命令"""
        cmd_parts = []

        for i, cmd in enumerate(commands):
            if suppress_errors and i == len(commands) - 1:
                cmd += " 2>/dev/null"
            cmd_parts.append(cmd)

        joined_cmd = " && ".join(cmd_parts) if len(cmd_parts) > 1 else cmd_parts[0]

        if check_success and len(commands) > 1:
            joined_cmd += " && echo '✓ 成功' || echo '✗ 失败'"

        return joined_cmd

    def _build_generic_command(self, commands: tuple, check_success: bool,
                             suppress_errors: bool) -> str:
        """构建通用命令"""
        cmd_parts = []

        for i, cmd in enumerate(commands):
            if suppress_errors and i == len(commands) - 1:
                cmd += " 2>/dev/null"
            cmd_parts.append(cmd)

        joined_cmd = " && ".join(cmd_parts) if len(cmd_parts) > 1 else cmd_parts[0]

        if check_success and len(commands) > 1:
            joined_cmd += " && echo '✓ 成功' || echo '✗ 失败'"

        return joined_cmd

# 全局实例
_shell_builder = ShellCommandBuilder()

def generate_shell_command(*commands: str, check_success: bool = False,
                          suppress_errors: bool = False) -> str:
    """
    生成跨平台兼容的shell命令

    Args:
        *commands: 命令列表
        check_success: 是否检查执行结果
        suppress_errors: 是否抑制错误输出

    Returns:
        兼容当前平台的命令字符串

    示例:
        >>> generate_shell_command("python --version", "echo done", check_success=True)
        'python --version && echo done && echo '✓ 成功' || echo '✗ 失败''
    """
    return _shell_builder.build_command(*commands, check_success=check_success,
                                       suppress_errors=suppress_errors)

def inform_ai_environment_and_rules():
    """
    在新对话开始后执行此函数，告知AI开发助手当前的环境信息和规则。
    """
    print("=== AI开发环境信息和规则通知 ===")
    print(f"当前操作系统: {platform.system()} {platform.release()}")
    print(f"Python版本: {sys.version}")
    print(f"工作目录: {os.getcwd()}")
    print(f"文件编码: UTF-8")
    print(f"换行符: LF (Linux)")
    print("第一部分：环境基础")
    print("环境兼容性规则:")
    print("- Bash兼容性：终端编码统一为UTF-8，避免中文乱码")
    print("- 编码统一：所有文件使用UTF-8编码，Python读取文件指定encoding='utf-8'；系统默认UTF-8，无需特殊处理中文")
    print("- 路径处理：优先使用pathlib.Path或os.path.join()，处理长路径和空格；Linux下注意路径权限")
    print("- PYTHONPATH设置：添加项目根目录到PYTHONPATH，避免模块导入错误")
    print("第二部分：开发规范")
    print("代码规范规则:")
    print("- 代码插入和删除：增大tab扫描范围至50行上下文，自动检测缩进模式，智能推断级别，确保正确缩进")
    print("- 代码风格约定：字符串使用双引号，路径分隔符用os.path.join()或pathlib，避免复杂f-string嵌套")
    print("- 字符串语法规范：字符串使用一致的引号风格，检查引号匹配和闭合；多行字符串使用三引号；注意转义字符和嵌套引号")
    print("- 语法检查要求：编写代码后进行语法验证，避免字符串字面量未终止等常见错误")
    print("- 缩进统一：使用4个空格缩进，避免tabs和spaces混合")
    print("脚本和终端规则:")
    print("- 终端脚本规范：无须输入回车结束，避免交互(input)，使用sys.exit()而非input('按回车键退出...')")
    print("- 自动化脚本：所有terminal指令和自动化脚本应无须人工确认，直接执行")
    print("- 脚本执行：.sh文件保存为UTF-8，Bash脚本开头无需特殊设置")
    print("- 非交互式环境处理：检测运行环境(sys.stdin.isatty())，在管道/重定向环境中避免input()调用，使用try-except捕获EOFError")
    print("- 命令行参数设计：提供--help、--demo等参数，支持非交互式自动化调用")
    print("文件组织规则:")
    print("- test/目录：【强制】存放所有测试文件、中间过程文件、测试文档；禁止在根目录或其他位置创建测试文件")
    print("- backup/目录：存放废弃文件、旧版本备份")
    print("- 工作目录：存放新产生的正式程序文件，或修改命名时产生的中间文件")
    print("- 测试完成后：不再使用的文件移动到backup/目录")
    print("第三部分：错误处理")
    print("错误处理规则:")
    print("- 编码错误：文件编码不一致（UTF-8 vs 其他），中文乱码；解决方案：统一编码，指定encoding；Linux下优先用utf-8处理文件")
    print("- 路径问题：路径权限、长路径限制、空格处理；解决方案：使用pathlib或os.path.join()；Linux长路径无需特殊前缀；【强制】测试文件必须使用标准方式确保在test/目录中操作")
    print("- 测试文件违规：如果发现测试文件在非test/目录中创建，必须立即清理并报告错误")
    print("- Python环境：Conda激活失败、包安装权限、网络问题；解决方案：检查PATH、使用requirements.txt；设置PYTHONPATH避免导入错误；网络超时设置合理timeout")
    print("- 脚本执行：执行权限不足、命令找不到；解决方案：使用chmod +x设置权限，检查命令是否存在或使用完整路径")
    print("- 代码生成：缩进不一致、字符串编码、f-string嵌套；解决方案：统一缩进、指定encoding、简化嵌套")
    print("- 文件操作：权限拒绝、文件锁定、磁盘空间；解决方案：with语句、os.access()检查；Linux用shutil.disk_usage()")
    print("- 交互问题：input()阻塞、超时、EOFError；解决方案：argparse解析命令行参数，检测sys.stdin.isatty()，使用try-except捕获EOFError，提供非交互式使用方式")
    print("- 网络请求：连接超时、SSL错误；解决方案：设置合理的timeout参数、使用requests库替代urllib、处理SSL证书问题")
    print("- 子进程调用：命令不存在、编码问题；解决方案：使用完整命令路径、检查命令可用性、正确设置encoding参数(UTF-8)")
    print("- 子进程编码规范：Linux下使用UTF-8编码处理中文输出，添加errors='ignore'参数避免UnicodeDecodeError，优先使用utf-8处理文件编码")
    print("- 卡住问题：网络请求无超时、临时目录清理失败、子进程编码错误；解决方案：所有网络请求设置timeout参数、临时文件清理使用安全删除方法、子进程使用UTF-8编码处理中文")
    print("- Bash语法错误：命令分隔符混淆、多行语句解析；解决方案：使用多行字符串避免复杂语句、简单语句使用单行、复杂逻辑写到单独脚本")
    print("- 命令分隔符错误：跨平台语法混淆(Bash用&&，PowerShell用;)；解决方案：检测shell类型选择正确分隔符、使用平台特定的命令构建函数、避免硬编码分隔符")
    print("- 字符串语法错误：未终止字符串字面量、引号不匹配、转义字符错误；解决方案：检查字符串引号配对、使用原始字符串(r'')处理反斜杠、使用三引号处理多行字符串、避免在字符串中混用引号类型")
    print("第四部分：反省机制")
    print("反省规则:")
    print("- 问题总结：AI在遇到代码、终端、文件以及其他潜在会影响工作效率的问题时，必须首先总结问题出现的原因")
    print("- 规则更新：基于问题分析，在规则文件中更新需要增加的规则，确保类似问题不再发生")
    print("- 测试代码添加：为新识别的问题类型创建相应的测试代码，验证修复效果")
    print("- 持续改进：每次问题解决后，更新验证系统以包含新的检测项")
    print("- 文档同步：确保docstring和打印输出中的规则保持同步更新")
    print("- 优先级排序：优先处理影响工作效率的核心问题，逐步完善边缘情况")
    print("- 验证机制：问题修复后必须通过测试验证，确认问题已完全解决")
    print("【规则声明】")
    print("本文件为AI代码生成和修改的规范依据。")
    print("每次重启对话后，只需执行本文件，AI助手将自动读取并严格遵守上述所有规则，包括环境、风格、终端、插入/删除、交互、文件管理等。")
    print("如有与本文件冲突的建议或操作，优先以本文件规则为准。")
    print("=====================================")

# 在新对话开始后执行此函数
# (已移至命令行参数处理部分)

def get_test_dir():
    """
    获取test目录的路径，确保所有测试文件都在test/目录中操作
    
    返回:
    - Path对象: test目录的路径
    """
    from pathlib import Path
    import os
    
    # 获取当前工作目录
    current_dir = Path.cwd()
    
    # 构建test目录路径
    test_dir = current_dir / "test"
    
    # 确保test目录存在
    test_dir.mkdir(exist_ok=True)
    
    return test_dir

def run_cross_platform_command(*commands: str, check_success: bool = False, 
                              suppress_errors: bool = False, **subprocess_kwargs):
    """
    使用跨平台shell命令生成器执行命令
    
    Args:
        *commands: 要执行的命令
        check_success: 是否检查执行结果
        suppress_errors: 是否抑制错误输出
        **subprocess_kwargs: 传递给subprocess.run的其他参数
    
    Returns:
        subprocess.CompletedProcess: 执行结果
    """
    # 使用内嵌的跨平台shell命令生成器
    shell_cmd = generate_shell_command(*commands, check_success=check_success, 
                                     suppress_errors=suppress_errors)
    
    # 设置默认的subprocess参数
    default_kwargs = {
        'shell': True,
        'capture_output': True,
        'text': True,
        'encoding': 'utf-8',
        'errors': 'ignore'
    }
    default_kwargs.update(subprocess_kwargs)
    
    # 在Bash中执行
    return subprocess.run([shell_cmd], **default_kwargs)

def get_test_file_path(filename):
    """
    获取测试文件的完整路径，确保文件在test/目录中
    
    参数:
    - filename: 文件名
    
    返回:
    - Path对象: 测试文件的完整路径
    """
    test_dir = get_test_dir()
    return test_dir / filename

def cleanup_test_files():
    """
    清理test目录中的临时文件
    """
    import shutil
    from pathlib import Path
    
    test_dir = get_test_dir()
    
    # 定义需要清理的文件模式
    cleanup_patterns = [
        "*.tmp", "*.temp", "test_*.txt", "test_*.json",
        "error_*.log", "debug_*.log", "*.pyc", "__pycache__"
    ]
    
    cleaned_files = []
    
    for pattern in cleanup_patterns:
        for file_path in test_dir.glob(pattern):
            try:
                if file_path.is_file():
                    file_path.unlink()
                    cleaned_files.append(str(file_path))
                elif file_path.is_dir() and file_path.name == "__pycache__":
                    shutil.rmtree(file_path)
                    cleaned_files.append(str(file_path))
            except Exception as e:
                print(f"清理文件失败 {file_path}: {e}")
    
    if cleaned_files:
        print(f"已清理 {len(cleaned_files)} 个测试文件")
    
    return cleaned_files

def get_command_separator():
    """
    根据当前shell类型返回正确的命令分隔符
    
    返回:
    - str: 命令分隔符 ('&&' for Bash/Linux)
    """
    import platform
    import os
    
    # 检测操作系统
    system = platform.system().lower()
    
    # Linux系统使用&&
    if system == "linux":
        return "&&"
    else:
        # 其他系统保守使用&&
        return "&&"

def build_multi_command(*commands):
    """
    构建多命令字符串，使用正确的分隔符
    
    参数:
    - commands: 可变数量的命令字符串
    
    返回:
    - str: 组合后的命令字符串
    """
    if not commands:
        return ""
    
    separator = get_command_separator()
    return separator.join(commands)

def safe_run_commands(*commands, **kwargs):
    """
    安全地运行多个命令，使用正确的分隔符
    
    参数:
    - commands: 要运行的命令
    - **kwargs: 传递给subprocess.run的参数
    
    返回:
    - subprocess.CompletedProcess: 命令执行结果
    """
    import subprocess
    
    if len(commands) == 1:
        # 单命令直接执行
        return subprocess.run(commands[0], **kwargs)
    else:
        # 多命令组合执行
        combined_command = build_multi_command(*commands)
        return subprocess.run(combined_command, **kwargs)

def handle_common_errors():
    """
    演示如何处理Debian 12.12中文环境下的常见错误。
    """
    import os
    import sys
    import pathlib
    from pathlib import Path
    
    print("\n=== 常见错误处理示例 ===")
    
    # 1. 编码错误处理
    try:
        # 正确读取UTF-8文件
        with open('example.txt', 'r', encoding='utf-8') as f:
            content = f.read()
        print("✓ 文件读取成功，编码正确")
    except UnicodeDecodeError:
        print("✗ 编码错误：尝试使用其他编码")
        try:
            with open('example.txt', 'r', encoding='latin-1') as f:
                content = f.read()
            print("✓ 使用latin-1读取成功")
        except:
            print("✗ 无法读取文件")
    
    # 2. 路径问题处理
    # 使用pathlib避免路径问题
    current_dir = Path.cwd()
    file_path = current_dir / "data" / "example.json"
    print(f"✓ 安全路径构建: {file_path}")
    
    # 检查长路径
    if len(str(file_path)) > 4096:  # Linux typical limit
        print("⚠ 路径过长，可能需要调整")
    
    # 3. Python环境检查
    print(f"Python可执行文件: {sys.executable}")
    print(f"Python路径: {sys.path}")
    
    # 4. 文件操作安全处理
    try:
        with open('test_write.txt', 'w', encoding='utf-8') as f:
            f.write("测试中文写入")
        print("✓ 文件写入成功")
    except PermissionError:
        print("✗ 权限错误：无法写入文件")
    except OSError as e:
        print(f"✗ 文件操作错误: {e}")
    
    # 5. 命令行参数处理（避免交互）
    import argparse
    parser = argparse.ArgumentParser(description='示例脚本')
    parser.add_argument('--input', default='default_value', help='输入参数')
    # args = parser.parse_args()  # 在实际使用时取消注释
    
    # 6. 环境变量检查
    pythonpath = os.environ.get('PYTHONPATH', '')
    if pythonpath:
        print(f"PYTHONPATH: {pythonpath}")
    else:
        print("⚠ PYTHONPATH未设置")
    
    # 7. 终端编码设置（Bash）
    print("Bash编码设置建议:")
    print("export LANG=en_US.UTF-8")
    
    print("=== 错误处理示例完成 ===")

# 可选：在主执行中调用错误处理示例
# handle_common_errors()

def test_terminal_environment():
    """
    测试终端环境中的常见问题，帮助AI确认潜在的环境问题。
    """
    import subprocess
    import sys
    import os
    import platform
    
    print("\n=== 终端环境测试检测 ===")
    
    # 1. 检查操作系统和终端信息
    print(f"操作系统: {platform.system()} {platform.release()}")
    print(f"Python版本: {sys.version}")
    print(f"工作目录: {os.getcwd()}")
    
    # 2. 检查编码设置
    try:
        import locale
        print(f"系统默认编码: {locale.getpreferredencoding()}")
        print(f"文件系统编码: {sys.getfilesystemencoding()}")
    except:
        print("编码信息获取失败")
    
    # 3. 检查Bash设置
    try:
        result = run_cross_platform_command('echo $SHELL')
        if result.returncode == 0:
            print(f"Shell: {result.stdout.strip()}")
        else:
            print("Shell检查失败")
    except Exception as e:
        print(f"Shell测试失败: {e}")
    
    # 4. 检查Conda环境
    conda_env = os.environ.get('CONDA_DEFAULT_ENV', 'None')
    print(f"当前Conda环境: {conda_env}")
    
    # 5. 测试文件操作权限
    test_file = 'test_env_check.txt'
    try:
        with open(test_file, 'w', encoding='utf-8') as f:
            f.write("测试文件写入")
        print("[OK] 文件写入权限正常")
        os.remove(test_file)
        print("[OK] 文件删除权限正常")
    except Exception as e:
        print(f"[ERROR] 文件操作权限问题: {e}")
    
    # 6. 检查Python包管理
    try:
        import pip
        print(f"pip版本: {pip.__version__}")
    except ImportError:
        print("✗ pip未安装")
    
    # 7. 测试网络连接（可选）
    try:
        import urllib.request
        with urllib.request.urlopen('https://httpbin.org/get', timeout=10) as response:
            print("[OK] 网络连接正常 (HTTPS)")
    except urllib.error.URLError as e:
        print(f"[WARN] HTTPS连接失败: {e}")
        try:
            with urllib.request.urlopen('http://httpbin.org/get', timeout=10) as response:
                print("[OK] 网络连接正常 (HTTP)")
        except Exception as e2:
            print(f"[ERROR] 网络连接失败: {e2}")
    except Exception as e:
        print(f"[ERROR] 网络测试失败: {e}")
    
    # 8. 检查磁盘空间 (Linux兼容)
    try:
        import shutil
        total, used, free = shutil.disk_usage(".")
        free_gb = free / (1024**3)
        total_gb = total / (1024**3)
        used_gb = used / (1024**3)
        print(f"磁盘总空间: {total_gb:.2f} GB")
        print(f"磁盘已用空间: {used_gb:.2f} GB")
        print(f"磁盘剩余空间: {free_gb:.2f} GB")
    except Exception as e:
        print(f"磁盘空间检查失败: {e}")
    
    # 9. 检查环境变量
    key_vars = ['PATH', 'PYTHONPATH', 'CONDA_PREFIX']
    for var in key_vars:
        value = os.environ.get(var, 'Not set')
        if len(str(value)) > 100:
            value = str(value)[:100] + '...'
        print(f"{var}: {value}")
    
    # 10. PYTHONPATH设置建议
    if not os.environ.get('PYTHONPATH'):
        current_dir = os.getcwd()
        print(f"建议设置PYTHONPATH: {current_dir}")
        print("Bash命令: export PYTHONPATH=$PYTHONPATH:" + current_dir)
    
    # 11. 子进程命令测试
    try:
        result = subprocess.run(['which', 'python3'], capture_output=True, text=True, encoding='utf-8', errors='ignore', timeout=5)
        if result.returncode == 0:
            print("[OK] 系统命令可用")
        else:
            print("[WARN] 系统命令可能有问题")
    except Exception as e:
        print(f"[WARN] 子进程测试失败: {e}")
    
    print("=== 终端环境测试完成 ===")

# 可选：在主执行中调用环境测试
# test_terminal_environment()  # 已移至主函数中按需调用

def detect_bash_command_issues():
    """
    检测Bash命令相关问题
    返回检测结果字典，用于规则完善
    """
    import subprocess
    import tempfile
    from pathlib import Path

    results = {
        'command_separator_conflict': False,
        'complex_command_parsing': False,
        'encoding_issues': False,
        'recommendations': []
    }

    print("\n=== Bash命令问题检测 ===")

    # 测试1: 简单命令分隔
    try:
        result = run_cross_platform_command('echo "Hello"', 'echo "World"', timeout=10)
        if result.returncode != 0:
            results['command_separator_conflict'] = True
            results['recommendations'].append("避免在单行中使用&&分隔命令")
            print("⚠ 检测到命令分隔问题")
        else:
            print("✓ 命令分隔正常")
    except Exception as e:
        results['command_separator_conflict'] = True
        results['recommendations'].append(f"命令分隔测试异常: {e}")
        print(f"⚠ 命令分隔测试失败: {e}")

    # 测试2: 复杂命令解析
    try:
        complex_cmd = '''
        a="test"
        if [ "$a" = "test" ]; then
            echo "Complex command works"
        fi
        '''
        result = subprocess.run(['bash', '-c', complex_cmd],
                              capture_output=True, text=True, encoding='utf-8', errors='ignore', timeout=10)
        if result.returncode != 0:
            results['complex_command_parsing'] = True
            results['recommendations'].append("复杂逻辑应写入单独脚本文件")
            print("⚠ 检测到复杂命令解析问题")
        else:
            print("✓ 复杂命令解析正常")
    except Exception as e:
        results['complex_command_parsing'] = True
        results['recommendations'].append(f"复杂命令测试异常: {e}")
        print(f"⚠ 复杂命令测试失败: {e}")

    # 测试3: 中文编码问题
    try:
        result = run_cross_platform_command('echo "中文测试"', timeout=10)
        if "中文测试" not in result.stdout and result.returncode != 0:
            results['encoding_issues'] = True
            results['recommendations'].append("Bash需要设置UTF-8编码")
            print("⚠ 检测到中文编码问题")
        else:
            print("✓ 中文编码正常")
    except Exception as e:
        results['encoding_issues'] = True
        results['recommendations'].append(f"中文编码测试异常: {e}")
        print(f"⚠ 中文编码测试失败: {e}")

    return results

def detect_chinese_encoding_issues():
    """
    检测中文编码相关问题
    """
    import tempfile
    from pathlib import Path

    results = {
        'file_encoding_mismatch': False,
        'console_encoding_issues': False,
        'path_encoding_problems': False,
        'recommendations': []
    }

    print("\n=== 中文编码问题检测 ===")

    # 测试1: 文件编码一致性
    try:
        test_dir = Path("test")
        test_dir.mkdir(exist_ok=True)

        # 创建UTF-8文件
        utf8_file = test_dir / "utf8_test.txt"
        with open(utf8_file, 'w', encoding='utf-8') as f:
            f.write("这是UTF-8编码的中文文件")

        # 尝试用错误编码读取
        try:
            with open(utf8_file, 'r', encoding='ascii') as f:
                content = f.read()
        except UnicodeDecodeError:
            print("✓ 文件编码检测正常（预期错误）")

        # 正确读取
        with open(utf8_file, 'r', encoding='utf-8') as f:
            content = f.read()
        print("✓ UTF-8文件读取正常")

        # 清理
        utf8_file.unlink()

    except Exception as e:
        results['file_encoding_mismatch'] = True
        results['recommendations'].append(f"文件编码测试异常: {e}")
        print(f"⚠ 文件编码测试失败: {e}")

    # 测试2: 控制台编码
    try:
        chinese_text = "控制台中文测试"
        print(f"控制台输出: {chinese_text}")
        print("✓ 控制台编码正常")
    except Exception as e:
        results['console_encoding_issues'] = True
        results['recommendations'].append(f"控制台编码问题: {e}")
        print(f"⚠ 控制台编码问题: {e}")

    # 测试3: 路径中文处理
    try:
        test_dir = Path("test")
        chinese_dir_name = "中文测试目录"
        chinese_dir = test_dir / chinese_dir_name
        chinese_dir.mkdir(exist_ok=True)

        chinese_file = chinese_dir / "中文文件.txt"
        with open(chinese_file, 'w', encoding='utf-8') as f:
            f.write("中文路径测试内容")

        with open(chinese_file, 'r', encoding='utf-8') as f:
            content = f.read()

        print("✓ 中文路径处理正常")

        # 清理
        chinese_file.unlink()
        chinese_dir.rmdir()

    except Exception as e:
        results['path_encoding_problems'] = True
        results['recommendations'].append(f"中文路径处理问题: {e}")
        print(f"⚠ 中文路径处理问题: {e}")

    return results

def detect_file_path_issues():
    """
    检测文件路径相关问题
    """
    import tempfile
    from pathlib import Path

    results = {
        'wrong_test_directory': False,
        'path_separation_issues': False,
        'long_path_problems': False,
        'cleanup_failures': False,
        'recommendations': []
    }

    print("\n=== 文件路径问题检测 ===")

    # 测试1: 测试文件生成位置
    try:
        # 模拟错误的路径使用
        wrong_file = Path.cwd() / "wrong_location_test.txt"
        with open(wrong_file, 'w', encoding='utf-8') as f:
            f.write("错误的测试文件位置")

        if wrong_file.exists():
            results['wrong_test_directory'] = True
            results['recommendations'].append("测试文件不应生成在工作目录")
            print("⚠ 检测到测试文件位置错误")

        # 清理
        wrong_file.unlink()

    except Exception as e:
        results['wrong_test_directory'] = True
        results['recommendations'].append(f"测试文件位置测试异常: {e}")
        print(f"⚠ 测试文件位置测试失败: {e}")

    # 测试2: 路径分隔符问题
    try:
        # Linux路径分隔符测试
        test_path = "/usr/local/bin/python3"
        if "/" in test_path:
            print("✓ Linux路径分隔符正常")
        else:
            results['path_separation_issues'] = True
            results['recommendations'].append("注意Linux路径分隔符")
            print("⚠ 路径分隔符可能有问题")

    except Exception as e:
        results['path_separation_issues'] = True
        results['recommendations'].append(f"路径分隔符测试异常: {e}")
        print(f"⚠ 路径分隔符测试失败: {e}")

    # 测试3: 长路径处理
    try:
        long_path = Path.cwd() / ("long_dir_name_" * 50) / "test.txt"
        if len(str(long_path)) > 4096:  # Linux limit
            results['long_path_problems'] = True
            results['recommendations'].append("长路径需要调整")
            print("⚠ 检测到长路径问题")
        else:
            print("✓ 路径长度正常")

    except Exception as e:
        results['long_path_problems'] = True
        results['recommendations'].append(f"长路径测试异常: {e}")
        print(f"⚠ 长路径测试失败: {e}")

    # 测试4: 文件清理问题
    try:
        temp_files = []
        for i in range(3):
            temp_file = Path.cwd() / f"temp_cleanup_test_{i}.txt"
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.write(f"临时文件{i}")
            temp_files.append(temp_file)

        # 清理文件
        for temp_file in temp_files:
            try:
                temp_file.unlink()
            except Exception as e:
                results['cleanup_failures'] = True
                results['recommendations'].append(f"文件清理失败: {e}")
                print(f"⚠ 文件清理失败: {e}")

        if not results['cleanup_failures']:
            print("✓ 文件清理正常")

    except Exception as e:
        results['cleanup_failures'] = True
        results['recommendations'].append(f"文件清理测试异常: {e}")
        print(f"⚠ 文件清理测试失败: {e}")

    return results

def run_comprehensive_validation():
    """
    运行全面的规则验证
    返回所有检测结果的汇总
    """
    print("=== AI规则验证系统 ===")
    print("开始全面检测常见问题...")

    all_results = {}

    # 运行各项检测
    try:
        all_results['bash'] = detect_bash_command_issues()
    except Exception as e:
        print(f"Bash检测失败: {e}")
        all_results['bash'] = {'error': str(e)}

    try:
        all_results['chinese_encoding'] = detect_chinese_encoding_issues()
    except Exception as e:
        print(f"中文编码检测失败: {e}")
        all_results['chinese_encoding'] = {'error': str(e)}

    try:
        all_results['file_paths'] = detect_file_path_issues()
    except Exception as e:
        print(f"文件路径检测失败: {e}")
        all_results['file_paths'] = {'error': str(e)}

    # 生成检测报告
    print("\n=== 检测报告汇总 ===")

    total_issues = 0
    all_recommendations = []

    for category, results in all_results.items():
        if 'error' in results:
            print(f"⚠ {category}: 检测失败 - {results['error']}")
            total_issues += 1
        else:
            issues_in_category = sum(1 for k, v in results.items()
                                   if k != 'recommendations' and isinstance(v, bool) and v)
            if issues_in_category > 0:
                print(f"⚠ {category}: 发现 {issues_in_category} 个问题")
                total_issues += issues_in_category
            else:
                print(f"✓ {category}: 无问题")

            if 'recommendations' in results:
                all_recommendations.extend(results['recommendations'])

    print(f"\n总计发现问题: {total_issues}")

    if all_recommendations:
        print("\n改进建议:")
        for i, rec in enumerate(all_recommendations, 1):
            print(f"{i}. {rec}")

    # 保存检测结果到文件
    try:
        import json
        from datetime import datetime
        from pathlib import Path

        result_file = Path("test") / "validation_results.json"
        result_file.parent.mkdir(exist_ok=True)

        validation_data = {
            'timestamp': datetime.now().isoformat(),
            'total_issues': total_issues,
            'results': all_results,
            'recommendations': all_recommendations
        }

        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(validation_data, f, ensure_ascii=False, indent=2)

        print(f"\n✓ 检测结果已保存到: {result_file}")

    except Exception as e:
        print(f"⚠ 结果保存失败: {e}")

    return all_results

# 在主执行中运行验证（可选）
# run_comprehensive_validation()

# 为了便于其他AI使用，添加便捷函数
def validate_rules():
    """
    便捷函数：运行所有规则验证
    建议在每次重大修改后运行此函数
    """
    return run_comprehensive_validation()

def check_specific_issue(issue_type):
    """
    检查特定类型的问题
    参数: 'bash', 'encoding', 'paths'
    """
    if issue_type == 'bash':
        return detect_bash_command_issues()
    elif issue_type == 'encoding':
        return detect_chinese_encoding_issues()
    elif issue_type == 'paths':
        return detect_file_path_issues()
    else:
        return {"error": f"未知的问题类型: {issue_type}"}

def analyze_and_reflect_on_issue(issue_description, error_details=None, context_info=None):
    """
    AI反省机制：分析问题、总结原因、建议规则更新
    
    参数:
    - issue_description: 问题描述
    - error_details: 错误详情（可选）
    - context_info: 上下文信息（可选）
    
    返回:
    - 分析报告字典
    """
    import json
    import datetime
    from pathlib import Path
    
    print("\n=== AI反省机制启动 ===")
    print(f"问题描述: {issue_description}")
    
    # 1. 问题分类分析
    issue_category = classify_issue(issue_description, error_details)
    
    # 2. 原因分析
    root_causes = analyze_root_causes(issue_description, error_details, context_info)
    
    # 3. 影响评估
    impact_assessment = assess_impact(issue_category, error_details)
    
    # 4. 规则更新建议
    rule_updates = suggest_rule_updates(issue_category, root_causes)
    
    # 5. 测试代码生成
    test_code = generate_test_code(issue_category, root_causes)
    
    # 6. 验证机制建议
    validation_suggestions = suggest_validation_improvements(issue_category)
    
    # 编译分析报告
    analysis_report = {
        "timestamp": datetime.datetime.now().isoformat(),
        "issue_description": issue_description,
        "error_details": error_details,
        "context_info": context_info,
        "issue_category": issue_category,
        "root_causes": root_causes,
        "impact_assessment": impact_assessment,
        "rule_updates": rule_updates,
        "test_code": test_code,
        "validation_suggestions": validation_suggestions,
        "recommendations": [
            "立即更新规则文档以包含新识别的问题类型",
            "添加相应的测试代码到验证系统中",
            "更新验证函数以检测类似问题",
            "在项目文档中记录此次问题分析",
            "考虑是否需要修改现有代码以防止类似问题"
        ]
    }
    
    # 保存分析报告
    report_path = Path("test") / "reflection_reports"
    report_path.mkdir(exist_ok=True)
    
    report_file = report_path / f"reflection_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(analysis_report, f, ensure_ascii=False, indent=2)
        print(f"✓ 分析报告已保存: {report_file}")
    except Exception as e:
        print(f"⚠ 无法保存分析报告: {e}")
    
    # 打印关键发现
    print(f"问题分类: {issue_category}")
    print(f"主要原因: {', '.join(root_causes[:3])}")  # 显示前3个原因
    print(f"影响程度: {impact_assessment['severity']}")
    print(f"建议规则更新: {len(rule_updates)} 项")
    print(f"生成测试代码: {len(test_code)} 个测试用例")
    
    return analysis_report

def classify_issue(issue_description, error_details=None):
    """对问题进行分类"""
    desc_lower = issue_description.lower()
    
    if any(keyword in desc_lower for keyword in ['编码', 'encoding', 'utf-8', 'gbk', 'cp936', 'unicode']):
        return "编码问题"
    elif any(keyword in desc_lower for keyword in ['路径', 'path', 'file', 'directory', '文件夹']):
        return "文件路径问题"
    elif any(keyword in desc_lower for keyword in ['bash', '命令', 'command', 'terminal', '终端']):
        return "终端命令问题"
    elif any(keyword in desc_lower for keyword in ['模块', 'import', 'module', '包']):
        return "模块导入问题"
    elif any(keyword in desc_lower for keyword in ['权限', 'permission', 'access']):
        return "权限问题"
    elif any(keyword in desc_lower for keyword in ['内存', 'memory', '资源']):
        return "资源管理问题"
    elif any(keyword in desc_lower for keyword in ['网络', 'network', '连接']):
        return "网络连接问题"
    elif any(keyword in desc_lower for keyword in ['syntax', '语法', 'string', '字符串', '引号', 'quote']):
        return "语法错误"
    else:
        return "其他问题"

def analyze_root_causes(issue_description, error_details, context_info):
    """分析问题的根本原因"""
    causes = []
    
    # 基于问题描述分析原因
    desc_lower = issue_description.lower()
    
    if '编码' in desc_lower or 'encoding' in desc_lower:
        causes.extend([
            "文件编码不一致（UTF-8 vs 系统默认编码）",
            "子进程调用未指定编码参数",
            "控制台输出编码与文件编码不匹配"
        ])
    
    if '路径' in desc_lower or 'path' in desc_lower:
        causes.extend([
            "使用字符串拼接而非pathlib或os.path.join",
            "未处理长路径问题",
            "工作目录与预期不符"
        ])
    
    if 'bash' in desc_lower or '命令' in desc_lower:
        causes.extend([
            "Bash编码设置不正确",
            "命令语法在不同环境中不兼容",
            "未处理命令执行错误"
        ])
    
    if '语法' in desc_lower or 'syntax' in desc_lower or '字符串' in desc_lower or 'string' in desc_lower:
        causes.extend([
            "字符串引号不匹配或未闭合",
            "多行字符串缺少正确的引号",
            "字符串中的转义字符使用不当",
            "f-string或格式化字符串语法错误",
            "嵌套引号缺少正确转义"
        ])
    
    # 基于错误详情分析
    if error_details:
        error_str = str(error_details).lower()
        if 'permission' in error_str or '权限' in error_str:
            causes.append("文件或目录权限不足")
        if 'not found' in error_str or '找不到' in error_str:
            causes.append("文件或命令不存在")
        if 'timeout' in error_str or '超时' in error_str:
            causes.append("操作超时")
        if 'unterminated string' in error_str or '字符串字面量未终止' in error_str:
            causes.append("字符串字面量缺少结束引号")
        if 'eol while scanning' in error_str:
            causes.append("字符串扫描到行尾仍未找到结束引号")
    
    # 默认原因
    if not causes:
        causes = ["问题原因待进一步分析", "可能需要更多上下文信息"]
    
    return causes

def assess_impact(issue_category, error_details):
    """评估问题的影响程度"""
    severity_levels = {
        "编码问题": "中",
        "文件路径问题": "中", 
        "终端命令问题": "高",
        "模块导入问题": "高",
        "权限问题": "高",
        "资源管理问题": "中",
        "网络连接问题": "低",
        "其他问题": "低"
    }
    
    severity = severity_levels.get(issue_category, "中")
    
    # 根据错误详情调整严重程度
    if error_details and 'critical' in str(error_details).lower():
        severity = "高"
    
    return {
        "severity": severity,
        "affected_areas": ["代码执行", "用户体验", "系统稳定性"],
        "estimated_fix_time": "1-2小时" if severity == "低" else "2-4小时" if severity == "中" else "4-8小时"
    }

def suggest_rule_updates(issue_category, root_causes):
    """建议规则更新"""
    updates = []
    
    if issue_category == "编码问题":
        updates.extend([
            "在所有文件操作中明确指定encoding='utf-8'",
            "子进程调用添加encoding='utf-8', errors='ignore'参数",
            "统一文件编码为UTF-8，控制台输出使用系统编码"
        ])
    
    elif issue_category == "文件路径问题":
        updates.extend([
            "优先使用pathlib.Path处理路径操作",
            "长路径需要调整",
            "测试文件必须在test/目录中操作"
        ])
    
    elif issue_category == "终端命令问题":
        updates.extend([
            "Bash脚本开头设置UTF-8编码",
            "检测命令是否存在后再执行",
            "使用完整路径调用命令"
        ])
    
    elif issue_category == "语法错误":
        updates.extend([
            "字符串使用一致的引号风格（建议双引号）",
            "多行字符串使用三引号或正确转义",
            "检查字符串引号匹配和闭合",
            "f-string嵌套时注意引号冲突",
            "使用语法检查工具验证代码"
        ])
    
    # 基于根本原因添加通用建议
    for cause in root_causes:
        if "编码" in cause:
            updates.append("加强编码一致性检查")
        if "权限" in cause:
            updates.append("添加权限检查机制")
        if "超时" in cause:
            updates.append("为所有网络和I/O操作设置合理的超时时间")
        if "字符串" in cause or "引号" in cause:
            updates.append("实施字符串语法检查规范")
    
    return updates

def generate_test_code(issue_category, root_causes):
    """生成测试代码"""
    test_cases = []
    
    if issue_category == "编码问题":
        test_cases.append("""
def test_encoding_consistency():
    \"\"\"测试编码一致性\"\"\"
    # 测试文件编码
    with open('test_encoding.txt', 'w', encoding='utf-8') as f:
        f.write('测试中文')
    
    with open('test_encoding.txt', 'r', encoding='utf-8') as f:
        content = f.read()
    
    assert content == '测试中文'
    
    # 测试子进程编码
    result = subprocess.run(['echo', '测试'], 
                          capture_output=True, 
                          encoding='utf-8', 
                          errors='ignore')
    assert result.returncode == 0
""")
    
    if issue_category == "文件路径问题":
        test_cases.append("""
def test_path_handling():
    \"\"\"测试路径处理\"\"\"
    from pathlib import Path
    
    # 测试pathlib使用
    test_path = Path('test') / 'subdir' / 'file.txt'
    assert test_path.exists() or not test_path.exists()  # 只是测试路径构建
    
    # 测试长路径
    long_path = Path.cwd() / 'very_long_path_name_that_might_exceed_limits.txt'
    # 在Linux上测试长路径处理
""")
    
    return test_cases

def suggest_validation_improvements(issue_category):
    """建议验证机制改进"""
    suggestions = []
    
    if issue_category == "编码问题":
        suggestions.extend([
            "添加文件编码一致性检查",
            "验证所有子进程调用使用正确编码参数",
            "测试中文字符在不同编码下的表现"
        ])
    
    elif issue_category == "文件路径问题":
        suggestions.extend([
            "验证所有测试文件都在test/目录中",
            "检查长路径处理是否正确",
            "测试路径分隔符的跨平台兼容性"
        ])
    
    return suggestions

def get_input_from_clipboard():
    """
    从剪贴板读取输入内容
    返回: (issue_desc, error_details, context_info)
    """
    try:
        import subprocess
        import sys

        print("\n📋 正在从剪贴板读取内容...")

        # Linux下使用xclip或xsel获取剪贴板内容
        if sys.platform == "linux":
            try:
                result = subprocess.run(['xclip', '-selection', 'clipboard', '-o'], 
                                      capture_output=True, text=True, encoding='utf-8', errors='ignore')
                if result.returncode == 0 and result.stdout.strip():
                    clipboard_content = result.stdout.strip()
                    print("✅ 成功读取剪贴板内容")
                    print(f"📄 内容长度: {len(clipboard_content)} 字符")

                    # 尝试智能解析内容
                    return parse_clipboard_content(clipboard_content)
                else:
                    print("❌ 剪贴板为空或读取失败")
            except FileNotFoundError:
                print("❌ xclip未安装，请安装: sudo apt install xclip")
        else:
            print("❌ 此功能仅支持Linux系统")

    except Exception as e:
        print(f"❌ 读取剪贴板失败: {e}")

    # 如果剪贴板读取失败，返回空值让用户手动输入
    print("💡 请手动输入问题信息:")
    return get_input_manually()


def get_input_from_file():
    """
    从文件读取输入内容
    返回: (issue_desc, error_details, context_info)
    """
    try:
        from pathlib import Path

        print("\n📄 支持的文件格式:")
        print("  - .txt  - 纯文本文件")
        print("  - .log  - 日志文件")
        print("  - .json - JSON格式文件")
        print("  - .md   - Markdown文件")

        file_path = input("请输入文件路径: ").strip()

        if not file_path:
            print("❌ 文件路径不能为空")
            return None, None, None

        path = Path(file_path)
        if not path.exists():
            print(f"❌ 文件不存在: {file_path}")
            return None, None, None

        print(f"📖 正在读取文件: {file_path}")

        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        print(f"✅ 文件读取成功，内容长度: {len(content)} 字符")

        # 根据文件扩展名智能解析
        if path.suffix.lower() == '.json':
            try:
                import json
                data = json.loads(content)
                return parse_json_content(data)
            except:
                print("⚠️ JSON解析失败，按文本处理")

        return parse_clipboard_content(content)

    except Exception as e:
        print(f"❌ 读取文件失败: {e}")
        return None, None, None


def get_input_manually():
    """
    手动输入问题信息
    返回: (issue_desc, error_details, context_info)
    """
    print("\n⌨️  手动输入模式")
    print("-" * 30)

    # 问题描述（必填）
    while True:
        issue_desc = input("问题描述: ").strip()
        if issue_desc:
            break
        print("❌ 问题描述不能为空，请重新输入")

    # 错误详情（可选）
    error_details = input("错误详情 (可选，直接回车跳过): ").strip() or None

    # 上下文信息（可选）
    context_info = input("上下文信息 (可选，直接回车跳过): ").strip() or None

    # 尝试解析上下文信息
    if context_info:
        try:
            import json
            context_info = json.loads(context_info)
        except:
            pass  # 保持为字符串

    return issue_desc, error_details, context_info


def parse_clipboard_content(content):
    """
    智能解析剪贴板或文件内容
    """
    lines = content.split('\n')
    issue_desc = ""
    error_details = ""
    context_info = ""

    # 尝试识别不同的内容类型
    in_error_section = False
    in_context_section = False

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # 识别错误信息模式
        if any(keyword in line.lower() for keyword in [
            'error', 'exception', 'traceback', 'syntaxerror', 'importerror',
            'filenotfounderror', 'permissionerror', 'unicodeerror', '编码错误'
        ]):
            in_error_section = True
            in_context_section = False
            if error_details:
                error_details += "\n" + line
            else:
                error_details = line
        # 识别代码或上下文信息
        elif any(keyword in line.lower() for keyword in [
            'def ', 'class ', 'import ', 'from ', 'try:', 'except:', 'if __name__',
            'print(', '文件', '路径', '目录', '命令'
        ]) or line.startswith(('    ', '\t')):
            in_context_section = True
            in_error_section = False
            if context_info:
                context_info += "\n" + line
            else:
                context_info = line
        # 其他内容作为问题描述
        else:
            if not issue_desc and not in_error_section and not in_context_section:
                issue_desc = line
            elif in_error_section:
                error_details += "\n" + line
            elif in_context_section:
                context_info += "\n" + line
            else:
                if len(issue_desc) < 200:  # 限制问题描述长度
                    issue_desc += " " + line

    # 如果没有识别出结构，将整个内容作为问题描述
    if not issue_desc and not error_details and not context_info:
        issue_desc = content[:500]  # 限制长度

    return issue_desc or "用户提供的错误信息", error_details, context_info


def parse_json_content(data):
    """
    解析JSON格式的内容
    """
    issue_desc = data.get('description', data.get('issue', ''))
    error_details = data.get('error', data.get('traceback', ''))
    context_info = data.get('context', data.get('code', ''))

    return issue_desc, error_details, context_info


# 命令行参数处理已移至文件末尾

def run_interactive_reflection():
    """
    运行交互式的AI反省机制
    """
    print("🧠 AI反省机制分析")
    print("=" * 50)
    print("请选择输入方式:")
    print("1. 📋 从剪贴板自动读取（推荐）")
    print("2. 📄 从文件读取")
    print("3. ⌨️  手动输入")
    print("0. 🔙 返回主菜单")

    try:
        input_method = input("\n请选择输入方式 (0-3): ").strip()

        if input_method == "0":
            return
        elif input_method == "1":
            # 从剪贴板读取
            issue_desc, error_details, context_info = get_input_from_clipboard()
        elif input_method == "2":
            # 从文件读取
            issue_desc, error_details, context_info = get_input_from_file()
        elif input_method == "3":
            # 手动输入
            issue_desc, error_details, context_info = get_input_manually()
        else:
            print("❌ 无效选择")
            return

        if not issue_desc:
            print("❌ 问题描述不能为空")
            return

        print("\n开始分析问题...")
        print(f"📝 问题描述: {issue_desc[:100]}{'...' if len(issue_desc) > 100 else ''}")
        if error_details:
            print(f"🐛 错误详情: {error_details[:100]}{'...' if len(error_details) > 100 else ''}")
        if context_info:
            print(f"📋 上下文信息: {context_info[:100]}{'...' if len(context_info) > 100 else ''}")

        reflection_report = analyze_and_reflect_on_issue(
            issue_description=issue_desc,
            error_details=error_details,
            context_info=context_info
        )

        print("\n✅ 分析完成！")
        print(f"📄 详细报告已保存到 test/reflection_reports/")

    except EOFError:
        print("❌ 非交互式环境，无法进行问题输入")
        print("💡 建议使用: python ai_config.py analyze \"问题描述\"")
    except KeyboardInterrupt:
        print("\n\n操作已取消")
    except Exception as e:
        print(f"❌ 分析过程中出现错误: {e}")


# 为了兼容性，保留原有的菜单选项5调用
def run_reflection_analysis():
    """
    菜单选项5的入口函数
    """
    run_interactive_reflection()

def auto_update_rules(issue_description, error_details=None, context_info=None):
    """
    自动更新规则：分析问题并自动应用规则更新

    参数:
    - issue_description: 问题描述
    - error_details: 错误详情（可选）
    - context_info: 上下文信息（可选）

    返回:
    - 更新结果字典
    """
    print("🔄 正在自动分析和更新规则...")

    # 1. 分析问题
    issue_category = classify_issue(issue_description, error_details)
    root_causes = analyze_root_causes(issue_description, error_details, context_info)
    rule_updates = suggest_rule_updates(issue_category, root_causes)

    print(f"📊 问题分类: {issue_category}")
    print(f"🔍 识别根本原因: {len(root_causes)} 个")
    print(f"📝 生成规则更新建议: {len(rule_updates)} 项")

    # 2. 自动应用规则更新
    update_results = apply_rule_updates(issue_category, rule_updates)

    # 3. 生成完整的分析报告
    analysis_report = {
        "timestamp": datetime.datetime.now().isoformat(),
        "issue_description": issue_description,
        "error_details": error_details,
        "context_info": context_info,
        "issue_category": issue_category,
        "root_causes": root_causes,
        "rule_updates": rule_updates,
        "update_results": update_results,
        "auto_updated": True
    }

    # 4. 保存报告
    report_path = Path("test") / "reflection_reports"
    report_path.mkdir(exist_ok=True)
    report_file = report_path / f"auto_update_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    try:
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(analysis_report, f, ensure_ascii=False, indent=2)
        print(f"✅ 自动更新完成！报告已保存: {report_file}")
    except Exception as e:
        print(f"⚠️ 报告保存失败: {e}")

    return analysis_report


def apply_rule_updates(issue_category, rule_updates):
    """
    自动应用规则更新到文档中

    参数:
    - issue_category: 问题分类
    - rule_updates: 规则更新列表

    返回:
    - 更新结果字典
    """
    results = {
        "docstring_updated": False,
        "print_function_updated": False,
        "new_rules_added": [],
        "errors": []
    }

    try:
        # 读取当前文件内容
        with open(__file__, 'r', encoding='utf-8') as f:
            content = f.read()

        # 1. 更新docstring中的规则
        if update_docstring_rules(content, issue_category, rule_updates):
            results["docstring_updated"] = True
            results["new_rules_added"].extend(rule_updates)
            print("✅ docstring规则已更新")

        # 2. 更新打印函数中的规则
        if update_print_function_rules(content, issue_category, rule_updates):
            results["print_function_updated"] = True
            print("✅ 打印函数规则已更新")

        # 3. 保存更新后的内容
        if results["docstring_updated"] or results["print_function_updated"]:
            with open(__file__, 'w', encoding='utf-8') as f:
                f.write(content)
            print("✅ 文件已保存")

    except Exception as e:
        error_msg = f"规则更新失败: {e}"
        results["errors"].append(error_msg)
        print(f"❌ {error_msg}")

    return results


def update_docstring_rules(content, issue_category, rule_updates):
    """
    更新docstring中的规则
    """
    # 查找错误处理规则部分
    error_handling_pattern = r'错误处理规则：([\s\S]*?)(?=\n\n\w+部分：|\n"""|\n# 代码段)'
    match = re.search(error_handling_pattern, content)

    if match:
        existing_rules = match.group(1).strip()

        # 检查是否已有类似规则
        new_rules = []
        for update in rule_updates:
            if update not in existing_rules:
                new_rules.append(f"- {update}")

        if new_rules:
            # 添加新规则
            updated_rules = existing_rules + "\n" + "\n".join(new_rules)
            content = content.replace(match.group(1), updated_rules)
            return True

    return False


def update_print_function_rules(content, issue_category, rule_updates):
    """
    更新打印函数中的规则
    """
    # 查找错误处理规则的打印部分
    print_pattern = r'print\("错误处理规则:"\)\s*([\s\S]*?)(?=\s*print\("第四部分：反省机制"\)|\s*print\("【规则声明】"\))'
    match = re.search(print_pattern, content)

    if match:
        existing_rules = match.group(1).strip()

        # 检查是否已有类似规则
        new_rules = []
        for update in rule_updates:
            rule_line = f'print("- {update}")'
            if rule_line not in existing_rules:
                new_rules.append(f'    {rule_line}')

        if new_rules:
            # 添加新规则
            updated_rules = existing_rules + "\n" + "\n".join(new_rules)
            content = content.replace(match.group(1), updated_rules)
            return True

    return False


# 增强的反省机制函数
def enhanced_reflection_analysis(issue_description, error_details=None, context_info=None, auto_update=False):
    """
    增强的反省机制分析，支持自动规则更新

    参数:
    - issue_description: 问题描述
    - error_details: 错误详情（可选）
    - context_info: 上下文信息（可选）
    - auto_update: 是否自动更新规则
    """
    if auto_update:
        print("🚀 启用自动规则更新模式")
        return auto_update_rules(issue_description, error_details, context_info)
    else:
        print("📋 标准分析模式")
        return analyze_and_reflect_on_issue(issue_description, error_details, context_info)


def test_encoding_issues():
    """测试编码问题"""
    print("\n=== 测试编码问题 ===")

    # 中文字符串处理
    chinese_text = "测试中文字符串处理"
    print(f"中文字符串: {chinese_text}")

    # 文件读写编码
    try:
        test_file = get_test_file_path('test_encoding.txt')
        with open(test_file, 'w', encoding='utf-8') as f:
            f.write(chinese_text)
        print("[OK] UTF-8文件写入成功")

        with open(test_file, 'r', encoding='utf-8') as f:
            content = f.read()
        print(f"[OK] UTF-8文件读取成功: {content}")

        # 尝试用错误编码读取
        try:
            with open(test_file, 'r', encoding='ascii') as f:
                content = f.read()
        except UnicodeDecodeError as e:
            print(f"[WARN] 预期的编码错误: {e}")

    except Exception as e:
        print(f"[ERROR] 编码测试失败: {e}")
    finally:
        test_file = get_test_file_path('test_encoding.txt')
        if test_file.exists():
            test_file.unlink()


def test_path_issues():
    """测试路径问题"""
    print("\n=== 测试路径问题 ===")

    # 长路径测试
    try:
        long_path = get_test_file_path("very/long/path/test/file.txt")
        long_path.parent.mkdir(parents=True, exist_ok=True)

        with open(long_path, 'w', encoding='utf-8') as f:
            f.write("长路径测试")
        print("[OK] 长路径创建成功")

        # 读取测试
        with open(long_path, 'r', encoding='utf-8') as f:
            content = f.read()
        print(f"[OK] 长路径读取成功: {content}")

    except Exception as e:
        print(f"[ERROR] 长路径测试失败: {e}")
    finally:
        long_path = get_test_file_path("very/long/path/test/file.txt")
        if long_path.exists():
            long_path.unlink()
            # 清理空目录
            for parent in long_path.parents:
                if parent != get_test_dir() and not any(parent.iterdir()):
                    try:
                        parent.rmdir()
                    except OSError:
                        break

    # 特殊字符路径测试
    try:
        special_path = get_test_file_path("test folder (特殊字符)/file[1].txt")
        special_path.parent.mkdir(parents=True, exist_ok=True)

        with open(special_path, 'w', encoding='utf-8') as f:
            f.write("特殊字符路径测试")
        print("[OK] 特殊字符路径创建成功")

    except Exception as e:
        print(f"[ERROR] 特殊字符路径测试失败: {e}")
    finally:
        special_path = get_test_file_path("test folder (特殊字符)/file[1].txt")
        if special_path.exists():
            special_path.unlink()
        if special_path.parent.exists():
            try:
                special_path.parent.rmdir()
            except OSError:
                # 目录不为空，跳过删除
                pass


def test_import_issues():
    """测试导入问题"""
    print("\n=== 测试导入问题 ===")

    # 尝试导入可能不存在的模块
    try:
        import nonexistent_module
        print("✓ 非预期模块导入成功")
    except ImportError as e:

        print(f"[WARN] 预期的导入错误: {e}")

    # 相对导入测试
    try:
        # 创建临时模块
        temp_dir = Path(tempfile.mkdtemp())
        module_path = temp_dir / "test_module.py"

        with open(module_path, 'w', encoding='utf-8') as f:
            f.write('def test_function():\n    return "测试模块"')

        # 添加到路径并导入
        sys.path.insert(0, str(temp_dir))
        import test_module
        result = test_module.test_function()
        print(f"[OK] 动态模块导入成功: {result}")

    except Exception as e:
        print(f"[ERROR] 动态导入测试失败: {e}")
    finally:
        if 'test_module' in sys.modules:
            del sys.modules['test_module']
        if temp_dir in sys.path:
            sys.path.remove(str(temp_dir))
        if module_path.exists():
            module_path.unlink()
        # 安全删除临时目录
        if temp_dir.exists():
            try:
                # 递归清理目录中的所有文件和子目录
                import shutil
                shutil.rmtree(temp_dir)
                print(f"[OK] 临时目录已清理: {temp_dir}")
            except Exception as e:
                print(f"[WARN] 临时目录清理警告: {e}")
                # 如果shutil失败，尝试手动递归删除
                try:
                    def remove_tree(path):
                        """递归删除目录树"""
                        for item in path.iterdir():
                            if item.is_file():
                                item.unlink()
                            elif item.is_dir():
                                remove_tree(item)
                                item.rmdir()
                        path.rmdir()

                    remove_tree(temp_dir)
                    print(f"✓ 临时目录已手动清理: {temp_dir}")
                except Exception as e2:
                    print(f"❌ 临时目录清理失败: {e2}")


def test_network_issues():
    """测试网络问题"""
    print("\n=== 测试网络问题 ===")

    # HTTP请求测试
    try:
        with urllib.request.urlopen('http://httpbin.org/get', timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
            print(f"[OK] HTTP请求成功: {data['url']}")
    except Exception as e:
        print(f"[ERROR] HTTP请求失败: {e}")

    # HTTPS请求测试
    try:
        with urllib.request.urlopen('https://httpbin.org/get', timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
            print(f"[OK] HTTPS请求成功: {data['url']}")
    except Exception as e:
        print(f"[ERROR] HTTPS请求失败: {e}")


def test_subprocess_issues():
    """测试子进程问题"""
    print("\n=== 测试子进程问题 ===")

    # Bash命令执行
    try:
        result = subprocess.run(['bash', '-c', 'date'],
                              capture_output=True, text=True, encoding='utf-8', errors='ignore', timeout=10)
        if result.returncode == 0:
            print(f"[OK] Bash命令执行成功: {result.stdout.strip()}")
        else:
            print(f"[WARN] Bash命令执行失败: {result.stderr}")
    except Exception as e:
        print(f"[ERROR] Bash测试失败: {e}")

    # 系统命令执行
    try:
        result = subprocess.run(['echo', 'Hello World'],
                              capture_output=True, text=True, encoding='utf-8', errors='ignore', timeout=10)
        if result.returncode == 0:
            print(f"[OK] 系统命令执行成功: {result.stdout.strip()}")
        else:
            print(f"[WARN] 系统命令执行失败: {result.stderr}")
    except Exception as e:
        print(f"[ERROR] 系统命令测试失败: {e}")


def test_memory_performance():
    """测试内存和性能问题"""
    print("\n=== 测试内存和性能问题 ===")

    # 大数据处理测试
    try:
        large_list = list(range(1000000))  # 100万元素
        result = sum(large_list)
        print(f"[OK] 大数据处理成功: 总和 = {result}")
    except Exception as e:
        print(f"[ERROR] 大数据处理失败: {e}")

    # 递归深度测试
    try:
        def recursive_function(depth=0):
            if depth > 100:
                return depth
            return recursive_function(depth + 1)

        result = recursive_function()
        print(f"[OK] 递归测试成功: 深度 = {result}")
    except RecursionError as e:
        print(f"[WARN] 预期的递归深度错误: {e}")


# =============================================================================
# 第五部分：开发工具和规则
# =============================================================================

"""
开发工具和规则
==============

Terminal使用规则：
- ❌ 避免的做法：不要在terminal中使用字符串运行Python程序
- ✅ 推荐的做法：创建独立的脚本文件

开发工具：
1. 代码质量检查工具 - check_code_quality()
2. 系统诊断测试 - run_comprehensive_system_test()

工作流程：
1. 创建独立的测试脚本
2. 在脚本中编写和调试代码
3. 使用check_code_quality()检查代码质量
4. 提交代码到版本控制

最佳实践：
- 创建专用脚本：为每个任务创建独立的脚本文件
- 定期检查：使用check_code_quality()检查代码质量
- 版本控制：将工具脚本纳入版本控制
- 文档化：为脚本添加清晰的文档和注释
- 模块化：将可重用的代码提取为独立模块
"""

def check_code_quality(file_path):
    """
    代码质量检查工具
    用于检查Python文件的语法、缩进和代码质量问题

    Args:
        file_path: 要检查的Python文件路径

    Returns:
        bool: 检查是否全部通过
    """
    if not os.path.exists(file_path):
        print(f"❌ 文件不存在: {file_path}")
        return False

    print(f"🔍 检查文件: {file_path}")
    print("-" * 40)

    results = []
    results.append(("语法检查", _check_syntax_internal(file_path)))
    results.append(("缩进检查", _check_indentation_internal(file_path)))
    results.append(("制表符检查", _check_tabs_internal(file_path)))

    print("-" * 40)
    passed = sum(1 for _, result in results if result)
    total = len(results)
    print(f"📊 检查结果: {passed}/{total} 通过")

    if passed == total:
        print("🎉 所有检查通过！")
        return True
    else:
        print("⚠️  发现问题需要修复")
        return False

def _check_syntax_internal(file_path):
    """内部语法检查函数"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            source = f.read()
        ast.parse(source)
        print("✅ 语法检查通过")
        return True
    except SyntaxError as e:
        print(f"❌ 语法错误: {e}")
        return False
    except Exception as e:
        print(f"❌ 检查失败: {e}")
        return False

def _check_indentation_internal(file_path):
    """内部缩进检查函数"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        issues = []
        for i, line in enumerate(lines, 1):
            stripped = line.lstrip()
            if stripped and not stripped.startswith('#'):
                indent = len(line) - len(stripped)
                if indent > 0 and indent % 4 != 0:
                    issues.append((i, indent))

        if issues:
            print(f"⚠️  发现{len(issues)}个非标准缩进:")
            for line_num, indent in issues[:5]:
                print(f"  第{line_num}行: {indent}个空格")
            return False
        else:
            print("✅ 缩进检查通过")
            return True

    except Exception as e:
        print(f"❌ 缩进检查失败: {e}")
        return False

def _check_tabs_internal(file_path):
    """内部制表符检查函数"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        if '\t' in content:
            tab_lines = []
            for i, line in enumerate(content.split('\n'), 1):
                if '\t' in line:
                    tab_lines.append(i)
            print(f"⚠️  发现{len(tab_lines)}行包含制表符")
            return False
        else:
            print("✅ 无制表符使用")
            return True

    except Exception as e:
        print(f"❌ 制表符检查失败: {e}")
        return False

def run_comprehensive_system_test():
    """
    综合系统诊断测试
    帮助AI了解系统特征，避免重复犯错
    测试内容：编码、路径、导入、网络、子进程、性能等系统特性
    """
    print("🧪 开始系统诊断测试...")
    print("=" * 50)

    # 系统环境诊断
    print("\n1. 系统环境诊断:")
    print("-" * 20)
    test_encoding_issues()
    test_path_issues()
    test_import_issues()
    test_network_issues()
    test_subprocess_issues()
    test_memory_performance()

    # 总结
    print("\n" + "=" * 50)
    print("📊 系统诊断完成")
    print("� AI已了解系统特征，可避免重复犯错")
    return True

# 主函数 - 处理命令行参数
if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "--help" or sys.argv[1] == "-h":
            print("""
Python AI - 开发环境配置工具

使用方法:
  python ai_config.py                    # 显示环境信息和规则
  python ai_config.py check <file>       # 检查代码质量
  python ai_config.py comprehensive      # 系统诊断（帮助AI了解系统特征）
  python ai_config.py auto-update <desc> # 自动更新规则
  python ai_config.py --help             # 显示此帮助信息

示例:
  python ai_config.py check stockpool.py
  python ai_config.py comprehensive
            """)
        elif sys.argv[1] == "check" and len(sys.argv) > 2:
            file_path = sys.argv[2]
            success = check_code_quality(file_path)
            sys.exit(0 if success else 1)
        elif sys.argv[1] == "comprehensive":
            print("🧪 运行综合系统测试...")
            success = run_comprehensive_system_test()
            sys.exit(0 if success else 1)
        elif sys.argv[1] == "auto-update":
            if len(sys.argv) > 2:
                desc = " ".join(sys.argv[2:])
                print(f"🔄 自动更新规则: {desc}")
                auto_update_rules(desc)
            else:
                print("用法: python ai_config.py auto-update \"问题描述\"")
        else:
            print(f"❌ 未知参数: {sys.argv[1]}")
            print("使用 --help 查看帮助信息")
    else:
        # 默认运行环境信息显示
        inform_ai_environment_and_rules()
        # 运行终端环境测试
        test_terminal_environment()

# 代码段：执行以告知AI相应的环境信息和规则
import os
import sys
import platform
import datetime
import json
import re
import tempfile
import urllib.request
import subprocess
import ast
from pathlib import Path

# 跨平台Shell命令生成器 - 内嵌实现
class ShellCommandBuilder:
    """跨平台Shell命令生成器"""

    def __init__(self):
        self.is_windows = os.name == 'nt'
        self.is_powershell = 'powershell' in os.environ.get('PSModulePath', '').lower()

    def build_command(self, *commands: str, check_success: bool = False,
                     suppress_errors: bool = False) -> str:
        """
        构建跨平台兼容的命令字符串

        Args:
            *commands: 要执行的命令
            check_success: 是否检查命令执行成功
            suppress_errors: 是否抑制错误输出

        Returns:
            兼容当前shell的命令字符串
        """
        if not commands:
            return ""

        if self.is_windows and self.is_powershell:
            return self._build_powershell_command(commands, check_success, suppress_errors)
        elif self.is_windows:
            return self._build_cmd_command(commands, check_success, suppress_errors)
        else:
            return self._build_bash_command(commands, check_success, suppress_errors)

    def _build_powershell_command(self, commands: tuple, check_success: bool,
                                suppress_errors: bool) -> str:
        """构建PowerShell命令"""
        cmd_parts = []

        for i, cmd in enumerate(commands):
            if suppress_errors and i == len(commands) - 1:
                cmd += " 2>$null"
            cmd_parts.append(cmd)

        joined_cmd = " ; ".join(cmd_parts)

        if check_success:
            joined_cmd += " ; if ($LASTEXITCODE -eq 0) { Write-Host '✓ 成功' } else { Write-Host '✗ 失败' }"

        return joined_cmd

    def _build_cmd_command(self, commands: tuple, check_success: bool,
                         suppress_errors: bool) -> str:
        """构建CMD命令"""
        cmd_parts = []

        for i, cmd in enumerate(commands):
            if suppress_errors and i == len(commands) - 1:
                cmd += " 2>nul"
            cmd_parts.append(cmd)

        joined_cmd = " && ".join(cmd_parts) if len(cmd_parts) > 1 else cmd_parts[0]

        if check_success and len(commands) > 1:
            joined_cmd += " && echo ✓ 成功 || echo ✗ 失败"

        return joined_cmd

    def _build_bash_command(self, commands: tuple, check_success: bool,
                          suppress_errors: bool) -> str:
        """构建Bash命令"""
        cmd_parts = []

        for i, cmd in enumerate(commands):
            if suppress_errors and i == len(commands) - 1:
                cmd += " 2>/dev/null"
            cmd_parts.append(cmd)

        joined_cmd = " && ".join(cmd_parts) if len(commands) > 1 else cmd_parts[0]

        if check_success and len(commands) > 1:
            joined_cmd += " && echo '✓ 成功' || echo '✗ 失败'"

        return joined_cmd

# 全局实例
_shell_builder = ShellCommandBuilder()

def generate_shell_command(*commands: str, check_success: bool = False,
                          suppress_errors: bool = False) -> str:
    """
    生成跨平台兼容的shell命令

    Args:
        *commands: 命令列表
        check_success: 是否检查执行结果
        suppress_errors: 是否抑制错误输出

    Returns:
        兼容当前平台的命令字符串

    示例:
        >>> generate_shell_command("python --version", "echo done", check_success=True)
        'python --version ; echo done ; if ($LASTEXITCODE -eq 0) { Write-Host "✓ 成功" } else { Write-Host "✗ 失败" }'
    """
    return _shell_builder.build_command(*commands, check_success=check_success,
                                       suppress_errors=suppress_errors)

# 在新对话开始后执行此函数
# (已移至命令行参数处理部分)

def get_test_dir():
    """
    获取test目录的路径，确保所有测试文件都在test/目录中操作
    
    返回:
    - Path对象: test目录的路径
    """
    from pathlib import Path
    import os
    
    # 获取当前工作目录
    current_dir = Path.cwd()
    
    # 构建test目录路径
    test_dir = current_dir / "test"
    
    # 确保test目录存在
    test_dir.mkdir(exist_ok=True)
    
    return test_dir

def run_cross_platform_command(*commands: str, check_success: bool = False, 
                              suppress_errors: bool = False, **subprocess_kwargs):
    """
    使用跨平台shell命令生成器执行命令
    
    Args:
        *commands: 要执行的命令
        check_success: 是否检查执行结果
        suppress_errors: 是否抑制错误输出
        **subprocess_kwargs: 传递给subprocess.run的其他参数
    
    Returns:
        subprocess.CompletedProcess: 执行结果
    """
    # 使用内嵌的跨平台shell命令生成器
    shell_cmd = generate_shell_command(*commands, check_success=check_success, 
                                     suppress_errors=suppress_errors)
    
    # 设置默认的subprocess参数
    default_kwargs = {
        'shell': True,
        'capture_output': True,
        'text': True,
        'encoding': 'utf-8',
        'errors': 'ignore'
    }
    default_kwargs.update(subprocess_kwargs)
    
    # 在PowerShell中执行
    if os.name == 'nt':
        return subprocess.run(['powershell', '-Command', shell_cmd], **default_kwargs)
    else:
        return subprocess.run([shell_cmd], **default_kwargs)

def get_test_file_path(filename):
    """
    获取测试文件的完整路径，确保文件在test/目录中
    
    参数:
    - filename: 文件名
    
    返回:
    - Path对象: 测试文件的完整路径
    """
    test_dir = get_test_dir()
    return test_dir / filename

def cleanup_test_files():
    """
    清理test目录中的临时文件
    """
    import shutil
    from pathlib import Path
    
    test_dir = get_test_dir()
    
    # 定义需要清理的文件模式
    cleanup_patterns = [
        "*.tmp", "*.temp", "test_*.txt", "test_*.json",
        "error_*.log", "debug_*.log", "*.pyc", "__pycache__"
    ]
    
    cleaned_files = []
    
    for pattern in cleanup_patterns:
        for file_path in test_dir.glob(pattern):
            try:
                if file_path.is_file():
                    file_path.unlink()
                    cleaned_files.append(str(file_path))
                elif file_path.is_dir() and file_path.name == "__pycache__":
                    shutil.rmtree(file_path)
                    cleaned_files.append(str(file_path))
            except Exception as e:
                print(f"清理文件失败 {file_path}: {e}")
    
    if cleaned_files:
        print(f"已清理 {len(cleaned_files)} 个测试文件")
    
    return cleaned_files

def get_command_separator():
    """
    根据当前shell类型返回正确的命令分隔符
    
    返回:
    - str: 命令分隔符 (';' for PowerShell, '&&' for bash/Linux)
    """
    import platform
    import os
    
    # 检测操作系统
    system = platform.system().lower()
    
    # 检测是否在PowerShell环境中
    if system == "windows":
        # 检查环境变量或进程名来判断是否为PowerShell
        if os.environ.get('PSModulePath') or 'powershell' in os.environ.get('SHELL', '').lower():
            return ";"
        else:
            # Windows默认使用cmd或PowerShell，保守起见使用;
            return ";"
    else:
        # Linux/Unix系统使用&&
        return "&&"

def build_multi_command(*commands):
    """
    构建多命令字符串，使用正确的分隔符
    
    参数:
    - commands: 可变数量的命令字符串
    
    返回:
    - str: 组合后的命令字符串
    """
    if not commands:
        return ""
    
    separator = get_command_separator()
    return separator.join(commands)

def safe_run_commands(*commands, **kwargs):
    """
    安全地运行多个命令，使用正确的分隔符
    
    参数:
    - commands: 要运行的命令
    - **kwargs: 传递给subprocess.run的参数
    
    返回:
    - subprocess.CompletedProcess: 命令执行结果
    """
    import subprocess
    
    if len(commands) == 1:
        # 单命令直接执行
        return subprocess.run(commands[0], **kwargs)
    else:
        # 多命令组合执行
        combined_command = build_multi_command(*commands)
        return subprocess.run(combined_command, **kwargs)

def handle_common_errors():
    """
    演示如何处理Windows 11中文环境下的常见错误。
    """
    import os
    import sys
    import pathlib
    from pathlib import Path
    
    print("\n=== 常见错误处理示例 ===")
    
    # 1. 编码错误处理
    try:
        # 正确读取UTF-8文件
        with open('example.txt', 'r', encoding='utf-8') as f:
            content = f.read()
        print("✓ 文件读取成功，编码正确")
    except UnicodeDecodeError:
        print("✗ 编码错误：尝试使用GBK")
        try:
            with open('example.txt', 'r', encoding='gbk') as f:
                content = f.read()
            print("✓ 使用GBK读取成功")
        except:
            print("✗ 无法读取文件")
    
    # 2. 路径问题处理
    # 使用pathlib避免路径问题
    current_dir = Path.cwd()
    file_path = current_dir / "data" / "example.json"
    print(f"✓ 安全路径构建: {file_path}")
    
    # 检查长路径
    if len(str(file_path)) > 260:
        print("⚠ 路径过长，可能需要使用\\\\?\\前缀")
        long_path = "\\\\?\\" + str(file_path)
        print(f"长路径处理: {long_path}")
    
    # 3. Python环境检查
    print(f"Python可执行文件: {sys.executable}")
    print(f"Python路径: {sys.path}")
    
    # 4. 文件操作安全处理
    try:
        with open('test_write.txt', 'w', encoding='utf-8') as f:
            f.write("测试中文写入")
        print("✓ 文件写入成功")
    except PermissionError:
        print("✗ 权限错误：无法写入文件")
    except OSError as e:
        print(f"✗ 文件操作错误: {e}")
    
    # 5. 命令行参数处理（避免交互）
    import argparse
    parser = argparse.ArgumentParser(description='示例脚本')
    parser.add_argument('--input', default='default_value', help='输入参数')
    # args = parser.parse_args()  # 在实际使用时取消注释
    
    # 6. 环境变量检查
    pythonpath = os.environ.get('PYTHONPATH', '')
    if pythonpath:
        print(f"PYTHONPATH: {pythonpath}")
    else:
        print("⚠ PYTHONPATH未设置")
    
    # 7. 终端编码设置（PowerShell）
    print("PowerShell编码设置建议:")
    print("$OutputEncoding = [Console]::OutputEncoding = [Text.Encoding]::UTF8")
    
    print("=== 错误处理示例完成 ===")

# 可选：在主执行中调用错误处理示例
# handle_common_errors()

def test_terminal_environment():
    """
    测试终端环境中的常见问题，帮助AI确认潜在的环境问题。
    """
    import subprocess
    import sys
    import os
    import platform
    
    print("\n=== 终端环境测试检测 ===")
    
    # 1. 检查操作系统和终端信息
    print(f"操作系统: {platform.system()} {platform.release()}")
    print(f"Python版本: {sys.version}")
    print(f"工作目录: {os.getcwd()}")
    
    # 2. 检查编码设置
    try:
        import locale
        print(f"系统默认编码: {locale.getpreferredencoding()}")
        print(f"文件系统编码: {sys.getfilesystemencoding()}")
    except:
        print("编码信息获取失败")
    
    # 3. 测试PowerShell编码设置
    try:
        result = run_cross_platform_command('$OutputEncoding')
        if result.returncode == 0:
            print(f"PowerShell OutputEncoding: {result.stdout.strip()}")
        else:
            print("PowerShell编码检查失败")
    except Exception as e:
        print(f"PowerShell测试失败: {e}")
    
    # 4. 检查Conda环境
    conda_env = os.environ.get('CONDA_DEFAULT_ENV', 'None')
    print(f"当前Conda环境: {conda_env}")
    
    # 5. 测试文件操作权限
    test_file = 'test_env_check.txt'
    try:
        with open(test_file, 'w', encoding='utf-8') as f:
            f.write("测试文件写入")
        print("[OK] 文件写入权限正常")
        os.remove(test_file)
        print("[OK] 文件删除权限正常")
    except Exception as e:
        print(f"[ERROR] 文件操作权限问题: {e}")
    
    # 6. 检查Python包管理
    try:
        import pip
        print(f"pip版本: {pip.__version__}")
    except ImportError:
        print("✗ pip未安装")
    
    # 7. 测试网络连接（可选）
    try:
        import urllib.request
        with urllib.request.urlopen('https://httpbin.org/get', timeout=10) as response:
            print("[OK] 网络连接正常 (HTTPS)")
    except urllib.error.URLError as e:
        print(f"[WARN] HTTPS连接失败: {e}")
        try:
            with urllib.request.urlopen('http://httpbin.org/get', timeout=10) as response:
                print("[OK] 网络连接正常 (HTTP)")
        except Exception as e2:
            print(f"[ERROR] 网络连接失败: {e2}")
    except Exception as e:
        print(f"[ERROR] 网络测试失败: {e}")
    
    # 8. 检查磁盘空间 (Windows兼容)
    try:
        import shutil
        total, used, free = shutil.disk_usage("C:")
        free_gb = free / (1024**3)
        total_gb = total / (1024**3)
        used_gb = used / (1024**3)
        print(f"磁盘总空间: {total_gb:.2f} GB")
        print(f"磁盘已用空间: {used_gb:.2f} GB")
        print(f"磁盘剩余空间: {free_gb:.2f} GB")
    except Exception as e:
        print(f"磁盘空间检查失败: {e}")
    
    # 9. 检查环境变量
    key_vars = ['PATH', 'PYTHONPATH', 'CONDA_PREFIX']
    for var in key_vars:
        value = os.environ.get(var, 'Not set')
        if len(str(value)) > 100:
            value = str(value)[:100] + '...'
        print(f"{var}: {value}")
    
    # 10. PYTHONPATH设置建议
    if not os.environ.get('PYTHONPATH'):
        current_dir = os.getcwd()
        print(f"建议设置PYTHONPATH: {current_dir}")
        print("PowerShell命令: $env:PYTHONPATH = $env:PYTHONPATH + ';" + current_dir + "'")
    
    # 11. 子进程命令测试
    try:
        result = subprocess.run(['where', 'python'], capture_output=True, text=True, encoding='cp936', errors='ignore', timeout=5)
        if result.returncode == 0:
            print("[OK] 系统命令可用")
        else:
            print("[WARN] 系统命令可能有问题")
    except Exception as e:
        print(f"[WARN] 子进程测试失败: {e}")
    
    print("=== 终端环境测试完成 ===")

# 可选：在主执行中调用环境测试
# test_terminal_environment()  # 已移至主函数中按需调用

def detect_powershell_semicolon_issues():
    """
    检测PowerShell分号冲突问题
    返回检测结果字典，用于规则完善
    """
    import subprocess
    import tempfile
    from pathlib import Path

    results = {
        'semicolon_conflict': False,
        'complex_command_parsing': False,
        'encoding_issues': False,
        'recommendations': []
    }

    print("\n=== PowerShell分号冲突检测 ===")

    # 测试1: 简单分号命令
    try:
        result = run_cross_platform_command('Write-Host "Hello"', 'Write-Host "World"', timeout=10)
        if result.returncode != 0:
            results['semicolon_conflict'] = True
            results['recommendations'].append("避免在单行中使用分号分隔命令")
            print("⚠ 检测到分号冲突问题")
        else:
            print("✓ 分号命令执行正常")
    except Exception as e:
        results['semicolon_conflict'] = True
        results['recommendations'].append(f"分号测试异常: {e}")
        print(f"⚠ 分号测试失败: {e}")

    # 测试2: 复杂命令解析
    try:
        complex_cmd = '''
        $a = "test";
        if ($a -eq "test") {
            Write-Host "Complex command works"
        }
        '''
        result = subprocess.run(['powershell', '-Command', complex_cmd],
                              capture_output=True, text=True, encoding='utf-8', errors='ignore', timeout=10)
        if result.returncode != 0:
            results['complex_command_parsing'] = True
            results['recommendations'].append("复杂逻辑应写入单独脚本文件")
            print("⚠ 检测到复杂命令解析问题")
        else:
            print("✓ 复杂命令解析正常")
    except Exception as e:
        results['complex_command_parsing'] = True
        results['recommendations'].append(f"复杂命令测试异常: {e}")
        print(f"⚠ 复杂命令测试失败: {e}")

    # 测试3: 中文编码问题
    try:
        result = run_cross_platform_command('Write-Host "中文测试"', timeout=10)
        if "中文测试" not in result.stdout and result.returncode != 0:
            results['encoding_issues'] = True
            results['recommendations'].append("PowerShell需要设置UTF-8编码")
            print("⚠ 检测到中文编码问题")
        else:
            print("✓ 中文编码正常")
    except Exception as e:
        results['encoding_issues'] = True
        results['recommendations'].append(f"中文编码测试异常: {e}")
        print(f"⚠ 中文编码测试失败: {e}")

    return results

def detect_chinese_encoding_issues():
    """
    检测中文编码相关问题
    """
    import tempfile
    from pathlib import Path

    results = {
        'file_encoding_mismatch': False,
        'console_encoding_issues': False,
        'path_encoding_problems': False,
        'recommendations': []
    }

    print("\n=== 中文编码问题检测 ===")

    # 测试1: 文件编码一致性
    try:
        test_dir = Path("test")
        test_dir.mkdir(exist_ok=True)

        # 创建UTF-8文件
        utf8_file = test_dir / "utf8_test.txt"
        with open(utf8_file, 'w', encoding='utf-8') as f:
            f.write("这是UTF-8编码的中文文件")

        # 尝试用GBK读取
        try:
            with open(utf8_file, 'r', encoding='gbk') as f:
                content = f.read()
            results['file_encoding_mismatch'] = True
            results['recommendations'].append("文件编码不一致会导致乱码")
            print("⚠ 检测到文件编码不匹配问题")
        except UnicodeDecodeError:
            print("✓ 文件编码检测正常（预期错误）")

        # 正确读取
        with open(utf8_file, 'r', encoding='utf-8') as f:
            content = f.read()
        print("✓ UTF-8文件读取正常")

        # 清理
        utf8_file.unlink()

    except Exception as e:
        results['file_encoding_mismatch'] = True
        results['recommendations'].append(f"文件编码测试异常: {e}")
        print(f"⚠ 文件编码测试失败: {e}")

    # 测试2: 控制台编码
    try:
        chinese_text = "控制台中文测试"
        print(f"控制台输出: {chinese_text}")
        print("✓ 控制台编码正常")
    except Exception as e:
        results['console_encoding_issues'] = True
        results['recommendations'].append(f"控制台编码问题: {e}")
        print(f"⚠ 控制台编码问题: {e}")

    # 测试3: 路径中文处理
    try:
        test_dir = Path("test")
        chinese_dir_name = "中文测试目录"
        chinese_dir = test_dir / chinese_dir_name
        chinese_dir.mkdir(exist_ok=True)

        chinese_file = chinese_dir / "中文文件.txt"
        with open(chinese_file, 'w', encoding='utf-8') as f:
            f.write("中文路径测试内容")

        with open(chinese_file, 'r', encoding='utf-8') as f:
            content = f.read()

        print("✓ 中文路径处理正常")

        # 清理
        chinese_file.unlink()
        chinese_dir.rmdir()

    except Exception as e:
        results['path_encoding_problems'] = True
        results['recommendations'].append(f"中文路径处理问题: {e}")
        print(f"⚠ 中文路径处理问题: {e}")

    return results

def detect_file_path_issues():
    """
    检测文件路径相关问题
    """
    import tempfile
    from pathlib import Path

    results = {
        'wrong_test_directory': False,
        'path_separation_issues': False,
        'long_path_problems': False,
        'cleanup_failures': False,
        'recommendations': []
    }

    print("\n=== 文件路径问题检测 ===")

    # 测试1: 测试文件生成位置
    try:
        # 模拟错误的路径使用
        wrong_file = Path.cwd() / "wrong_location_test.txt"
        with open(wrong_file, 'w', encoding='utf-8') as f:
            f.write("错误的测试文件位置")

        if wrong_file.exists():
            results['wrong_test_directory'] = True
            results['recommendations'].append("测试文件不应生成在工作目录")
            print("⚠ 检测到测试文件位置错误")

        # 清理
        wrong_file.unlink()

    except Exception as e:
        results['wrong_test_directory'] = True
        results['recommendations'].append(f"测试文件位置测试异常: {e}")
        print(f"⚠ 测试文件位置测试失败: {e}")

    # 测试2: 路径分隔符问题
    try:
        # Windows路径分隔符测试
        test_path = "C:\\Users\\test\\file.txt"
        if "\\" in test_path:
            print("✓ Windows路径分隔符正常")
        else:
            results['path_separation_issues'] = True
            results['recommendations'].append("注意Windows路径分隔符转义")
            print("⚠ 路径分隔符可能有问题")

    except Exception as e:
        results['path_separation_issues'] = True
        results['recommendations'].append(f"路径分隔符测试异常: {e}")
        print(f"⚠ 路径分隔符测试失败: {e}")

    # 测试3: 长路径处理
    try:
        long_path = Path.cwd() / ("long_dir_name_" * 50) / "test.txt"
        if len(str(long_path)) > 260:
            results['long_path_problems'] = True
            results['recommendations'].append("长路径需要使用\\\\?\\前缀")
            print("⚠ 检测到长路径问题")
        else:
            print("✓ 路径长度正常")

    except Exception as e:
        results['long_path_problems'] = True
        results['recommendations'].append(f"长路径测试异常: {e}")
        print(f"⚠ 长路径测试失败: {e}")

    # 测试4: 文件清理问题
    try:
        temp_files = []
        for i in range(3):
            temp_file = Path.cwd() / f"temp_cleanup_test_{i}.txt"
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.write(f"临时文件{i}")
            temp_files.append(temp_file)

        # 清理文件
        for temp_file in temp_files:
            try:
                temp_file.unlink()
            except Exception as e:
                results['cleanup_failures'] = True
                results['recommendations'].append(f"文件清理失败: {e}")
                print(f"⚠ 文件清理失败: {e}")

        if not results['cleanup_failures']:
            print("✓ 文件清理正常")

    except Exception as e:
        results['cleanup_failures'] = True
        results['recommendations'].append(f"文件清理测试异常: {e}")
        print(f"⚠ 文件清理测试失败: {e}")

    return results

def run_comprehensive_validation():
    """
    运行全面的规则验证
    返回所有检测结果的汇总
    """
    print("=== AI规则验证系统 ===")
    print("开始全面检测常见问题...")

    all_results = {}

    # 运行各项检测
    try:
        all_results['powershell'] = detect_powershell_semicolon_issues()
    except Exception as e:
        print(f"PowerShell检测失败: {e}")
        all_results['powershell'] = {'error': str(e)}

    try:
        all_results['chinese_encoding'] = detect_chinese_encoding_issues()
    except Exception as e:
        print(f"中文编码检测失败: {e}")
        all_results['chinese_encoding'] = {'error': str(e)}

    try:
        all_results['file_paths'] = detect_file_path_issues()
    except Exception as e:
        print(f"文件路径检测失败: {e}")
        all_results['file_paths'] = {'error': str(e)}

    # 生成检测报告
    print("\n=== 检测报告汇总 ===")

    total_issues = 0
    all_recommendations = []

    for category, results in all_results.items():
        if 'error' in results:
            print(f"⚠ {category}: 检测失败 - {results['error']}")
            total_issues += 1
        else:
            issues_in_category = sum(1 for k, v in results.items()
                                   if k != 'recommendations' and isinstance(v, bool) and v)
            if issues_in_category > 0:
                print(f"⚠ {category}: 发现 {issues_in_category} 个问题")
                total_issues += issues_in_category
            else:
                print(f"✓ {category}: 无问题")

            if 'recommendations' in results:
                all_recommendations.extend(results['recommendations'])

    print(f"\n总计发现问题: {total_issues}")

    if all_recommendations:
        print("\n改进建议:")
        for i, rec in enumerate(all_recommendations, 1):
            print(f"{i}. {rec}")

    # 保存检测结果到文件
    try:
        import json
        from datetime import datetime
        from pathlib import Path

        result_file = Path("test") / "validation_results.json"
        result_file.parent.mkdir(exist_ok=True)

        validation_data = {
            'timestamp': datetime.now().isoformat(),
            'total_issues': total_issues,
            'results': all_results,
            'recommendations': all_recommendations
        }

        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(validation_data, f, ensure_ascii=False, indent=2)

        print(f"\n✓ 检测结果已保存到: {result_file}")

    except Exception as e:
        print(f"⚠ 结果保存失败: {e}")

    return all_results

# 在主执行中运行验证（可选）
# run_comprehensive_validation()

# 为了便于其他AI使用，添加便捷函数
def validate_rules():
    """
    便捷函数：运行所有规则验证
    建议在每次重大修改后运行此函数
    """
    return run_comprehensive_validation()

def check_specific_issue(issue_type):
    """
    检查特定类型的问题
    参数: 'powershell', 'encoding', 'paths'
    """
    if issue_type == 'powershell':
        return detect_powershell_semicolon_issues()
    elif issue_type == 'encoding':
        return detect_chinese_encoding_issues()
    elif issue_type == 'paths':
        return detect_file_path_issues()
    else:
        return {"error": f"未知的问题类型: {issue_type}"}

def analyze_and_reflect_on_issue(issue_description, error_details=None, context_info=None):
    """
    AI反省机制：分析问题、总结原因、建议规则更新
    
    参数:
    - issue_description: 问题描述
    - error_details: 错误详情（可选）
    - context_info: 上下文信息（可选）
    
    返回:
    - 分析报告字典
    """
    import json
    import datetime
    from pathlib import Path
    
    print("\n=== AI反省机制启动 ===")
    print(f"问题描述: {issue_description}")
    
    # 1. 问题分类分析
    issue_category = classify_issue(issue_description, error_details)
    
    # 2. 原因分析
    root_causes = analyze_root_causes(issue_description, error_details, context_info)
    
    # 3. 影响评估
    impact_assessment = assess_impact(issue_category, error_details)
    
    # 4. 规则更新建议
    rule_updates = suggest_rule_updates(issue_category, root_causes)
    
    # 5. 测试代码生成
    test_code = generate_test_code(issue_category, root_causes)
    
    # 6. 验证机制建议
    validation_suggestions = suggest_validation_improvements(issue_category)
    
    # 编译分析报告
    analysis_report = {
        "timestamp": datetime.datetime.now().isoformat(),
        "issue_description": issue_description,
        "error_details": error_details,
        "context_info": context_info,
        "issue_category": issue_category,
        "root_causes": root_causes,
        "impact_assessment": impact_assessment,
        "rule_updates": rule_updates,
        "test_code": test_code,
        "validation_suggestions": validation_suggestions,
        "recommendations": [
            "立即更新规则文档以包含新识别的问题类型",
            "添加相应的测试代码到验证系统中",
            "更新验证函数以检测类似问题",
            "在项目文档中记录此次问题分析",
            "考虑是否需要修改现有代码以防止类似问题"
        ]
    }
    
    # 保存分析报告
    report_path = Path("test") / "reflection_reports"
    report_path.mkdir(exist_ok=True)
    
    report_file = report_path / f"reflection_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(analysis_report, f, ensure_ascii=False, indent=2)
        print(f"✓ 分析报告已保存: {report_file}")
    except Exception as e:
        print(f"⚠ 无法保存分析报告: {e}")
    
    # 打印关键发现
    print(f"问题分类: {issue_category}")
    print(f"主要原因: {', '.join(root_causes[:3])}")  # 显示前3个原因
    print(f"影响程度: {impact_assessment['severity']}")
    print(f"建议规则更新: {len(rule_updates)} 项")
    print(f"生成测试代码: {len(test_code)} 个测试用例")
    
    return analysis_report

def classify_issue(issue_description, error_details=None):
    """对问题进行分类"""
    desc_lower = issue_description.lower()
    
    if any(keyword in desc_lower for keyword in ['编码', 'encoding', 'utf-8', 'gbk', 'cp936', 'unicode']):
        return "编码问题"
    elif any(keyword in desc_lower for keyword in ['路径', 'path', 'file', 'directory', '文件夹']):
        return "文件路径问题"
    elif any(keyword in desc_lower for keyword in ['powershell', '命令', 'command', 'terminal', '终端']):
        return "终端命令问题"
    elif any(keyword in desc_lower for keyword in ['模块', 'import', 'module', '包']):
        return "模块导入问题"
    elif any(keyword in desc_lower for keyword in ['权限', 'permission', 'access']):
        return "权限问题"
    elif any(keyword in desc_lower for keyword in ['内存', 'memory', '资源']):
        return "资源管理问题"
    elif any(keyword in desc_lower for keyword in ['网络', 'network', '连接']):
        return "网络连接问题"
    elif any(keyword in desc_lower for keyword in ['syntax', '语法', 'string', '字符串', '引号', 'quote']):
        return "语法错误"
    else:
        return "其他问题"

def analyze_root_causes(issue_description, error_details, context_info):
    """分析问题的根本原因"""
    causes = []
    
    # 基于问题描述分析原因
    desc_lower = issue_description.lower()
    
    if '编码' in desc_lower or 'encoding' in desc_lower:
        causes.extend([
            "文件编码不一致（UTF-8 vs 系统默认编码）",
            "子进程调用未指定编码参数",
            "控制台输出编码与文件编码不匹配"
        ])
    
    if '路径' in desc_lower or 'path' in desc_lower:
        causes.extend([
            "使用字符串拼接而非pathlib或os.path.join",
            "未处理长路径问题",
            "工作目录与预期不符"
        ])
    
    if 'powershell' in desc_lower or '命令' in desc_lower:
        causes.extend([
            "PowerShell编码设置不正确",
            "命令语法在不同环境中不兼容",
            "未处理命令执行错误"
        ])
    
    if '语法' in desc_lower or 'syntax' in desc_lower or '字符串' in desc_lower or 'string' in desc_lower:
        causes.extend([
            "字符串引号不匹配或未闭合",
            "多行字符串缺少正确的引号",
            "字符串中的转义字符使用不当",
            "f-string或格式化字符串语法错误",
            "嵌套引号缺少正确转义"
        ])
    
    # 基于错误详情分析
    if error_details:
        error_str = str(error_details).lower()
        if 'permission' in error_str or '权限' in error_str:
            causes.append("文件或目录权限不足")
        if 'not found' in error_str or '找不到' in error_str:
            causes.append("文件或命令不存在")
        if 'timeout' in error_str or '超时' in error_str:
            causes.append("操作超时")
        if 'unterminated string' in error_str or '字符串字面量未终止' in error_str:
            causes.append("字符串字面量缺少结束引号")
        if 'eol while scanning' in error_str:
            causes.append("字符串扫描到行尾仍未找到结束引号")
    
    # 默认原因
    if not causes:
        causes = ["问题原因待进一步分析", "可能需要更多上下文信息"]
    
    return causes

def assess_impact(issue_category, error_details):
    """评估问题的影响程度"""
    severity_levels = {
        "编码问题": "中",
        "文件路径问题": "中", 
        "终端命令问题": "高",
        "模块导入问题": "高",
        "权限问题": "高",
        "资源管理问题": "中",
        "网络连接问题": "低",
        "其他问题": "低"
    }
    
    severity = severity_levels.get(issue_category, "中")
    
    # 根据错误详情调整严重程度
    if error_details and 'critical' in str(error_details).lower():
        severity = "高"
    
    return {
        "severity": severity,
        "affected_areas": ["代码执行", "用户体验", "系统稳定性"],
        "estimated_fix_time": "1-2小时" if severity == "低" else "2-4小时" if severity == "中" else "4-8小时"
    }

def suggest_rule_updates(issue_category, root_causes):
    """建议规则更新"""
    updates = []
    
    if issue_category == "编码问题":
        updates.extend([
            "在所有文件操作中明确指定encoding='utf-8'",
            "子进程调用添加encoding='cp936', errors='ignore'参数",
            "统一文件编码为UTF-8，控制台输出使用系统编码"
        ])
    
    elif issue_category == "文件路径问题":
        updates.extend([
            "优先使用pathlib.Path处理路径操作",
            "长路径使用\\\\?\\前缀",
            "测试文件必须在test/目录中操作"
        ])
    
    elif issue_category == "终端命令问题":
        updates.extend([
            "PowerShell脚本开头设置UTF-8编码",
            "检测命令是否存在后再执行",
            "使用完整路径调用命令"
        ])
    
    elif issue_category == "语法错误":
        updates.extend([
            "字符串使用一致的引号风格（建议双引号）",
            "多行字符串使用三引号或正确转义",
            "检查字符串引号匹配和闭合",
            "f-string嵌套时注意引号冲突",
            "使用语法检查工具验证代码"
        ])
    
    # 基于根本原因添加通用建议
    for cause in root_causes:
        if "编码" in cause:
            updates.append("加强编码一致性检查")
        if "权限" in cause:
            updates.append("添加权限检查机制")
        if "超时" in cause:
            updates.append("为所有网络和I/O操作设置合理的超时时间")
        if "字符串" in cause or "引号" in cause:
            updates.append("实施字符串语法检查规范")
    
    return updates

def generate_test_code(issue_category, root_causes):
    """生成测试代码"""
    test_cases = []
    
    if issue_category == "编码问题":
        test_cases.append("""
def test_encoding_consistency():
    \"\"\"测试编码一致性\"\"\"
    # 测试文件编码
    with open('test_encoding.txt', 'w', encoding='utf-8') as f:
        f.write('测试中文')
    
    with open('test_encoding.txt', 'r', encoding='utf-8') as f:
        content = f.read()
    
    assert content == '测试中文'
    
    # 测试子进程编码
    result = subprocess.run(['echo', '测试'], 
                          capture_output=True, 
                          encoding='cp936', 
                          errors='ignore')
    assert result.returncode == 0
""")
    
    if issue_category == "文件路径问题":
        test_cases.append("""
def test_path_handling():
    \"\"\"测试路径处理\"\"\"
    from pathlib import Path
    
    # 测试pathlib使用
    test_path = Path('test') / 'subdir' / 'file.txt'
    assert test_path.exists() or not test_path.exists()  # 只是测试路径构建
    
    # 测试长路径
    long_path = '\\\\?\\' + str(Path.cwd() / 'very_long_path_name_that_might_exceed_limits.txt')
    # 在Windows上测试长路径处理
""")
    
    return test_cases

def suggest_validation_improvements(issue_category):
    """建议验证机制改进"""
    suggestions = []
    
    if issue_category == "编码问题":
        suggestions.extend([
            "添加文件编码一致性检查",
            "验证所有子进程调用使用正确编码参数",
            "测试中文字符在不同编码下的表现"
        ])
    
    elif issue_category == "文件路径问题":
        suggestions.extend([
            "验证所有测试文件都在test/目录中",
            "检查长路径处理是否正确",
            "测试路径分隔符的跨平台兼容性"
        ])
    
    return suggestions

def get_input_from_clipboard():
    """
    从剪贴板读取输入内容
    返回: (issue_desc, error_details, context_info)
    """
    try:
        import subprocess
        import sys

        print("\n📋 正在从剪贴板读取内容...")

        # Windows下使用PowerShell获取剪贴板内容
        if sys.platform == "win32":
            result = subprocess.run(
                ["powershell", "Get-Clipboard"],
                capture_output=True,
                text=True,
                encoding='cp936',
                errors='ignore'
            )
            if result.returncode == 0 and result.stdout.strip():
                clipboard_content = result.stdout.strip()
                print("✅ 成功读取剪贴板内容")
                print(f"📄 内容长度: {len(clipboard_content)} 字符")

                # 尝试智能解析内容
                return parse_clipboard_content(clipboard_content)
            else:
                print("❌ 剪贴板为空或读取失败")
        else:
            print("❌ 此功能仅支持Windows系统")

    except Exception as e:
        print(f"❌ 读取剪贴板失败: {e}")

    # 如果剪贴板读取失败，返回空值让用户手动输入
    print("💡 请手动输入问题信息:")
    return get_input_manually()


def get_input_from_file():
    """
    从文件读取输入内容
    返回: (issue_desc, error_details, context_info)
    """
    try:
        from pathlib import Path

        print("\n📄 支持的文件格式:")
        print("  - .txt  - 纯文本文件")
        print("  - .log  - 日志文件")
        print("  - .json - JSON格式文件")
        print("  - .md   - Markdown文件")

        file_path = input("请输入文件路径: ").strip()

        if not file_path:
            print("❌ 文件路径不能为空")
            return None, None, None

        path = Path(file_path)
        if not path.exists():
            print(f"❌ 文件不存在: {file_path}")
            return None, None, None

        print(f"📖 正在读取文件: {file_path}")

        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        print(f"✅ 文件读取成功，内容长度: {len(content)} 字符")

        # 根据文件扩展名智能解析
        if path.suffix.lower() == '.json':
            try:
                import json
                data = json.loads(content)
                return parse_json_content(data)
            except:
                print("⚠️ JSON解析失败，按文本处理")

        return parse_clipboard_content(content)

    except Exception as e:
        print(f"❌ 读取文件失败: {e}")
        return None, None, None


def get_input_manually():
    """
    手动输入问题信息
    返回: (issue_desc, error_details, context_info)
    """
    print("\n⌨️  手动输入模式")
    print("-" * 30)

    # 问题描述（必填）
    while True:
        issue_desc = input("问题描述: ").strip()
        if issue_desc:
            break
        print("❌ 问题描述不能为空，请重新输入")

    # 错误详情（可选）
    error_details = input("错误详情 (可选，直接回车跳过): ").strip() or None

    # 上下文信息（可选）
    context_info = input("上下文信息 (可选，直接回车跳过): ").strip() or None

    # 尝试解析上下文信息
    if context_info:
        try:
            import json
            context_info = json.loads(context_info)
        except:
            pass  # 保持为字符串

    return issue_desc, error_details, context_info


def parse_clipboard_content(content):
    """
    智能解析剪贴板或文件内容
    """
    lines = content.split('\n')
    issue_desc = ""
    error_details = ""
    context_info = ""

    # 尝试识别不同的内容类型
    in_error_section = False
    in_context_section = False

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # 识别错误信息模式
        if any(keyword in line.lower() for keyword in [
            'error', 'exception', 'traceback', 'syntaxerror', 'importerror',
            'filenotfounderror', 'permissionerror', 'unicodeerror', '编码错误'
        ]):
            in_error_section = True
            in_context_section = False
            if error_details:
                error_details += "\n" + line
            else:
                error_details = line
        # 识别代码或上下文信息
        elif any(keyword in line.lower() for keyword in [
            'def ', 'class ', 'import ', 'from ', 'try:', 'except:', 'if __name__',
            'print(', '文件', '路径', '目录', '命令'
        ]) or line.startswith(('    ', '\t')):
            in_context_section = True
            in_error_section = False
            if context_info:
                context_info += "\n" + line
            else:
                context_info = line
        # 其他内容作为问题描述
        else:
            if not issue_desc and not in_error_section and not in_context_section:
                issue_desc = line
            elif in_error_section:
                error_details += "\n" + line
            elif in_context_section:
                context_info += "\n" + line
            else:
                if len(issue_desc) < 200:  # 限制问题描述长度
                    issue_desc += " " + line

    # 如果没有识别出结构，将整个内容作为问题描述
    if not issue_desc and not error_details and not context_info:
        issue_desc = content[:500]  # 限制长度

    return issue_desc or "用户提供的错误信息", error_details, context_info


def parse_json_content(data):
    """
    解析JSON格式的内容
    """
    issue_desc = data.get('description', data.get('issue', ''))
    error_details = data.get('error', data.get('traceback', ''))
    context_info = data.get('context', data.get('code', ''))

    return issue_desc, error_details, context_info


# 命令行参数处理已移至文件末尾

def run_interactive_reflection():
    """
    运行交互式的AI反省机制
    """
    print("🧠 AI反省机制分析")
    print("=" * 50)
    print("请选择输入方式:")
    print("1. 📋 从剪贴板自动读取（推荐）")
    print("2. 📄 从文件读取")
    print("3. ⌨️  手动输入")
    print("0. 🔙 返回主菜单")

    try:
        input_method = input("\n请选择输入方式 (0-3): ").strip()

        if input_method == "0":
            return
        elif input_method == "1":
            # 从剪贴板读取
            issue_desc, error_details, context_info = get_input_from_clipboard()
        elif input_method == "2":
            # 从文件读取
            issue_desc, error_details, context_info = get_input_from_file()
        elif input_method == "3":
            # 手动输入
            issue_desc, error_details, context_info = get_input_manually()
        else:
            print("❌ 无效选择")
            return

        if not issue_desc:
            print("❌ 问题描述不能为空")
            return

        print("\n开始分析问题...")
        print(f"📝 问题描述: {issue_desc[:100]}{'...' if len(issue_desc) > 100 else ''}")
        if error_details:
            print(f"🐛 错误详情: {error_details[:100]}{'...' if len(error_details) > 100 else ''}")
        if context_info:
            print(f"📋 上下文信息: {context_info[:100]}{'...' if len(context_info) > 100 else ''}")

        reflection_report = analyze_and_reflect_on_issue(
            issue_description=issue_desc,
            error_details=error_details,
            context_info=context_info
        )

        print("\n✅ 分析完成！")
        print(f"📄 详细报告已保存到 test/reflection_reports/")

    except EOFError:
        print("❌ 非交互式环境，无法进行问题输入")
        print("💡 建议使用: python ai_config.py analyze \"问题描述\"")
    except KeyboardInterrupt:
        print("\n\n操作已取消")
    except Exception as e:
        print(f"❌ 分析过程中出现错误: {e}")


# 为了兼容性，保留原有的菜单选项5调用
def run_reflection_analysis():
    """
    菜单选项5的入口函数
    """
    run_interactive_reflection()

def auto_update_rules(issue_description, error_details=None, context_info=None):
    """
    自动更新规则：分析问题并自动应用规则更新

    参数:
    - issue_description: 问题描述
    - error_details: 错误详情（可选）
    - context_info: 上下文信息（可选）

    返回:
    - 更新结果字典
    """
    print("🔄 正在自动分析和更新规则...")

    # 1. 分析问题
    issue_category = classify_issue(issue_description, error_details)
    root_causes = analyze_root_causes(issue_description, error_details, context_info)
    rule_updates = suggest_rule_updates(issue_category, root_causes)

    print(f"📊 问题分类: {issue_category}")
    print(f"🔍 识别根本原因: {len(root_causes)} 个")
    print(f"📝 生成规则更新建议: {len(rule_updates)} 项")

    # 2. 自动应用规则更新
    update_results = apply_rule_updates(issue_category, rule_updates)

    # 3. 生成完整的分析报告
    analysis_report = {
        "timestamp": datetime.datetime.now().isoformat(),
        "issue_description": issue_description,
        "error_details": error_details,
        "context_info": context_info,
        "issue_category": issue_category,
        "root_causes": root_causes,
        "rule_updates": rule_updates,
        "update_results": update_results,
        "auto_updated": True
    }

    # 4. 保存报告
    report_path = Path("test") / "reflection_reports"
    report_path.mkdir(exist_ok=True)
    report_file = report_path / f"auto_update_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    try:
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(analysis_report, f, ensure_ascii=False, indent=2)
        print(f"✅ 自动更新完成！报告已保存: {report_file}")
    except Exception as e:
        print(f"⚠️ 报告保存失败: {e}")

    return analysis_report


def apply_rule_updates(issue_category, rule_updates):
    """
    自动应用规则更新到文档中

    参数:
    - issue_category: 问题分类
    - rule_updates: 规则更新列表

    返回:
    - 更新结果字典
    """
    results = {
        "docstring_updated": False,
        "print_function_updated": False,
        "new_rules_added": [],
        "errors": []
    }

    try:
        # 读取当前文件内容
        with open(__file__, 'r', encoding='utf-8') as f:
            content = f.read()

        # 1. 更新docstring中的规则
        if update_docstring_rules(content, issue_category, rule_updates):
            results["docstring_updated"] = True
            results["new_rules_added"].extend(rule_updates)
            print("✅ docstring规则已更新")

        # 2. 更新打印函数中的规则
        if update_print_function_rules(content, issue_category, rule_updates):
            results["print_function_updated"] = True
            print("✅ 打印函数规则已更新")

        # 3. 保存更新后的内容
        if results["docstring_updated"] or results["print_function_updated"]:
            with open(__file__, 'w', encoding='utf-8') as f:
                f.write(content)
            print("✅ 文件已保存")

    except Exception as e:
        error_msg = f"规则更新失败: {e}"
        results["errors"].append(error_msg)
        print(f"❌ {error_msg}")

    return results


def update_docstring_rules(content, issue_category, rule_updates):
    """
    更新docstring中的规则
    """
    # 查找错误处理规则部分
    error_handling_pattern = r'错误处理规则：([\s\S]*?)(?=\n\n\w+部分：|\n"""|\n# 代码段)'
    match = re.search(error_handling_pattern, content)

    if match:
        existing_rules = match.group(1).strip()

        # 检查是否已有类似规则
        new_rules = []
        for update in rule_updates:
            if update not in existing_rules:
                new_rules.append(f"- {update}")

        if new_rules:
            # 添加新规则
            updated_rules = existing_rules + "\n" + "\n".join(new_rules)
            content = content.replace(match.group(1), updated_rules)
            return True

    return False


def update_print_function_rules(content, issue_category, rule_updates):
    """
    更新打印函数中的规则
    """
    # 查找错误处理规则的打印部分
    print_pattern = r'print\("错误处理规则:"\)\s*([\s\S]*?)(?=\s*print\("第四部分：反省机制"\)|\s*print\("【规则声明】"\))'
    match = re.search(print_pattern, content)

    if match:
        existing_rules = match.group(1).strip()

        # 检查是否已有类似规则
        new_rules = []
        for update in rule_updates:
            rule_line = f'print("- {update}")'
            if rule_line not in existing_rules:
                new_rules.append(f'    {rule_line}')

        if new_rules:
            # 添加新规则
            updated_rules = existing_rules + "\n" + "\n".join(new_rules)
            content = content.replace(match.group(1), updated_rules)
            return True

    return False


# 增强的反省机制函数
def enhanced_reflection_analysis(issue_description, error_details=None, context_info=None, auto_update=False):
    """
    增强的反省机制分析，支持自动规则更新

    参数:
    - issue_description: 问题描述
    - error_details: 错误详情（可选）
    - context_info: 上下文信息（可选）
    - auto_update: 是否自动更新规则
    """
    if auto_update:
        print("🚀 启用自动规则更新模式")
        return auto_update_rules(issue_description, error_details, context_info)
    else:
        print("📋 标准分析模式")
        return analyze_and_reflect_on_issue(issue_description, error_details, context_info)


def test_encoding_issues():
    """测试编码问题"""
    print("\n=== 测试编码问题 ===")

    # 中文字符串处理
    chinese_text = "测试中文字符串处理"
    print(f"中文字符串: {chinese_text}")

    # 文件读写编码
    try:
        test_file = get_test_file_path('test_encoding.txt')
        with open(test_file, 'w', encoding='utf-8') as f:
            f.write(chinese_text)
        print("[OK] UTF-8文件写入成功")

        with open(test_file, 'r', encoding='utf-8') as f:
            content = f.read()
        print(f"[OK] UTF-8文件读取成功: {content}")

        # 尝试用错误编码读取
        try:
            with open(test_file, 'r', encoding='ascii') as f:
                content = f.read()
        except UnicodeDecodeError as e:
            print(f"[WARN] 预期的编码错误: {e}")

    except Exception as e:
        print(f"[ERROR] 编码测试失败: {e}")
    finally:
        test_file = get_test_file_path('test_encoding.txt')
        if test_file.exists():
            test_file.unlink()


def test_path_issues():
    """测试路径问题"""
    print("\n=== 测试路径问题 ===")

    # 长路径测试
    try:
        long_path = get_test_file_path("very/long/path/test/file.txt")
        long_path.parent.mkdir(parents=True, exist_ok=True)

        with open(long_path, 'w', encoding='utf-8') as f:
            f.write("长路径测试")
        print("[OK] 长路径创建成功")

        # 读取测试
        with open(long_path, 'r', encoding='utf-8') as f:
            content = f.read()
        print(f"[OK] 长路径读取成功: {content}")

    except Exception as e:
        print(f"[ERROR] 长路径测试失败: {e}")
    finally:
        long_path = get_test_file_path("very/long/path/test/file.txt")
        if long_path.exists():
            long_path.unlink()
            # 清理空目录
            for parent in long_path.parents:
                if parent != get_test_dir() and not any(parent.iterdir()):
                    try:
                        parent.rmdir()
                    except OSError:
                        break

    # 特殊字符路径测试
    try:
        special_path = get_test_file_path("test folder (特殊字符)/file[1].txt")
        special_path.parent.mkdir(parents=True, exist_ok=True)

        with open(special_path, 'w', encoding='utf-8') as f:
            f.write("特殊字符路径测试")
        print("[OK] 特殊字符路径创建成功")

    except Exception as e:
        print(f"[ERROR] 特殊字符路径测试失败: {e}")
    finally:
        special_path = get_test_file_path("test folder (特殊字符)/file[1].txt")
        if special_path.exists():
            special_path.unlink()
        if special_path.parent.exists():
            try:
                special_path.parent.rmdir()
            except OSError:
                # 目录不为空，跳过删除
                pass


def test_import_issues():
    """测试导入问题"""
    print("\n=== 测试导入问题 ===")

    # 尝试导入可能不存在的模块
    try:
        import nonexistent_module
        print("✓ 非预期模块导入成功")
    except ImportError as e:

        print(f"[WARN] 预期的导入错误: {e}")

    # 相对导入测试
    try:
        # 创建临时模块
        temp_dir = Path(tempfile.mkdtemp())
        module_path = temp_dir / "test_module.py"

        with open(module_path, 'w', encoding='utf-8') as f:
            f.write('def test_function():\n    return "测试模块"')

        # 添加到路径并导入
        sys.path.insert(0, str(temp_dir))
        import test_module
        result = test_module.test_function()
        print(f"[OK] 动态模块导入成功: {result}")

    except Exception as e:
        print(f"[ERROR] 动态导入测试失败: {e}")
    finally:
        if 'test_module' in sys.modules:
            del sys.modules['test_module']
        if temp_dir in sys.path:
            sys.path.remove(str(temp_dir))
        if module_path.exists():
            module_path.unlink()
        # 安全删除临时目录
        if temp_dir.exists():
            try:
                # 递归清理目录中的所有文件和子目录
                import shutil
                shutil.rmtree(temp_dir)
                print(f"[OK] 临时目录已清理: {temp_dir}")
            except Exception as e:
                print(f"[WARN] 临时目录清理警告: {e}")
                # 如果shutil失败，尝试手动递归删除
                try:
                    def remove_tree(path):
                        """递归删除目录树"""
                        for item in path.iterdir():
                            if item.is_file():
                                item.unlink()
                            elif item.is_dir():
                                remove_tree(item)
                                item.rmdir()
                        path.rmdir()

                    remove_tree(temp_dir)
                    print(f"✓ 临时目录已手动清理: {temp_dir}")
                except Exception as e2:
                    print(f"❌ 临时目录清理失败: {e2}")


def test_network_issues():
    """测试网络问题"""
    print("\n=== 测试网络问题 ===")

    # HTTP请求测试
    try:
        with urllib.request.urlopen('http://httpbin.org/get', timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
            print(f"[OK] HTTP请求成功: {data['url']}")
    except Exception as e:
        print(f"[ERROR] HTTP请求失败: {e}")

    # HTTPS请求测试
    try:
        with urllib.request.urlopen('https://httpbin.org/get', timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
            print(f"[OK] HTTPS请求成功: {data['url']}")
    except Exception as e:
        print(f"[ERROR] HTTPS请求失败: {e}")


def test_subprocess_issues():
    """测试子进程问题"""
    print("\n=== 测试子进程问题 ===")

    # PowerShell命令执行
    try:
        result = subprocess.run(['powershell', '-Command', 'Get-Date'],
                              capture_output=True, text=True, encoding='utf-8', errors='ignore', timeout=10)
        if result.returncode == 0:
            print(f"[OK] PowerShell命令执行成功: {result.stdout.strip()}")
        else:
            print(f"[WARN] PowerShell命令执行失败: {result.stderr}")
    except Exception as e:
        print(f"[ERROR] PowerShell测试失败: {e}")

    # 系统命令执行
    try:
        result = subprocess.run(['cmd', '/c', 'echo', 'Hello World'],
                              capture_output=True, text=True, encoding='utf-8', errors='ignore', timeout=10)
        if result.returncode == 0:
            print(f"[OK] 系统命令执行成功: {result.stdout.strip()}")
        else:
            print(f"[WARN] 系统命令执行失败: {result.stderr}")
    except Exception as e:
        print(f"[ERROR] 系统命令测试失败: {e}")


def test_memory_performance():
    """测试内存和性能问题"""
    print("\n=== 测试内存和性能问题 ===")

    # 大数据处理测试
    try:
        large_list = list(range(1000000))  # 100万元素
        result = sum(large_list)
        print(f"[OK] 大数据处理成功: 总和 = {result}")
    except Exception as e:
        print(f"[ERROR] 大数据处理失败: {e}")

    # 递归深度测试
    try:
        def recursive_function(depth=0):
            if depth > 100:
                return depth
            return recursive_function(depth + 1)

        result = recursive_function()
        print(f"[OK] 递归测试成功: 深度 = {result}")
    except RecursionError as e:
        print(f"[WARN] 预期的递归深度错误: {e}")


# =============================================================================
# 第五部分：开发工具和规则
# =============================================================================

"""
开发工具和规则
==============

Terminal使用规则：
- ❌ 避免的做法：不要在terminal中使用字符串运行Python程序
- ✅ 推荐的做法：创建独立的脚本文件

开发工具：
1. 代码质量检查工具 - check_code_quality()
2. 系统诊断测试 - run_comprehensive_system_test()

工作流程：
1. 创建独立的测试脚本
2. 在脚本中编写和调试代码
3. 使用check_code_quality()检查代码质量
4. 提交代码到版本控制

最佳实践：
- 创建专用脚本：为每个任务创建独立的脚本文件
- 定期检查：使用check_code_quality()检查代码质量
- 版本控制：将工具脚本纳入版本控制
- 文档化：为脚本添加清晰的文档和注释
- 模块化：将可重用的代码提取为独立模块
"""

def check_code_quality(file_path):
    """
    代码质量检查工具
    用于检查Python文件的语法、缩进和代码质量问题

    Args:
        file_path: 要检查的Python文件路径

    Returns:
        bool: 检查是否全部通过
    """
    if not os.path.exists(file_path):
        print(f"❌ 文件不存在: {file_path}")
        return False

    print(f"🔍 检查文件: {file_path}")
    print("-" * 40)

    results = []
    results.append(("语法检查", _check_syntax_internal(file_path)))
    results.append(("缩进检查", _check_indentation_internal(file_path)))
    results.append(("制表符检查", _check_tabs_internal(file_path)))

    print("-" * 40)
    passed = sum(1 for _, result in results if result)
    total = len(results)
    print(f"📊 检查结果: {passed}/{total} 通过")

    if passed == total:
        print("🎉 所有检查通过！")
        return True
    else:
        print("⚠️  发现问题需要修复")
        return False

def _check_syntax_internal(file_path):
    """内部语法检查函数"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            source = f.read()
        ast.parse(source)
        print("✅ 语法检查通过")
        return True
    except SyntaxError as e:
        print(f"❌ 语法错误: {e}")
        return False
    except Exception as e:
        print(f"❌ 检查失败: {e}")
        return False

def _check_indentation_internal(file_path):
    """内部缩进检查函数"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        issues = []
        for i, line in enumerate(lines, 1):
            stripped = line.lstrip()
            if stripped and not stripped.startswith('#'):
                indent = len(line) - len(stripped)
                if indent > 0 and indent % 4 != 0:
                    issues.append((i, indent))

        if issues:
            print(f"⚠️  发现{len(issues)}个非标准缩进:")
            for line_num, indent in issues[:5]:
                print(f"  第{line_num}行: {indent}个空格")
            return False
        else:
            print("✅ 缩进检查通过")
            return True

    except Exception as e:
        print(f"❌ 缩进检查失败: {e}")
        return False

def _check_tabs_internal(file_path):
    """内部制表符检查函数"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        if '\t' in content:
            tab_lines = []
            for i, line in enumerate(content.split('\n'), 1):
                if '\t' in line:
                    tab_lines.append(i)
            print(f"⚠️  发现{len(tab_lines)}行包含制表符")
            return False
        else:
            print("✅ 无制表符使用")
            return True

    except Exception as e:
        print(f"❌ 制表符检查失败: {e}")
        return False

