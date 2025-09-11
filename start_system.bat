@echo off
chcp 65001 >nul
echo 股票监控系统启动器
echo ========================================

echo 正在启动系统 (自动环境检查)...
python start_system.py %*

if %errorlevel% neq 0 (
    echo.
    echo 如果出现环境问题，请手动激活rqsdk环境：
    echo conda activate rqsdk
    echo 然后重新运行：python start_system.py
)

echo.
echo 按任意键退出...
pause >nul
