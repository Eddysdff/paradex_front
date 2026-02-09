@echo off
chcp 65001 >nul
title Paradex BTC 秒开关

echo ========================================
echo   Paradex BTC 秒开关策略
echo ========================================
echo.

REM 切换到脚本所在目录
cd /d "%~dp0"

REM 检查 Python
where python >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [错误] 未找到 Python，请先安装 Python 3.7+
    pause
    exit /b 1
)

REM 检查依赖
python -c "import paradex_py" 2>nul
if %ERRORLEVEL% neq 0 (
    echo [提示] 正在安装依赖...
    pip install -r requirements.txt
)

echo [启动] 运行策略...
echo.
python scalper.py

echo.
echo [结束] 策略已停止
pause
