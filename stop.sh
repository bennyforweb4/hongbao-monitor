#!/bin/bash
cd "$(dirname "$0")"

if [ -f monitor.pid ]; then
    PID=$(cat monitor.pid)
    if kill -0 "$PID" 2>/dev/null; then
        kill "$PID"
        echo "已停止 (PID: $PID)"
    else
        echo "PID 文件中的进程不存在"
    fi
    rm monitor.pid
fi
pkill -f "python.*app.py" 2>/dev/null && echo "已清理残留进程" || echo "无残留进程"
