#!/bin/bash
cd "$(dirname "$0")"

if [ -f monitor.pid ]; then
    OLD_PID=$(cat monitor.pid)
    kill "$OLD_PID" 2>/dev/null
    rm monitor.pid
fi
pkill -f "python.*app.py" 2>/dev/null
sleep 1

nohup python3 app.py >> run.log 2>&1 &
echo $! > monitor.pid
echo "已启动 (PID: $!), 访问 http://localhost:8888"
