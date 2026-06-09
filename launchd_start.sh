#!/bin/bash
# LaunchAgent wrapper
exec >> "/Users/bennylife/Desktop/富豪群红包监听/run.log" 2>&1
echo "[$(date)] LaunchAgent starting monitor.py"
cd "/Users/bennylife/Desktop/富豪群红包监听"
exec /usr/bin/python3 "/Users/bennylife/Desktop/富豪群红包监听/monitor.py"
