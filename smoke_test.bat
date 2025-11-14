@echo off
python queuectl.py enqueue "{\"id\":\"t1\",\"command\":\"echo OK\",\"max_retries\":1}"
python queuectl.py enqueue "{\"id\":\"t2_fail\",\"command\":\"cmd /c exit 3\",\"max_retries\":2}"

python queuectl.py worker start --count 2

timeout /t 1

python queuectl.py status

timeout /t 8

python queuectl.py dlq list

python queuectl.py worker stop
