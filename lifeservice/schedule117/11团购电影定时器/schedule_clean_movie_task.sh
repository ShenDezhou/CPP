


today=$(date +%Y%m%d)
cat /task/result/system/task-118/14* >  /task/result/system/task-118/baseinfo.$today
rm -f /task/result/system/task-118/14*


cat /task/result/system/task-119/14* > /task/result/system/task-119/maoyan_shortdesc.$today
rm -f /task/result/system/task-119/14*


cat /task/result/system/task-120/14* > /task/result/system/task-120/maoyan_comments.$today
rm -f /task/result/system/task-120/14*
