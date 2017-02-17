

find ./data -ctime +15 | xargs rm -f {}

today=$(date "+%Y%m%d")
/usr/bin/python bin/ServiceAppMonitor.py > data/serviceapp_report_${today}.html 

# 可以改用php版本的发邮件脚本 （咨询冯孟孟）
/usr/bin/sogou-mds-syncfile -m service_app_spider -l data/serviceapp_report_${today}.html



