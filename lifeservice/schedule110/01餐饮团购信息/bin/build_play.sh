date=`date +%Y%m%d%H%M`
echo $date
rm tmp/play.hui -rf
while read line
do
    echo $line
    curl -s $line > tmp/page.play
    python bin/parser_play.py tmp/page.play $line >> tmp/play.hui
done < $1
date=`date +%Y%m%d%H%M`
echo $date

