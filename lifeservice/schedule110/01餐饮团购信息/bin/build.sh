date=`date +%Y%m%d%H%M`
echo $date
rm tmp/shop.hui -rf
while read line
do
    echo $line
    curl -s $line > tmp/page
    python bin/parser.py tmp/page $line >> tmp/shop.hui
done < $1
date=`date +%Y%m%d%H%M`
echo $date

