file=$1
num=$2

for i in `seq 0 $num`
do
	awk 'NR%'$num'=='$i'{print}' $1 > data/play.$i
done
mv data/play.0 data/play.$num
