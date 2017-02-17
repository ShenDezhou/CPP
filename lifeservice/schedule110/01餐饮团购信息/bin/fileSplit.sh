file=$1
num=$2

for i in `seq 0 $num`
do
	awk 'NR%'$num'=='$i'{print}' $1 > data/shop.$i
done
mv data/shop.0 data/shop.$num
