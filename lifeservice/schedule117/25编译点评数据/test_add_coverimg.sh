
conf="conf/dianping_beijing_cover_img"


function add() {
	input=$1;  output=$2;
	awk -F'\t' 'ARGIND == 1 {
		url=$1; photo=$2
		if (photo != "") {
			urlPhoto[url] = photo
		}
	} ARGIND == 2 {
		key=$1; value=$2;
		print
		if (key == "url") { url = value; next; }
		if (key == "title") {
			if (url in urlPhoto) {
				print "photo\t" urlPhoto[url]
			}
		}

	}' $conf $input > $output

}

for file in $(ls Crawler/beijing/restaurant/); do
	input="Crawler/beijing/restaurant/$file"
	output=bak/$file	
	echo $file
	#echo $output
	
	add $input $output
done
