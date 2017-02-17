
awk -F'\t' '{
	if (FNR == 1) { print; next }
	poi = $7
	len = split(poi, array, ",")
	if (len == 2) {
		$7 = array[2] "," array[1]
	} else {
		print "ERROR\t" $0
	}

	line = $1
	for(i=2; i<=NF; ++i) {
		line = line "\t" $i
	}
	print line
}' $1
