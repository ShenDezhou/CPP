


awk -F'\t' 'ARGIND==1 {
	if ($0~/^#/ || $0 == "") {
		next
	}
	filterids[$1]
} ARGIND == 2 {
	if (FNR == 1) {
		print; next
	}
	if ($1 in filterids) {
		next
	}
	print
}' conf/movie_black_list_conf  Output/movie/movie_detail.table 
