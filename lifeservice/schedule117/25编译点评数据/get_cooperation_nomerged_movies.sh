
#Output/movie/movie_detail.table


#tmp/cooperation_movie
# Maoyan  345775  水果宝贝之水果总动员    0       杨劲松

:<<EOF
awk -F'\t' 'ARGIND==1 {
	if (NF < 5 || $5 == "") {
		next
	}
	source=$1;  id=$2;  director=$5
	gsub(/·/, "", director)
	idDirector[source "_" id] = director
} ARGIND==2 {
	if (FNR == 1 || $2 != "") {
		next
	}
	id=$1; title=$3
	if (id in idDirector) {
		print title "\t" idDirector[id]
	}
}' tmp/cooperation_movie Output/movie/movie_detail.table
EOF




awk -F'\t' '{
	if (NF < 6 || $1!~/_/) {
		next
	}
	if ($6=="" || $6 == "director") {
		next
	}

	id=$1;  title=$4; director=$6
	gsub(/·/, "", director)

	if (!(id in existids)) {
		existids[id]
		print id "\t" title "\t" director
	}

}' tmp/movie_id_merge #/fuwu/Spider/Mtime/tmp/movie_titles 


