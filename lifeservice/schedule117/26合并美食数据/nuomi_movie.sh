

function add_nuomi_movie() {
	NuomiCMRelation=/fuwu/Source/Cooperation/Nuomi/data/nuomi_movie_cm_relation
	MovieIDMap=/fuwu/Source/tmp/movie_id_merge
	CinemaIDMap=/fuwu/Source/conf/cinema_vrid_id_conf

	[ ! -f $NuomiCMRelation -o ! -f $MovieIDMap -o ! -f $CinemaIDMap ] && return 

	awk -F'\t' 'BEGIN {
		lineNum = 2000000
	} ARGIND==1 {
		# 加载电影ID的映射
		if (NF < 3) {
			next
		}
		movieid=$1;  source=$2;  nuomiMovieid=$3;
		if (source~/糯米/ && movieid!~/糯米/) {
			movieidMap[nuomiMovieid] = movieid
		}
	} ARGIND==2 {
		# 加载影院ID映射
		if (NF < 3) {
			next
		}
		source=$1;  nuomiCinemaid=$2;  cinemaid=$3;
		if (source!~/糯米/ || NF < 5) {
			next
		}
		cinemaidMap[nuomiCinemaid] = cinemaid
	} ARGIND == 3 {
		if (NF < 3) {
			next
		}
		nuomiCinemaid=$1;  nuomiMovieid=$2;
		if (!(nuomiCinemaid in cinemaidMap) || !(nuomiMovieid in movieidMap)) {
			next
		}
		$1=cinemaidMap[nuomiCinemaid];  $2=movieidMap[nuomiMovieid]

		line=(++lineNum)
		for(idx=1; idx<=NF; ++idx) {
			line = line "\t" $idx
		}
		print line
	}' $MovieIDMap $CinemaIDMap $NuomiCMRelation
	LOG "add nuomi's cinema_movie relation done."
}

add_nuomi_movie
