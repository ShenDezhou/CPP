
# id      title   brand   address province        city    district        area    poi     tel     businesstime    hasimax score
#CinemaDetail="/fuwu/Merger/Output/movie/cinema_detail.table"

# id      cinemaid        movieid source  date    week    start   end     price   room    language        dimensional     seat
#CinemaMovie="/fuwu/Merger/Output/movie/cinema_movie_rel.table"


CinemaDetail=$1;  MovieDetail=$2;  CinemaMovie=$3;

awk -F'\t' 'BEGIN {
	print "id\tprovince\tcity\tmovieid"
	lineCnt = 0;
} ARGIND == 1 {
	province=$6; city = $7;  cinemaid = $1;
	if (province != "") {
		cityprovince[city] = province;
	}
	cinemaCity[cinemaid] = city
} ARGIND==2 {
	movieids[$1]
} ARGIND == 3 {
	cinemaid = $2;  movieid = $3;
	if (!(cinemaid in cinemaCity) || !(movieid in movieids)) {
		next
	}
	city = cinemaCity[cinemaid];
	item = cityprovince[city] "\t" city "\t" movieid
	if (item in itemArray) {
		next
	}
	itemArray[item]
	print ++lineCnt "\t" item

}' $CinemaDetail $MovieDetail $CinemaMovie
