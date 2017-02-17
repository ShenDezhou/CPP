movieidconf=/fuwu/Source/tmp/movie_id_merge
#221228  Maoyan  247645


# 抽取各家的电影数据

awk -F'\t' '{
	key = $1 "\t" $2 "\t" $3
	if (key in exist) {
		next
	}
	exist[key]
	print $1 "\t" $2 "\t" $3 "\t" $17 "\t" $18
}' /fuwu/Source/Cooperation/*/data/*_movie_movie_detail > other


awk -F'\t' '{
	if ($1!~/^[0-9]+$/) {
		next
	}
	print $1 "\t" $2 "\t" $3 "\t" $17 "\t" $18
}' /fuwu/Source/Output/movie/movie_detail.table > mtime


awk -F'\t' 'ARGIND==1 {
	mtimeid=$1; source=$2; id=$3
	idMap[id"\t"source] = mtimeid
} ARGIND==2 {
	id=$1; title=$3; content=$4"\t"$5
	idTitle[id] = title
	print id "\tMtime\t" title "\t" content
} ARGIND==3 {
	id=$1; source=$2; title=$3; content=$4"\t"$5
	#print "[" id "\t" source "]"
	if (id "\t" source in idMap) {
		mtimeid = idMap[id "\t" source]
		print mtimeid "\t" source "\t" idTitle[mtimeid] "\t" content
	}
	

}' $movieidconf mtime other | sort -t$'\t' -k1,1 -k2,2 > zzz

awk -F'\t' 'BEGIN {
	print "id\ttitle\tmtime\tmtime\tmaoyan\tmaoyan\tdianping\tdianping\twepiao\twepiao\tkou\tkou"

}{
	id=$1; source=$2; title=$3; content=$4"\t"$5
	if (id != lastid) {
		print lastid "\t" lasttitle "\t" mtime "\t" maoyan "\t" dianping "\t" wepiao "\t" kou
		lastid=id; lasttitle=title; mtime=""; maoyan=""; kou=""; dianping=""; wepiao="";
	}
	if (source == "Mtime") { mtime = content ;}
	if (source == "Maoyan") { maoyan = content }
	if (source == "Dianping") { dianping = content }
	if (source == "Kou") { kou = content }
	if (source == "Wepiao") { wepiao = content }

}' zzz

