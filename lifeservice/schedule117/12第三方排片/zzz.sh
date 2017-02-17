awk -F'\t' '{
		key = $1 "" $2 "" $4 "" $6 "" $9
		if (key in existKeys) {
			next
		}
		existKeys[key]		
	print
}' /fuwu/Source/Cooperation/Nuomi/data/nuomi_movie_cm_relation  > /fuwu/Source/Cooperation/Nuomi/data/nuomi_movie_cm_relation.uniq
