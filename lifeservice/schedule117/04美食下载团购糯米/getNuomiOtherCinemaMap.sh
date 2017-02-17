
python bin/getNuomiOtherCinemaMap.py > nuomi_ting_nomatch


awk -F'\t' '{
	id=$1; source=$2;
	for (i=3; i<=NF; ++i) {
		print id "\t" source "\t" $i
	}
}' nuomi_ting_nomatch | sort -t$'\t' -k1,1 -k3,3 | awk -F'\t' '{
	id=$1
	if (id != lastid) {
		lastid = id
		print "\n"
	}
	print
}' | awk -F'\t' '{
	if ($0 == "") {
		if (lastNMTing != "") {
			print lastNMTing
			lastkey=""; lastNMTing = ""
			
		}
		print; next
	}
	id=$1; source=$2; ting=$3
	if (source=="·ÇÅ´Ã×")  {
		key = substr(ting, 0, 2)
		if (key == lastkey) {
			print $0 "\t" lastNMTing
		} else {
			print $0
			#print lastNMTing
		}
		lastkey=""; lastNMTing = ""

	} if (source=="Å´Ã×") {
		lastkey = substr(ting, 0, 2)
		lastNMTing = $0		
	}
} END {
	if (lastNMTing != "") {
		print lastNMTing
	}
}' > nuomi_ting_nomatch.sort
