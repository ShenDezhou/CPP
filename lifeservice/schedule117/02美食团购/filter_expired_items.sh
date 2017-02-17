


awk -F'\t' '{
	if (NF != 2) {
		next
	}
	city=$1; ids=$2;
	len = split(ids, idArray, ",")
	for (idx in idArray) {
		validids[idArray[idx]]
	}
	allLen += len

} END {
	print allLen
}' tuandata/all/beijing.idlist

