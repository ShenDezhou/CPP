
. ./bin/Tool.sh

# 分发数据到Output下
function dispatch() {
	srcPath=$1; type=$2

	for city in $(ls $srcPath/); do
		if [ "$city" = "movie" -o "$city" = "other" ]; then
			LOG "movie/other dir, continue."
			continue
		fi

		for srcFile in $(ls $srcPath/$city/$type/*.table); do
			destFile=${srcFile/Input/Output}
			destDir=$(dirname $destFile)
			if [ ! -d $destDir ]; then
				mkdir -p $destDir
			fi

			srcFileLine=$(cat $srcFile | wc -l)
			if [ $srcFileLine -le 1 ]; then
				LOG "$city : $srcFile is too small"	
				continue
			fi

			mv $destFile $destFile.bak
			cp $srcFile $destFile
		done

		LOG "dispatch data for $city city done."
	done
}

if [ $# -lt 2 ]; then
	echo "Usage: sh $0 srcPath type"
else
	dispatch $1 $2
fi
