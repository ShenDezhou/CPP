. /search/liubing/Tool/Shell/Tool.sh 

# 备份数据
# arg1: 新数据  arg2: 原备份数据  arg3: 主键列
function backupFile() {
	newFile=$1;  backFile=$2;  keyidx=$3;
	if [ ! -f $newFile -o $keyidx -le 0 ]; then
		LOG "[Error]: when backup file $1 to $2 with keyidx of $3"
		return -1
	fi
	# 如果备份文件不存在，直接复制
	if [ ! -f $backFile ]; then
		cp $newFile $backFile
		LOG "$backFile is not exist, copy $newFile to $backFile"
		return 0
	fi

	today=$(date +%Y%m%d)
	output=$backFile.$today
	# 合并最新文件与备份文件，取两者并集，优先取新文件的内容
	awk -F'\t' -v KeyIdx=$keyidx 'ARGIND==1 {
		print;  keys[$KeyIdx]
	} ARGIND == 2 {
		if ($KeyIdx in keys) {
			next
		}
		print
	}' $newFile $backFile > $output

	mv $backFile $backFile.bak
	cp $output $backFile
	
	LOG "merge $1 and $2, backup into $output"
}


# 备份电影的详情、演员、片花、评论数据
function backup_movie_info() {
	movieOnlinePath=/fuwu/Merger/Output/movie/
	movieBackupPath=/fuwu/Merger/history/movie/
	
	find $movieBackupPath -ctime +14 | xargs rm -f {} 

	backupFile $movieOnlinePath/movie_detail.table $movieBackupPath/movie_detail.table 2
	backupFile $movieOnlinePath/movie_actors.table $movieBackupPath/movie_actors.table 2
	backupFile $movieOnlinePath/movie_videos.table $movieBackupPath/movie_videos.table 2
	backupFile $movieOnlinePath/movie_comments.table $movieBackupPath/movie_comments.table 2
}



function backup() {
	# 备份电影的数据
	backup_movie_info
}


backup
