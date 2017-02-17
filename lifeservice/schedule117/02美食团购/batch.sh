


for idsFile in $(ls tuandata/update/*.ids ); do
	file=${idsFile/.ids/.data.format}
	file=${file/update/all}
	
	mv $file $file.addupdate

	cp $idsFile $file


done
