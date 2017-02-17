
NuomiShopPath=/fuwu/Source/Cooperation/Nuomi/data
NuomiTuanOnlinePath=/fuwu/Source/Cooperation/Nuomi/tuan
for nuomiShop in $(ls $NuomiShopPath/*shop); do
	city=${nuomiShop##*/}
	city=${city/_shop/}
	
	#LOG "handle $city" 
	dianpingShopFile=/fuwu/Merger/Output/$city/restaurant/dianping_detail.baseinfo.table
	dianpingTuanFile=/fuwu/Merger/Output/$city/restaurant/dianping_detail.tuan.table
	nuomiShopFile=$nuomiShop
	nuomiTuanFile=${nuomiShop/shop/tuan}	

	nuomiTuanOnline=	

	if [ ! -f $dianpingShopFile ]; then
		LOG "$city has not $dianpingShopFile" 
		continue
	fi
	
	echo "create_nuomi_tuan_info $ShopMerge $nuomiTuanFile $nuomiShopFile > $NuomiTuanOnlinePath/$city"
done
