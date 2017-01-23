<?php
	include "db.php";
	$err = 0;
	//获取模板编号
	$fileID = 0;
	if($_POST['schemaFileID'] != 0)
		$fileID = $_POST['schemaFileID'];
	else
		$fileID = $_POST['fileID'];

	//处理上传的xml文件
	$dataFileName = "dataFile".$fileID.".xml";
	if(file_exists("upload/".$dataFileName)) {
		echo $_FILES["xmlfile"]["name"]."在校验中，请稍后进行。 <a href=index.php>返回</a>";
		exit;
	} else {
		if($_POST['xmlurl'] != '') {
			$xml = file_get_contents($_POST['xmlurl']);
			if(!isset($xml) || strlen($xml)==0) {
				echo $_POST['xmlurl']."无法访问。 <a href=index.php>返回</a>";
				$err = 1;
			} else { //保存文件
				$fp = fopen("./upload/".$dataFileName, "w");
				//echo $xml;
				fwrite($fp, $xml);
				fclose($fp);
			}
		} else if(!move_uploaded_file($_FILES['xmlfile']['tmp_name'], "./upload/".$dataFileName)) {
			echo "数据文件上传失败。</br> <a href=index.php>返回</a>";
			$err = 1;
		}
	}

	//只有在用户输入模板号时，才保存上传的scheme文件和length文件，如果存在，则覆盖。
	if($_POST['schemaFileID'] == 0) {
		//处理shemale文件
		//从svn同步schema和length文件
		$schemaFileName = "schemaFile".$fileID.".xsd";
		//$cmd_prefix = "scp root@10.10.122.81:/search/odin/daemon/web_vr_exspider_new/ExVrFetcher/data/base/";
		$cmd = "sh templateSync.sh";
		system($cmd, $res);
		if(!file_exists("templates/".$schemaFileName)) {
			echo $schemaFileName." 不存在，请联系做schema文件的同学将其同步到svn。</p>";
			$err = 1;
		}

		//处理length文件
		$lengthFileName = "lengthFile".$fileID.".txt";
		if(!file_exists("templates/".$lengthFileName)) {
			echo $lengthFileName." 不存在，请联系做length文件的同学将其同步到svn。</p>";
			$err = 1;
		}
	}
	
	if($err == 1) {
		echo "<a href=index.php>返回</a>";
		unlink("upload/".$dataFileName);
		exit;
	}


	//校验xml
	$root_path = dirname(__FILE__);
	$command = $root_path."/bin/check_xml upload/".$dataFileName." ".$fileID." ".$root_path."/templates/ 2>&1";
	exec($command, $res); //校验程序的输出信息会被记录在$res字符串数组中

	//返回校验结果
	$res_size = count($res);
	if($res_size > 0) {
		if($res[$res_size-1] == "SUCCESS: xml data is valid.") { //通过校验，最后一行的日志信息为UCCESS: xml data is valid.”
			echo $_FILES["xmlfile"]["name"]."通过校验，模板号为".$fileID."</br>";
		} else {
			echo $_FILES["xmlfile"]["name"]." 校验失败，模板号为".$fileID."。错误信息如下：</p>";
			echo "<textarea rows=\"30\" cols=\"150\">";
			$limit = $res_size;
			//当错误信息行数太多时，ie会卡死，因此限制最多显示1000
			//if($res_size > 1000) {
				//$limit = 1000;
			//}
			for($i=0; $i<$limit; $i++) {
				if(strstr($res[$i], "NOTICE"))
					continue;
				echo $res[$i]."\n";
			}
			echo "</textarea>";

		}
	} else {
		echo "校验程序运行异常。</br>";
	}

	
	//删除本次xml数据文件
	unlink("upload/".$dataFileName);
	//保存模板号
	if($_POST['fileID'] != '') {
		$conn = db_init();
		//判断该模板号是否已存在
		$sql = "select count(*) cnt from xmlverify where template_id=".$_POST['fileID'];
		$result = mysql_query($sql);
		$row = mysql_fetch_array($result);
		$count =  $row['cnt'];
		if($count == 0) {
			$sql = "insert into xmlverify (template_id) values(".$_POST['fileID'].")";
			mysql_query($sql);
		}
		mysql_close($conn);
	}

	echo "<a href=\"index.php\">返回</a>";
?>
