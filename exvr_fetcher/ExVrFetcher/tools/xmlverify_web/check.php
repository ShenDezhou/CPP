<?php
	include "db.php";
	$err = 0;
	//��ȡģ����
	$fileID = 0;
	if($_POST['schemaFileID'] != 0)
		$fileID = $_POST['schemaFileID'];
	else
		$fileID = $_POST['fileID'];

	//�����ϴ���xml�ļ�
	$dataFileName = "dataFile".$fileID.".xml";
	if(file_exists("upload/".$dataFileName)) {
		echo $_FILES["xmlfile"]["name"]."��У���У����Ժ���С� <a href=index.php>����</a>";
		exit;
	} else {
		if($_POST['xmlurl'] != '') {
			$xml = file_get_contents($_POST['xmlurl']);
			if(!isset($xml) || strlen($xml)==0) {
				echo $_POST['xmlurl']."�޷����ʡ� <a href=index.php>����</a>";
				$err = 1;
			} else { //�����ļ�
				$fp = fopen("./upload/".$dataFileName, "w");
				//echo $xml;
				fwrite($fp, $xml);
				fclose($fp);
			}
		} else if(!move_uploaded_file($_FILES['xmlfile']['tmp_name'], "./upload/".$dataFileName)) {
			echo "�����ļ��ϴ�ʧ�ܡ�</br> <a href=index.php>����</a>";
			$err = 1;
		}
	}

	//ֻ�����û�����ģ���ʱ���ű����ϴ���scheme�ļ���length�ļ���������ڣ��򸲸ǡ�
	if($_POST['schemaFileID'] == 0) {
		//����shemale�ļ�
		//��svnͬ��schema��length�ļ�
		$schemaFileName = "schemaFile".$fileID.".xsd";
		//$cmd_prefix = "scp root@10.10.122.81:/search/odin/daemon/web_vr_exspider_new/ExVrFetcher/data/base/";
		$cmd = "sh templateSync.sh";
		system($cmd, $res);
		if(!file_exists("templates/".$schemaFileName)) {
			echo $schemaFileName." �����ڣ�����ϵ��schema�ļ���ͬѧ����ͬ����svn��</p>";
			$err = 1;
		}

		//����length�ļ�
		$lengthFileName = "lengthFile".$fileID.".txt";
		if(!file_exists("templates/".$lengthFileName)) {
			echo $lengthFileName." �����ڣ�����ϵ��length�ļ���ͬѧ����ͬ����svn��</p>";
			$err = 1;
		}
	}
	
	if($err == 1) {
		echo "<a href=index.php>����</a>";
		unlink("upload/".$dataFileName);
		exit;
	}


	//У��xml
	$root_path = dirname(__FILE__);
	$command = $root_path."/bin/check_xml upload/".$dataFileName." ".$fileID." ".$root_path."/templates/ 2>&1";
	exec($command, $res); //У�����������Ϣ�ᱻ��¼��$res�ַ���������

	//����У����
	$res_size = count($res);
	if($res_size > 0) {
		if($res[$res_size-1] == "SUCCESS: xml data is valid.") { //ͨ��У�飬���һ�е���־��ϢΪ�SUCCESS: xml data is valid.��
			echo $_FILES["xmlfile"]["name"]."ͨ��У�飬ģ���Ϊ".$fileID."</br>";
		} else {
			echo $_FILES["xmlfile"]["name"]." У��ʧ�ܣ�ģ���Ϊ".$fileID."��������Ϣ���£�</p>";
			echo "<textarea rows=\"30\" cols=\"150\">";
			$limit = $res_size;
			//��������Ϣ����̫��ʱ��ie�Ῠ����������������ʾ1000
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
		echo "У����������쳣��</br>";
	}

	
	//ɾ������xml�����ļ�
	unlink("upload/".$dataFileName);
	//����ģ���
	if($_POST['fileID'] != '') {
		$conn = db_init();
		//�жϸ�ģ����Ƿ��Ѵ���
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

	echo "<a href=\"index.php\">����</a>";
?>
