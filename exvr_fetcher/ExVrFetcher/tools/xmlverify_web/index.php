<html>
	<head>
		<meta http-equiv="Content-Type" content="text/html; charset=GBK">
		<script type="text/javascript" src="jquery-1.7.min.js"> </script>
		<title>schema & length��֤����</title>
	</head>
	<body>
		<center><h1>schema & length ��֤����</h1></center>
		<form action="check.php" method="post" enctype="multipart/form-data">
			<label for="xmlfileUrl">xml URL:</label>
			<input type="text" name="xmlurl" id="xmlurl"/>
			<label for="xmlfile">���� xml�ļ�: </label>
			<input type="file" name="xmlfile" id="xmlfile" />
			</p>
			<label for="IDs">ѡ��ģ��ţ�</label>
			<select name="schemaFileID" id="IDS">
				<option value="0">---</option>
				<?php
					include "db.php";
					$conn = db_init();
					$sql = "select template_id from xmlverify order by template_id";
					$result = mysql_query($sql);
					while($row = mysql_fetch_array($result)) {
						echo "<option value=\"".$row['template_id']."\">".$row['template_id']."��ģ��</option>";
					}
					mysql_close($conn);
				?>
			</select>
			<!--
			<label for="schemafile">schema�ļ�: </label>
			<input type="file" name="schemafile" id="schemafile" />
			</p>
			<label for="lengthfile">length�ļ�: </label>
			<input type="file" name="lengthfile" id="lengthfile" />
			</p>
			-->
			<label for="fileID">���� ������ģ��ţ�</label>
			<input type="text" name="fileID" id="fileID"/>
			</p>
			<input type="submit" name="submit" value="xml verify">
			<input type="reset" name="reset" value="reset">
		</form>
		<script type="text/javascript">
			$(document).ready(function() {
				$("form").submit(function(e){
					//xml�ļ��ǿ�У��
					if($("#xmlfile").val()=='' && $("#xmlurl").val()=='') {
						alert("���ύһ��xml�ļ�����url��ַ");
						return false;
					}

					//ģ��ŷǿ�У��
					var inputedID = $("#fileID").val();
					var selectedID = $("#IDS").val();
					if(selectedID != 0 && inputedID != '') {
						alert("���ܼ�ѡ��һ�����е�ģ���������һ��ģ��ţ�");
						return false;
					}
					if(selectedID == 0 && inputedID == '') {
						alert("��ѡ��һ������ģ��Ż�������һ���µ�ģ��ţ�");
						return false;
					}

					if(inputedID != '') {
						//ģ�������У��
						var re = /^[1-9]\d*$/;
						if(!re.test(inputedID)) {
							alert("ģ��ű�������������");
							return false;
						}
						//����������ģ��ţ���ͬʱ�ύschema�ļ���length�ļ�
						/*
						var schemaFile = $("#schemafile").val();
						var lengthFile = $("#lengthfile").val();
						if(schemaFile == '') {
							alert("schema�ļ�����Ϊ�ա�");
							return false;
						}
						if(lengthFile == '') {
							alert("length�ļ�����Ϊ�ա�");
							return false;
						}
						*/
					}
					return true;
				});
			});
		</script>
	</body>
</html>
