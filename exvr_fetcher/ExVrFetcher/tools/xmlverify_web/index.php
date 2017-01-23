<html>
	<head>
		<meta http-equiv="Content-Type" content="text/html; charset=GBK">
		<script type="text/javascript" src="jquery-1.7.min.js"> </script>
		<title>schema & length验证工具</title>
	</head>
	<body>
		<center><h1>schema & length 验证工具</h1></center>
		<form action="check.php" method="post" enctype="multipart/form-data">
			<label for="xmlfileUrl">xml URL:</label>
			<input type="text" name="xmlurl" id="xmlurl"/>
			<label for="xmlfile">或者 xml文件: </label>
			<input type="file" name="xmlfile" id="xmlfile" />
			</p>
			<label for="IDs">选择模板号：</label>
			<select name="schemaFileID" id="IDS">
				<option value="0">---</option>
				<?php
					include "db.php";
					$conn = db_init();
					$sql = "select template_id from xmlverify order by template_id";
					$result = mysql_query($sql);
					while($row = mysql_fetch_array($result)) {
						echo "<option value=\"".$row['template_id']."\">".$row['template_id']."号模板</option>";
					}
					mysql_close($conn);
				?>
			</select>
			<!--
			<label for="schemafile">schema文件: </label>
			<input type="file" name="schemafile" id="schemafile" />
			</p>
			<label for="lengthfile">length文件: </label>
			<input type="file" name="lengthfile" id="lengthfile" />
			</p>
			-->
			<label for="fileID">或者 输入新模板号：</label>
			<input type="text" name="fileID" id="fileID"/>
			</p>
			<input type="submit" name="submit" value="xml verify">
			<input type="reset" name="reset" value="reset">
		</form>
		<script type="text/javascript">
			$(document).ready(function() {
				$("form").submit(function(e){
					//xml文件非空校验
					if($("#xmlfile").val()=='' && $("#xmlurl").val()=='') {
						alert("请提交一个xml文件或者url地址");
						return false;
					}

					//模板号非空校验
					var inputedID = $("#fileID").val();
					var selectedID = $("#IDS").val();
					if(selectedID != 0 && inputedID != '') {
						alert("不能既选择一个已有的模板号又输入一个模板号！");
						return false;
					}
					if(selectedID == 0 && inputedID == '') {
						alert("请选择一个已有模板号或者输入一个新的模板号！");
						return false;
					}

					if(inputedID != '') {
						//模板号整数校验
						var re = /^[1-9]\d*$/;
						if(!re.test(inputedID)) {
							alert("模板号必须是正整数。");
							return false;
						}
						//当输入了新模板号，需同时提交schema文件和length文件
						/*
						var schemaFile = $("#schemafile").val();
						var lengthFile = $("#lengthfile").val();
						if(schemaFile == '') {
							alert("schema文件不能为空。");
							return false;
						}
						if(lengthFile == '') {
							alert("length文件不能为空。");
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
