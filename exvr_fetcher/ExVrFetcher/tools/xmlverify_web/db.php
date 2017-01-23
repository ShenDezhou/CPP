<?php
function db_init() {
	$conn = mysql_connect("zstat1.dev.ctc.wd.ss.nop.vm.sogou-op.org", "root", "Sogou-RD@2008");
	if(!$conn) {
		die('数据库连接失败：'.mysql_error());
	}
	mysql_select_db('vr', $conn);
	return $conn;
}
?>
