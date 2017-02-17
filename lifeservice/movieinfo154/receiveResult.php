<?php
        $data = json_decode(file_get_contents('php://input'), true);
        $result=$data["result"];
        $timestamp=$data["timestamp"];
        $user=$data["user"];
        $taskid=$data["taskid"];
	$tableid=$data["tableid"];
        $path="result" . "/" . $user . "/" . $taskid . "/";
        if(!is_dir($path)){
                mkdir($path,0777,true);
        }
        #$file=fopen($path .$tableid . "." .  $timestamp,"a+");
        $file=fopen($path . $timestamp,"a+");
        for($i=0;$i<count($result);$i++)
        {
                fputs($file, $result[$i]. "\n");
        }
?>
