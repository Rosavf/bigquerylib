<?php

require_once '../app/core/manager.php';

$bigQuery = BigQuery::getInstance('informe-211921');
$sql = "SELECT BUKRS,BUDAT FROM `informe-211921.BALANZA.BSEG_2019_1` LIMIT 100";
$bseg = $bigQuery->select($sql);

echo(json_encode($bseg));

?>