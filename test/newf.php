<?php
include 'BaiduBce.phar';
require 'SampleConf.php';

//ini_set("display_errors", "On");
//error_reporting(E_ALL);

use BaiduBce\BceClientConfigOptions;
use BaiduBce\Util\Time;
use BaiduBce\Util\MimeTypes;
use BaiduBce\Http\HttpHeaders;
use BaiduBce\Services\Bos\BosClient;

//调用配置文件中的参数
global $BOS_TEST_CONFIG;
//新建BosClient
$client = new BosClient($BOS_TEST_CONFIG);

$bucketName = "ssss-dd";
$exist = $client->doesBucketExist($bucketName);
if(!$exist){
    $client->createBucket($bucketName);
}
?>
