<?php

use GuzzleHttp\HTTP\Client;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Table;
use Google\Cloud\Core\ExponentialBackoff;

class BigQuery{

    //clase de bigquery cliente
    private $bigQueryClient;
    private static $instance;

    public static function getInstance($projectId){

        if (!self::$instance instanceof self){

            self::$instance = new self($projectId);
   
        }
        
        return self::$instance;

    }

    //
    private function __construct($projectId){

        try {
            
            $this->bigQueryClient = new BigQueryClient(['projectId'=>$projectId]);

        }

        catch(Exception $e){

            die($e);

        }
    }

    //funcion general de consulta
    //$dml query en lenguaje SQL standard
    //$type tipo de dato buscado en la consulta
    public function getQuery($dml,$response){

        $query = $this->bigQueryClient->query($dml);
        $queryResults = $this->bigQueryClient->runQuery($query);
        
        if ($queryResults->isComplete()) {

            //
            switch ($response) {

                case 'rows':
                $rows = $queryResults->rows();
                return $rows;
                break;

                case 'info':
                $info = $queryResults->info();
                return $info;
                break;
                
                default: 
                $rows = $queryResults->rows();
                return $rows;
                break;

            }

        }

        //
        else {
            
            return null;

        }

    }

    //cargamos el archivo de la tabla a migrar
    public function loadLocalFile($id,$file,$schema,$settings,$disposition){

        //tabla y dataset
        $dataset = $this->bigQueryClient->dataset($id['dataset']);
        $table = $dataset->table($id['table']);

        //creamos el trabajo de carga
        $loadJobConfig = $table->load(fopen($file['source'], 'r'))->sourceFormat($file['format']);

        //delimitador de campos entre columnas
        $loadJobConfig->fieldDelimiter($settings['delimiter']);
        $loadJobConfig->ignoreUnknownValues($settings['ignoreUnknowValues']);
        $loadJobConfig->quote($settings['quote']);
        $loadJobConfig->allowQuotedNewlines($settings['allowQuotedNewLines']);
        $loadJobConfig->allowJaggedRows($settings['allowJaggedRows']);
        $loadJobConfig->nullMarker($settings['nullMarker']);

        $loadJobConfig->writeDisposition($disposition['write']);
        $loadJobConfig->createDisposition($disposition['create']);


        //esquema siempre en minusculas
        $loadJobConfig->schema($schema);

        //trabajo de carga iniciado
        $job = $table->runJob($loadJobConfig);

        // poll the job until it is complete
        $backoff = new ExponentialBackoff(10);
        $backoff->execute(function () use ($job) {

            $job->reload();

            if (!$job->isComplete()) {

                throw new Exception('Job has not yet completed', 500);

            }

        });

        if (isset($job->info()['status']['errorResult'])) {

            $error = $job->info()['status']['errorResult']['message'];
            printf('Error running job: %s'.$error);

            return false;

        }

        else {

            return true;

        }

    }


        //
    public function select($dml){

        $rows=$this->getQuery($dml,'rows');
        
        $table=[];
        foreach ($rows as $row) {
    
            $line=[];
            foreach ($row as $key => $cell) {
    
                if(is_array($cell)){
    
                    $line1=[];
                    $type;
                    $keys=array_keys($cell);
    
                    if((count($keys)==0)||($keys[0]==0)){
    
                        $type="num";
    
                    }
    
                    else{
    
                        $type="assoc";
    
                    }
    
                    switch ($type) {
    
                        case 'assoc': 
    
                        foreach ($cell as $key1=>$cell1) {
    
                            if(is_array($cell1)){
    
                                $line2=[];
                                $keys1=array_keys($cell1);
                                $tipe1;
    
                                if((count($keys1)==0)||($keys1[0]==0)){
    
                                    $type1="num";
                
                                }
                
                                else{
                
                                    $type1="assoc";
                
                                }
    
                                switch ($type1) {
    
                                    case 'num':
    
                                    foreach ($cell1 as $cell2) {
    
                                        if(is_array($cell2)){
    
    
    
                                        }
    
                                        else{
    
                                            $line2[]=$cell2;
    
                                        }
    
                                    }
    
    
                                    break;
    
                                    case 'assoc':
    
                                    foreach ($cell1 as $key2 => $cell2) {
    
                                            
    
                                    }
    
                                    break;
                                        
                                    default:
    
                                    break;
                                }
    
                                $line1[$key1]=$cell1;
    
                            }
    
                            else{
    
                                $line1[$key1]=$cell1;
    
                            }
    
                        }
                            
                        break;
    
                        case 'num': 
                            
                        foreach ($cell as $cell1) {
    
                            if(is_array($cell1)){
    
                                $line2=[];
                                $keys1=array_keys($cell1);
                                $tipe1;
    
                                if((count($keys1)==0)||($keys1[0]==0)){
    
                                    $type1="num";
                
                                }
                
                                else{
                
                                    $type1="assoc";
                
                                }
    
                                switch ($type1) {
    
                                    case 'num':
    
                                    foreach ($cell1 as $cell2) {
    
                                        if(is_array($cell2)){
    
    
    
                                        }
    
                                        else{
    
                                            $line2[]=$cell2;
    
                                        }
    
                                    }
    
    
                                    break;
    
                                    case 'assoc':
    
                                        foreach ($cell1 as $key2 => $cell2) {
    
                                            if(is_array($cell2)){
    
    
    
                                            }
        
                                            else{
        
                                                $line2[$key2]=$cell2;
        
                                            }
    
                                        }
    
                                    break;
                                        
                                    default:
    
                                    break;
                                }
    
                                $line1[]=$cell1;
    
                            }
    
                            else{
    
                                $line1[]=$cell1;
    
                            }
    
                        }
                                                    
                        break;
                            
                        default: break;
                    
                    }
    
                    $line[$key]=$line1;
    
                }
    
                else{
    
                    $line[$key]=$cell;
    
                }
    
            }
    
            $table[]=$line;
    
        }
    
        $rows=null;
    
        return $table;
            
        }

    }

?>