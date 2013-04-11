<?php

/**
 * Main connection class.
 * 
 * @author tiagojsag
 */

namespace Simbiotica\YQLBundle\Connection;

class Connection
{
    /**
     * Internal variables
     */
    public $json_decode = true;
    
    /**
     * Endpoint urls
     */
    protected $apiUrl;
    protected $params;
    
    function __construct()
    {
        $this->apiUrl = "http://query.yahooapis.com/v1/public/yql";
        $this->params = array("env" => "http://datatables.org/alltables.env");
    }
    
    protected function request($uri, $method = 'GET', $args = array())
    {
        if (!array_key_exists('params', $args))
            $args['params'] = $this->params;
        else
            $args['params'] = array_merge($args['params'], $this->params);
        
        $url = $this->apiUrl . $uri;
    
        if (!isset($args['headers']['Accept'])) {
            $args['headers']['Accept'] = 'application/json';
        }
    
        $options = array(
                CURLOPT_RETURNTRANSFER => true,
        );
    
        $ch = curl_init($url);
        curl_setopt_array($ch, $options);
    
        $response = array();
        $response['return'] = ($this->json_decode) ? (array) json_decode(
                curl_exec($ch)) : curl_exec($ch);
        $response['info'] = curl_getinfo($ch);
    
        if ($response['info']['http_code'] == 401) {
            $this->authorized = $this->getAccessToken();
            return $this->request($uri, $method, $args);
        }
    
        $payload = new Payload($url, $options);
        $payload->setRawResponse($response);
    
        curl_close($ch);
    
        return $payload;
    }
    
    public function runSql($sql)
    {
        $params = array('q' => $sql);
        $payload = $this->request('sql', 'POST', array('params' => $params));

        $info = $payload->getInfo();
        $rawResponse = $payload->getRawResponse();
        if ($info['http_code'] != 200) {
            var_dump($payload->getRequest());
            if (!empty($rawResponse['return']['error']))
                throw new \RuntimeException(sprintf(
                    'There was a problem with your request "%s": %s',
                    $payload->getRequest(),
                    implode('<br>', $rawResponse['return']['error'])));
            else
                throw new \RuntimeException(sprintf(
                    'There was a problem with your request "%s"',
                    $payload->getRequest()));
        }
        
        return $payload;
    }

    /**
     * Gets the name of all available tables
     */
    public function getTableNames()
    {
        $sql = "SHOW TABLES";
                
        return $this->runSql($sql);
    }


    /**
     * Gets all the records of a defined table.
     * @param $table The name of table
     * @param $params array of parameters.
     *   Valid parameters:
     *   - 'rows_per_page' : Number of rows per page.
     *   - 'page' : Page index.
     *   - 'order' : array of $column => asc/desc.
     */
    public function getAllRows($table, $params = array())
    { 
        return $this->getAllRowsForColumns($table, null, $params);
    }

    /**
     * Gets given columns from all the records of a defined table.
     * @param $table the name of table
     * @param $params array of parameters.
     *   Valid parameters:
     *   - 'rows_per_page' : Number of rows per page.
     *   - 'page' : Page index.
     *   - 'order' : array of $column => asc/desc.
     */
    public function getAllRowsForColumns($table, $columns = null, $params = array())
    {
        return $this->getRowsForColumns($table, $columns, $filter = null, $params);
    }
    
    /**
     * Gets given columns from the records of a defined table that match the given condition.
     * @param $table the name of table
     * @param $params array of parameters.
     *   Valid parameters:
     *   - 'rows_per_page' : Number of rows per page.
     *   - 'page' : Page index.
     *   - 'order' : array of $column => asc/desc.
     */
    public function getRowsForColumns($table, $columns = null, $filter = null, $params = array())
    {
        if ($columns == null || !is_array($columns) || empty($columns))
            $columnsString = "*";
        else
            $columnsString = implode(', ', $columns);
        
        if ($filter == null || !is_array($filter) || empty($filter))
            $filterString = "1=1";
        else
        {
            $filterString = implode(' AND ', array_map(function($key, $elem)
            {
                if (is_int($elem))
                    return sprintf('%s = %d', $key, $elem);
                if (is_bool($elem))
                    return sprintf('%s = %s', $key, $elem?'1':'0');
                if (is_string($elem))
                    return sprintf('%s = \'%s\'', $key, $elem);
            }, array_keys($filter), $filter));
        }
        
        $extrasString = '';
        if (isset($params['rows_per_page']))
        {
            $extrasString .= sprintf(" LIMIT %s", $params['rows_per_page']);
            if (isset($params['page']))
                $extrasString .= sprintf(" OFFSET %s", $params['page']);
        }
        if (isset($params['order']))
        {
            $extrasString .= 'ORDER BY '.implode(',', array_map(function ($field, $order){
                return sprintf('%s %s', $field, $order);
            }, array_flip($params['order']), $params['order']));
        }
        
        $sql = sprintf("SELECT %s FROM %s WHERE %s %s", $columnsString, $table, $filterString, $extrasString);
        
        return $this->runSql($sql);
    }
}

?>