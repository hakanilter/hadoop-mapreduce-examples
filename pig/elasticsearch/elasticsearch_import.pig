REGISTER elasticsearch-hadoop.jar;

/* disable speculative execution */
set mapred.map.tasks.speculative.execution false
set mapred.reduce.tasks.speculative.execution false

DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.nodes=localhost:9200');

/* load nyse data */
A = LOAD '$INPUT' USING PigStorage() AS (exchange:chararray, stock_symbol:chararray, stock_date:chararray, stock_price_open:double,
        stock_price_high:double, stock_price_low:double, stock_price_close:double, stock_volume:double, stock_price_adj_close:double);

/* write to elasticsearch */
STORE A INTO '$OUTPUT' USING EsStorage();
