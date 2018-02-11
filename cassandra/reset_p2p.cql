truncate p2p.tx;
truncate ops_batch;
truncate ops_tx_cnt;
copy ops_batch (id, batch_start_dt, batch_end_dt, process_start_dt, process_end_dt) from 'ops_batch_init.csv';
copy ops_tx_cnt (id, cnt) from 'ops_tx_cnt_init.csv' ;
select * from ops_tx_cnt;
select * from ops_batch;