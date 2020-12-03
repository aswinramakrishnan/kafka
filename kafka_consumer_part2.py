from kafka import KafkaConsumer
import json
import msg_handler
from datetime import datetime
import time
import pandas as pd
import base64

record_count = 0
poll_count = 0
offset=0
ct_order_confirmed=0
ct_order_cancelled=0
ct_order_placed=0
ct_order_finalized=0

class KafkaConsumr:
    def __init__(self,bootstrap_servers,topic,auto_offset_reset,group_id,enable_auto_commit,table_name,host_name,db_user,db_password,database,db_port,return_cols,table_name_order_status):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.auto_offset_reset=auto_offset_reset
        self.group_id=group_id
        self.enable_auto_commit=enable_auto_commit
        self.table_name = table_name
        self.host_name=host_name
        self.db_user=db_user
        self.db_password=db_password
        self.database=database
        self.db_port=db_port
        self.return_cols=return_cols
        self.table_name_order_status= table_name_order_status
        self.consumer_client = KafkaConsumer(topic, group_id=self.group_id, bootstrap_servers=self.bootstrap_servers, auto_offset_reset=self.auto_offset_reset, enable_auto_commit=self.enable_auto_commit)

    def msg_res(self,input_tuple):
        global ct_order_confirmed,ct_order_cancelled,ct_order_placed,ct_order_finalized
        if input_tuple is not None and len(input_tuple)>0:
            msg_df = pd.DataFrame(input_tuple, columns=self.return_cols)
            max_tsp = msg_df['tsp'].max()
            # input_tuple_orders = msg_df[msg_df['order_status'].isnull()] [['order_id','model','reservation_date','tsp']]
            msg_df_notnull = msg_df[msg_df['order_status'].notnull()][['order_id', 'order_status']]
            out = msg_df_notnull.groupby(['order_status']).count().reset_index().rename(columns={'order_id':'sums'}).set_index('order_status').to_dict()['sums']

            if 'ORDER_CONFIRMED' in out:
                ct_order_confirmed += out['ORDER_CONFIRMED']
                ct_order_finalized += out['ORDER_CONFIRMED']
            if 'ORDER_CANCELLED' in out:
                ct_order_cancelled += out['ORDER_CANCELLED']
                ct_order_finalized -= out['ORDER_CANCELLED']
            if 'ORDER_PLACED' in out:
                ct_order_placed+=out['ORDER_PLACED']

            print('point_of_time:', max_tsp)
            print('ct_order_confirmed:',ct_order_confirmed, ', ct_order_cancelled:', ct_order_cancelled,', ct_order_placed:', ct_order_placed, ', ct_order_finalized:', ct_order_finalized )
            tsla_order_status=(max_tsp, ct_order_finalized, ct_order_confirmed, ct_order_cancelled, ct_order_placed )
            return str(input_tuple)[1:-1].replace('None','Null') , str(tsla_order_status).replace('None','Null')
        return None,None

    def msg_trans(self,tp, messages,record_count,offset):
        try:
            write_db_vals = []
            for message in messages:
                tsp = datetime.fromtimestamp( message.timestamp/1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                msj = json.loads(json.loads(message.value))
                msj['tsp'] = tsp
                aal = tuple(list(msj.values())  )
                write_db_vals.append(aal)
                record_count+=1
                offset = message.offset
            if len(write_db_vals) > 0:
                return write_db_vals,record_count,offset
            return None,-1,offset
        except Exception as er:
            raise ValueError('Error with msg_trans and send to slack/email', e)

    # @retry(tries=3, delay=2, backoff=2)
    def consume_messages(self,offset, max_poll_count, batch_size):
        global record_count, poll_count
        record_count = 0
        parsed_msg = None
        tsla_order_status=None
        raw_list=[]
        try:
            if not self.consumer_client:
                self.consumer_client = KafkaConsumer(topic, group_id=self.group_id, bootstrap_servers=self.bootstrap_servers, auto_offset_reset=self.auto_offset_reset, enable_auto_commit=self.enable_auto_commit)
            while record_count < batch_size :
                future = time.time()+10
                msg = self.consumer_client.poll(timeout_ms=10000, max_records=10 )
                if msg is None:
                    poll_count += 1
                    continue
                else:
                    for tp, messages in msg.items():
                        try:
                            # print('before_msg_trans', record_count, poll_count)
                            parsed_msg,record_count,offset = self.msg_trans(tp, messages,record_count,offset)
                            raw_list+=parsed_msg
                            # print('after_msg_trans',record_count, poll_count, raw_list)
                            poll_count =0
                        except Exception as ex:
                            print('exception', ex)
                            poll_count += 1
                            self.consumer_client.seek(tp,offset=offset)
                            raise ValueError('Error while reading kafka message from  topic and send to slack/email', ex)
                if time.time()>future :
                    break
            # print('raw_list', raw_list)
            parsed_res,tsla_order_status = self.msg_res(raw_list)
            print('record_count:', record_count, ', poll_count:' , poll_count, ' , tsla_order_status:' , tsla_order_status, ' , parsed_res:', parsed_res)
        except Exception as ex:
            raise ValueError('Error consume_messages and send to slack/email', ex)
        else:
            record_count_new = record_count
            record_count = 0
            poll_count = 0
            return parsed_res, record_count_new, offset,tsla_order_status

    def kafka_consumer(self):
        print("Kafka consumer", self.consumer_client)
        global offset
        while True:
            # TRY-
            parsed_msg, record_count, offset,tsla_order_status = self.consume_messages(offset,max_poll_count=1,batch_size=9)
            if parsed_msg is not None:
                print('MsgHandler')
                msc = msg_handler.MsgHandler(host_name, db_user, db_password, database, db_port)
                msc.write_to_mysql(self.table_name, parsed_msg)
                msc.write_to_mysql(self.table_name_order_status, tsla_order_status)
                self.consumer_client.commit()
                print('done')

    # def msg_transformation(self, tp, messages,record_count,offset):
    #     try:
    #         write_db_vals = []
    #         for message in messages:
    #             msj = json.loads(json.loads(message.value))
    #             aal = tuple(list(msj.values()))
    #             write_db_vals.append(aal)
    #             record_count+=1
    #             offset = message.offset
    #         print(write_db_vals)
    #         if len(write_db_vals) > 0:
    #             return str(write_db_vals)[1:-1],record_count,offset
    #         return None,-1,offset
    #     except Exception as er:
    #         raise ValueError('Error with msg transformation and send to slack/email', e)
    #
    # def kafka_consumer_try(self):
    #     print("try kafka", self.consumer_client)
    #     global offset
    #     while True:
    #         # WORKING -
    #         msgs=self.consumer_client.poll(max_records=10,timeout_ms=10000)
    #         print('polling')
    #         global record_count
    #         for tp, messages in msgs.items():
    #             parsed_msg,record_count,offset = self.msg_transformation(tp,messages,record_count,offset)
    #             msc = msg_handler.MsgHandler(host_name, db_user, db_password, database, db_port)
    #             msc.write_to_mysql(self.table_name,parsed_msg)
    #             # self.consumer_client.commit()

if __name__ == '__main__':
    # Could be stored in config.json file
    bootstrap_servers='localhost:9092'
    topic='part_2'
    # Need to be latest if you are going to re-run this.
    auto_offset_reset='earliest'
    group_id='test'
    host_name = 'localhost'
    db_user = 'root'
    encoded = b'cGFzc3dvcmQ='
    decoded = base64.b64decode(encoded)
    db_password = decoded.decode('utf-8')
    database = 'kafka'
    db_port = 3306
    table_name = 'tsla_orders'
    enable_auto_commit=False
    return_cols=['order_id','model','reservation_date','order_status','tsp']
    table_name_order_status='tsla_order_status'
    kpc = KafkaConsumr(bootstrap_servers, topic, auto_offset_reset, group_id, enable_auto_commit, table_name, host_name, db_user, db_password, database, db_port,return_cols,table_name_order_status)
    try:
        kpc.kafka_consumer()
    except Exception as e:
        raise ValueError('Error with consumer and send to slack/email', e)
    print('Done')


