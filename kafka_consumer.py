from kafka import KafkaConsumer
import json
import msg_handler
import time
import csv
import pandas as pd
import base64

record_count = 0
poll_count = 0
offset=0

class KafkaConsumr:
    def __init__(self,bootstrap_servers,topic,auto_offset_reset,group_id,enable_auto_commit,table_name,host_name,db_user,db_password,database,db_port):
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
        self.consumer_client = KafkaConsumer(topic, group_id=self.group_id, bootstrap_servers=self.bootstrap_servers, auto_offset_reset=self.auto_offset_reset, enable_auto_commit=self.enable_auto_commit)

    def msg_res(self,write_db_vals):
        if write_db_vals is not None and len(write_db_vals)>0:
            return str(write_db_vals)[1:-1].replace('None','Null')
        return None

    def msg_trans(self,tp, messages,record_count,offset):
        try:
            write_db_vals = []
            for message in messages:
                msj = json.loads(json.loads(message.value))
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
            parsed_res = self.msg_res(raw_list)
            print('record_count: ', record_count, ', poll_count: ' , poll_count, ' , parsed_res: ', parsed_res)
        except Exception as ex:
            raise ValueError('Error consume_messages and send to slack/email', ex)
        else:
            record_count_new = record_count
            record_count = 0
            poll_count = 0
            return parsed_res, record_count_new, offset

    def kafka_consumer(self):
        print("Kafka consumer", self.consumer_client)
        global offset
        while True:
            # TRY-
            parsed_msg, record_count, offset = self.consume_messages(offset,max_poll_count=1,batch_size=9)
            if parsed_msg is not None:
                print('MsgHandler')
                msc = msg_handler.MsgHandler(host_name, db_user, db_password, database, db_port)
                msc.write_to_mysql(self.table_name, parsed_msg)
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
    topic='part_1'
    # Need to be latest if you are going to re-run this.
    auto_offset_reset='earliest'
    group_id='test'
    host_name = 'localhost'
    db_user = 'root'
    encoded=b'cGFzc3dvcmQ='
    decoded = base64.b64decode(encoded)
    db_password = decoded.decode('utf-8')
    database = 'kafka'
    db_port = 3306
    table_name = 'tsla'
    enable_auto_commit=False
    kpc = KafkaConsumr(bootstrap_servers, topic, auto_offset_reset, group_id, enable_auto_commit, table_name, host_name, db_user, db_password, database, db_port)
    try:
        kpc.kafka_consumer()
    except Exception as e:
        raise ValueError('Error with consumer and send to slack/email', e)
    print('Done')


