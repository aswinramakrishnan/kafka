import mysql.connector
from retry import retry
import base64

max_attempts_db = 3
max_attempts_kafka = 3

class MsgHandler:
    def __init__(self, host_name,
                 db_user, db_password, database,
                 db_port):
        self._host_name = host_name
        self._db_user = db_user
        self._db_password = db_password
        self._database = database
        self._db_port = db_port
        self._mysql_connection= None

    def connect_db(self):
        # print(self._host_name,self._db_user, self._db_password, self._database, self._db_port)
        return mysql.connector.connect(host=self._host_name, user=self._db_user, password=self._db_password, database=self._database, port=self._db_port)

    @retry(tries=max_attempts_db, delay=2, backoff=2)
    def write_to_mysql(self, table,ins_vals):
        try:
            self._mysql_connection = self.connect_db()
            cursor = self._mysql_connection.cursor()
            print("load to table :"+ str(table),ins_vals)
            cursor.execute("Insert into {table_name} values {ins_vals};".format(table_name=table, ins_vals=ins_vals))
            # print(cursor.fetchall())
            self._mysql_connection.commit()
        except Exception as ex:
            if self._mysql_connection:
                self._mysql_connection.rollback()
            raise ex
        finally:
            if self._mysql_connection:
                self._mysql_connection.close()

    @retry(tries=max_attempts_kafka, delay=2, backoff=2)
    def commit_kafka_messages_offset(self, kafka_client):
        try:
            kafka_client.commit_topic()
        except Exception as ex:
            raise ex

if __name__ == '__main__':
    schema = None
    host_name = 'localhost'
    db_user = 'root'
    encoded = b'cGFzc3dvcmQ='
    decoded = base64.b64decode(encoded)
    db_password = decoded.decode('utf-8')
    database = 'kafka'
    db_port = 3306
    bucket_name = None
    a = MsgHandler( host_name,db_user, db_password, database,db_port)
    # a.write_to_mysql('temp', "(14,'test'),(15,'test')" )
    # a.write_to_mysql('tsla',
    #                  "('O8', 'Model F', '2020-07-23 11:31:29', 'Sean', 'Bailey', '73416 Williams Walks', 'Jonesview', 17306, 'SN'), ('O9', 'Model F', '2020-07-23 11:31:29', 'Sean', 'Bailey', '73416 Williams Walks', 'Jonesview', 17306, 'SND')")
    #                  "('O1', 'Model D', '2020-09-09 18:58:27', 'Kimberly', 'Pena', '064 Clark Point', 'West Jeremy', 31248, 'AZ')", "('O2', 'Model E', '2020-06-19 22:46:09', 'Amy', 'Young', '25367 Carpenter Prairie Apt. 119', 'Danielstad', 51314, 'UZ')", "('O1', 'Model D', '2020-09-09 18:58:27', 'Kimberly', 'Pena', '064 Clark Point', 'West Jeremy', 31248, 'AZ')", "('O2', 'Model E', '2020-06-19 22:46:09', 'Amy', 'Young', '25367 Carpenter Prairie Apt. 119', 'Danielstad', 51314, 'UZ')", "('O1', 'Model D', '2020-09-09 18:58:27', 'Kimberly', 'Pena', '064 Clark Point', 'West Jeremy', 31248, 'AZ')", "('O2', 'Model E', '2020-06-19 22:46:09', 'Amy', 'Young', '25367 Carpenter Prairie Apt. 119', 'Danielstad', 51314, 'UZ')", "('O1', 'Model D', '2020-09-09 18:58:27', 'Kimberly', 'Pena', '064 Clark Point', 'West Jeremy', 31248, 'AZ')", "('O2', 'Model E', '2020-06-19 22:46:09', 'Amy', 'Young', '25367 Carpenter Prairie Apt. 119', 'Danielstad', 51314, 'UZ')", "('O1', 'Model D', '2020-09-09 18:58:27', 'Kimberly', 'Pena', '064 Clark Point', 'West Jeremy', 31248, 'AZ')", "('O2', 'Model E', '2020-06-19 22:46:09', 'Amy', 'Young', '25367 Carpenter Prairie Apt. 119', 'Danielstad', 51314, 'UZ')"
    #
    print('done')



