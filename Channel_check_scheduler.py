import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta


host='==hostname=='
database='portal'
user='portal'
password='==password=='


def select(query):
    try:
        conn = mysql.connector.connect(host, database, user, password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute(query)
            records = cursor.fetchall()           
    except Error as e:
        print(e)
    finally:
        return records
        conn.close()
        cursor.close()

def insert(query,value):
    try:
        conn = mysql.connector.connect(host, database, user, password)
        if conn.is_connected():
            cursor = conn.cursor()
            if (type(value) is list):
                 cursor.executemany(query,value)
            else:
                 cursor.execute(query,value)
            conn.commit()          
    except Error as e:
        print(e)
    finally:
        conn.close()
        cursor.close()




def check_region(region):
    get_channels_Query="SELECT c.id FROM channels c INNER JOIN departments d  ON (c.dep_id=d.id) WHERE d.deleted=0 AND d.reg_num=" + str(region)
    channels_rec=select(get_channels_Query)
    insert_q="insert into tasks_queue_new (channel_id,priority,date,vol_ratio,tries_count, login, referer) VALUES(%s,%s,%s,%s,%s,%s,%s);"
    now_t=str(datetime.now().timestamp())
    now=str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    upd_q="UPDATE channels_scheduler SET last_check=%s where reg_num=%s"
    upd_data=(now,str(region))
    insert_data=[]
    for row in channels_rec:
        insert_data.append((str(row[0]),0,now_t,16000,1,'scheduler',str(region)+' - ' + now))
    insert(insert_q,insert_data)
    insert(upd_q,upd_data)

if __name__ == '__main__':
    start_hour=16
    start_day=1
    max_prior=3
    last_prior_Query = "SELECT priority,last_check FROM channels_scheduler ORDER BY last_check DESC, priority LIMIT 1"
    rec=select(last_prior_Query)
    last_prior=rec[0][0]
    last_date=rec[0][1]
    now=datetime.now()
    now_day=now.weekday()
    if (last_prior == max_prior): 
        if (now_day==start_day and now.hour>=start_hour):
            last_prior=0
        else:
            exit(0)
    print("last_prior = ", last_prior)
    current_prior=last_prior+1
    regions_Query = "SELECT reg_num FROM channels_scheduler where priority="+ str(current_prior)
    regions_c=select(regions_Query)
    for row in regions_c:
        print("id = ", row[0])
        check_region(row[0])
