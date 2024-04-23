from datetime import datetime
import pika
import pymysql
import time
 
db=pymysql.connect(
	host='host.docker.internal',
	user='root',
	password='Navya@7972',
	db='ims'
	)

cursor=db.cursor()

def connect_to_rabbitmq():
	connection=None
	while connection is None:
		try:
			connection=pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
			channel=connection.channel()
			channel.queue_declare(queue='health_check_queue')
			print("Connected to rabbitmq")
			return connection, channel
		except pika.exceptions.AMQPConnectionError:
			print("Failed, retrying in 5sec")
			time.sleep(5)

connection, channel= connect_to_rabbitmq()

def callback(ch, method, properties, body):
	print(f'Recieved health check request {body}')
	service_name="Inventory  management service"
	status="OK"
	timestamp=datetime.now()

	query="insert into health_checks (service_name, status, timestamp) values (%s, %s, %s)"
	values=(service_name, status, timestamp)
	cursor.execute(query, values)
	db.commit()

	print("Health check successful")

channel.basic_consume(queue='health_check_queue', on_message_callback=callback, auto_ack=True)
print("Waiting for health check requests")
channel.start_consuming()
