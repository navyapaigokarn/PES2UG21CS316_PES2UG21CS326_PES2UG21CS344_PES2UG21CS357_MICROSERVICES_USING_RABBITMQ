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
			channel.queue_declare('stock_management_queue')
			print("Connected to rabbitmq")
			return connection, channel
		except pika.exceptions.AMQPConnectionError:
			print("Failed, retrying in 5sec")
			time.sleep(5)


connection, channel=connect_to_rabbitmq()

def callback(ch, method, properties, body):
	itemlist=[]
	stock_data=eval(body)
	print(f"Received stock update data: {stock_data}")
	query="update items set quantity = %s where id = %s"
	values=(stock_data["quantity"], stock_data["item_id"])
	cursor.execute(query, values)
	db.commit()
	query="select * from items"
	cursor.execute(query)
	items=cursor.fetchall()
	for item in items:
		itemlist.append({
			'id': item[0],
			'name': item[1], 
			'description': item[2],
			'price': float(item[3]), 
			'quantity': item[4]
		})
	channel.basic_publish(exchange='', routing_key="get_items_queue", body=str(itemlist))
	channel.basic_ack(delivery_tag=method.delivery_tag)
	print("Stock updated successfully")

channel.basic_consume(queue='stock_management_queue', on_message_callback=callback, auto_ack=False)
print("Stock management queue ready, waiting for requests")
channel.start_consuming()