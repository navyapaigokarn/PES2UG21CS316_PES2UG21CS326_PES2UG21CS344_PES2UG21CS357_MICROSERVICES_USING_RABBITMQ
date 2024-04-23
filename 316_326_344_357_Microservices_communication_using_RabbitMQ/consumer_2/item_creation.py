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
			channel.queue_declare('item_creation_queue')
			channel.queue_declare('get_items_queue')
			print("Connected to rabbitmq")
			return connection, channel
		except pika.exceptions.AMQPConnectionError:
			print("Failed, retrying in 5sec")
			time.sleep(5)


connection, channel=connect_to_rabbitmq()
def callback(ch, method, properties, body):
	item_data=eval(body)
	print(f"Received item creation request: {item_data}")
	query="insert into items (name, description, price, quantity) values (%s, %s, %s, %s)"
	values=(item_data["name"], item_data["description"], item_data["price"], item_data["quantity"])
	cursor.execute(query, values)
	db.commit()
	itemlist=[]
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
	print(itemlist)
	channel.basic_publish(exchange='', routing_key="get_items_queue", body=str(itemlist))
	channel.basic_ack(delivery_tag=method.delivery_tag)
	print("Item created successfully")

channel.basic_consume(queue="item_creation_queue", on_message_callback=callback, auto_ack=False)
print("Waiting for item creation requests")
channel.start_consuming()

