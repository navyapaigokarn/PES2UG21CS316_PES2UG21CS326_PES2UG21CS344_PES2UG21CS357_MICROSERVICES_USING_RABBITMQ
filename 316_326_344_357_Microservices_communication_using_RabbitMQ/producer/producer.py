from flask import Flask, request, render_template, url_for, redirect
import pika
import time
import pymysql

db=pymysql.connect(
	host='host.docker.internal',
	user='root', 
	password='coleadonis23#J', 
	db='ims'
)
cursor=db.cursor()

app=Flask(__name__, static_folder="static", template_folder="templates")

def connect_to_rabbitmq():
    connection=None
    while connection is None:
        try: 
            connection=pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            channel=connection.channel()
            channel.queue_declare(queue='health_check_queue')
            channel.queue_declare(queue='item_creation_queue')
            channel.queue_declare(queue='order_processing_queue')
            channel.queue_declare(queue='stock_management_queue')
            channel.queue_declare(queue='get_items_queue')
            print("Connected to rabbitmq")
        except pika.exceptions.AMQPConnectionError:
            print("Failed to connect, retrying in 5 sec")
            time.sleep(5)
    return connection, channel

connection, channel=connect_to_rabbitmq()

def get_items():
    items=[]
    method_frame, header_frame, body=channel.basic_get(queue="get_items_queue")
    if method_frame:
        items=eval(body.decode('utf-8'))
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    return items

@app.route('/') 
def home():
    return render_template('producer.html')

@app.route('/getitems')
def getitems():
    items=get_items()
    return render_template('viewitems.html', items=items)

@app.route('/health_check', methods=['GET'])
def health_check():
    channel.basic_publish(exchange='', routing_key='health_check_queue',  body='Health check request')
    return 'Health check request sent'

@app.route('/items', methods=['POST'])
def create_item():
    data={
        'name': request.form["name"], 
        'description': request.form["description"], 
        'price': float(request.form["price"]), 
        'quantity': int(request.form["quantity"])
    }
    channel.basic_publish(exchange='', routing_key='item_creation_queue', body=str(data))
    return redirect(url_for('getitems'))

@app.route('/stock', methods=['POST'])
def stock_update():
    data = {
        'item_id': int(request.form["item_id"]), 
        'quantity': int(request.form["quantity"])
    }
    channel.basic_publish(exchange='', routing_key='stock_management_queue', body=str(data))
    return redirect(url_for('getitems'))

@app.route('/orders', methods=['POST'])
def order_process():
    data = {
        'item_id': int(request.form["item_id"]),
        'quantity': int(request.form["quantity"]), 
        'customer_name': request.form["customer_name"], 
        'shipping_address': request.form["shipping_address"]
    }
    channel.basic_publish(exchange='', routing_key='order_processing_queue', body=str(data))
    return 'Order request sent'
    
if __name__=='__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)