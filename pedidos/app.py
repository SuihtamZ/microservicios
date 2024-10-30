import os
import sys
import pika
import json
import logging
import threading
import time
import requests  # Usaremos requests para comunicar con microservicios
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_jwt_extended import jwt_required

# Asegurar que el directorio raíz esté en sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.auth import configurar_jwt

# Configuración básica de logging
logging.basicConfig(level=logging.INFO)

# Crear instancia de Flask
app = Flask(__name__)

# Configurar la base de datos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_DIR, "pedidos.db")
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{db_path}'

# Inicializa SQLAlchemy y JWT
db = SQLAlchemy(app)
jwt = configurar_jwt(app)

# Modelo Pedido
class Pedido(db.Model):
    id_pedido = db.Column(db.Integer, primary_key=True)
    id_cliente = db.Column(db.Integer, nullable=False)
    nombre_cliente = db.Column(db.String(100), nullable=True)
    correo_cliente = db.Column(db.String(100), nullable=True)
    estado = db.Column(db.String(50), default='pendiente')

# Modelo DetallePedido
class DetallePedido(db.Model):
    id_detalle = db.Column(db.Integer, primary_key=True)
    id_pedido = db.Column(db.Integer, db.ForeignKey('pedido.id_pedido'), nullable=False)
    id_producto = db.Column(db.Integer, nullable=False)
    nombre_producto = db.Column(db.String(100), nullable=True)
    precio_producto = db.Column(db.Float, nullable=True)
    cantidad = db.Column(db.Integer, nullable=False)

# Crear las tablas si no existen
with app.app_context():
    db.create_all()

def obtener_token():
    """Obtiene un token JWT del microservicio de clientes."""
    datos = {"usuario": "usuario", "password": "contraseña123"}
    response = requests.post('http://localhost:5003/login', json=datos)
    if response.status_code == 200:
        token = response.json().get("access_token")
        logging.info(f"Token obtenido correctamente: {token}")
        return token 
    else:
        logging.error("Error al obtener el token JWT")
        return None

def validar_cliente(id_cliente):
    """Valida si el cliente existe en el microservicio de clientes."""
    token = obtener_token()  # Obtener un nuevo token antes de cada solicitud
    if not token:
        logging.error("No se pudo obtener un token JWT")
        return False, "No se pudo obtener un token JWT"

    headers = {'Authorization': f'Bearer {token}'}
    logging.info(f"Usando token: {token} para validar cliente.")

    try:
        response = requests.get(f'http://localhost:5003/clientes/{id_cliente}', headers=headers)

        if response.status_code == 404:
            return False, "ID de cliente no existe"
        elif response.status_code == 401:
            logging.error("Token JWT no autorizado. Verifica la autenticación.")
            return False, "Token JWT no autorizado"
        
        return True, response.json()

    except requests.exceptions.RequestException as e:
        logging.error(f"Error al conectar con el microservicio de clientes: {str(e)}")
        return False, "Error de comunicación con el microservicio de clientes"

def validar_producto(id_producto):
    """Valida si el producto existe en el microservicio de productos."""
    token = obtener_token()  # Obtener un token para la solicitud
    if not token:
        return False, "No se pudo obtener un token JWT"

    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(f'http://localhost:5001/productos/{id_producto}', headers=headers)
        if response.status_code == 404:
            return False, "ID de producto no existe"
        elif response.status_code == 401:
            return False, "Token JWT no autorizado"
        elif response.status_code != 200:
            return False, f"Error al obtener el producto: {response.status_code}"
        return True, response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al conectar con el microservicio de productos: {str(e)}")
        return False, "Error de comunicación con el microservicio de productos"

@app.route('/pedido', methods=['POST'])
def crear_pedido():
    """Crea un pedido tras validar los datos."""
    datos = request.get_json()

    # Validar cliente
    cliente_valido, cliente_data = validar_cliente(datos['id_cliente'])
    if not cliente_valido:
        return jsonify({'error': cliente_data}), 404

    # Validar producto
    producto_valido, producto_data = validar_producto(datos['id_producto'])
    if not producto_valido:
        return jsonify({'error': producto_data}), 404

    # Crear el pedido y detalle
    nuevo_pedido = Pedido(
        id_cliente=datos['id_cliente'],
        nombre_cliente=cliente_data['nombre'],
        correo_cliente=cliente_data['email']
    )
    db.session.add(nuevo_pedido)
    db.session.commit()

    detalle = DetallePedido(
        id_pedido=nuevo_pedido.id_pedido,
        id_producto=datos['id_producto'],
        nombre_producto=producto_data['nombre'],
        precio_producto=producto_data['precio'],
        cantidad=datos['cantidad']
    )
    db.session.add(detalle)
    db.session.commit()

    return jsonify({'mensaje': 'Pedido creado exitosamente'}), 201

def procesar_cliente_actualizado(data):
    """Callback para procesar los pedidos relacionados con un cliente actualizado."""
    logging.info(f"Procesando cliente actualizado: {data}")
    try:
        with app.app_context():
            pedidos = Pedido.query.filter_by(id_cliente=data['id_cliente']).all()
            if not pedidos:
                logging.info(f"No se encontraron pedidos para el cliente con ID {data['id_cliente']}")
            else:
                for pedido in pedidos:
                    pedido.nombre_cliente = data.get('nombre')
                    pedido.correo_cliente = data.get('email')
                db.session.commit()
                logging.info(f"Pedidos actualizados para el cliente con ID {data['id_cliente']}")
    except Exception as e:
        logging.error(f"Error al procesar cliente actualizado: {str(e)}")

def procesar_producto_actualizado(data):
    """Callback para procesar los pedidos relacionados con un producto actualizado."""
    logging.info(f"Procesando producto actualizado: {data}")
    try:
        with app.app_context():
            detalles = DetallePedido.query.filter_by(id_producto=data['id_producto']).all()
            if not detalles:
                logging.info(f"No se encontraron detalles de pedidos para el producto con ID {data['id_producto']}")
            else:
                for detalle in detalles:
                    detalle.nombre_producto = data.get('nombre')
                    detalle.precio_producto = data.get('precio')
                db.session.commit()
                logging.info(f"Pedidos actualizados para el producto con ID {data['id_producto']}")
    except Exception as e:
        logging.error(f"Error al procesar producto actualizado: {str(e)}")

def consumir_eventos(queue_name, evento_nombre, callback):
    """Configura y escucha eventos desde RabbitMQ."""
    connection = None
    channel = None
    while True:
        try:
            if connection is None or connection.is_closed:
                logging.info("Conectando a RabbitMQ...")
                connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
                channel = connection.channel()
                channel.queue_declare(queue=queue_name)

            def callback_wrapper(ch, method, properties, body):
                mensaje = json.loads(body)
                logging.info(f"Mensaje recibido de RabbitMQ: {mensaje}")
                if mensaje.get("evento") == evento_nombre:
                    logging.info(f"Procesando evento: {evento_nombre}")
                    callback(mensaje.get("datos"))

            channel.basic_consume(queue=queue_name, on_message_callback=callback_wrapper, auto_ack=True)
            logging.info(f"Esperando eventos '{evento_nombre}' en la cola '{queue_name}'...")
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logging.error(f"Error al conectar con RabbitMQ: {str(e)}. Reintentando en 5 segundos...")
            time.sleep(5)
        except Exception as e:
            logging.error(f"Error inesperado: {str(e)}. Cerrando la conexión y reintentando en 5 segundos...")
            if connection is not None and not connection.is_closed:
                connection.close()
            time.sleep(5)

if __name__ == '__main__':
    hilo_cliente = threading.Thread(
        target=consumir_eventos, args=('clientes_eventos', 'cliente_actualizado', procesar_cliente_actualizado)
    )
    hilo_cliente.daemon = True
    hilo_cliente.start()
    
    hilo_producto = threading.Thread(
        target=consumir_eventos, args=('productos_eventos', 'producto_actualizado', procesar_producto_actualizado)
    )
    hilo_producto.daemon = True
    hilo_producto.start()

    logging.info("Iniciando la aplicación Flask en el puerto 5002...")
    app.run(debug=True, port=5002)
