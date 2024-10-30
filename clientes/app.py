import os
import sys
import time
import functools
import logging
import pika
import json
import threading
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import IntegrityError

# Asegurar que el directorio raíz esté en sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importar autenticación y resiliencia desde utils
from utils.auth import configurar_jwt, generar_token, autenticar_usuario
from flask_jwt_extended import jwt_required
from utils.resiliencia import manejar_errores

# Configuración de logging
logging.basicConfig(level=logging.INFO)

# Inicializar la aplicación Flask
app = Flask(__name__)

# Configuración de la base de datos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_DIR, "clientes.db")
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{db_path}'
db = SQLAlchemy(app)

# Inicializar JWT
jwt = configurar_jwt(app)

@app.route('/login', methods=['POST'])
def login():
    datos = request.get_json()
    usuario = datos.get("usuario")
    password = datos.get("password")

    if autenticar_usuario(usuario, password):
        # Genera el token JWT y registra un log
        token = generar_token(usuario)
        logging.info(f"Token generado para {usuario}: {token}")
        return jsonify(access_token=token), 200
    else:
        logging.error(f"Intento fallido de login para el usuario: {usuario}")
        return jsonify({"error": "Credenciales inválidas"}), 401

# Definición del modelo Cliente
class Cliente(db.Model):
    id_cliente = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(100), nullable=False, unique=True)

# Crear la base de datos si no existe
with app.app_context():
    db.create_all()

def reintento(intentos=3, espera=2):
    """Decorador para reintentar la ejecución de una función."""
    def decorador(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for intento in range(intentos):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.warning(f"Intento {intento + 1} fallido: {str(e)}")
                    time.sleep(espera)
            raise Exception("Todos los intentos fallaron")
        return wrapper
    return decorador

@reintento()
def publicar_evento(evento, datos):
    """Publica un evento en RabbitMQ con manejo de errores."""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='clientes_eventos')

        mensaje = json.dumps({"evento": evento, "datos": datos})
        channel.basic_publish(exchange='', routing_key='clientes_eventos', body=mensaje)
        logging.info(f"Evento publicado: {mensaje}")
        connection.close()
    except pika.exceptions.AMQPConnectionError as e:
        logging.error(f"Error al conectar con RabbitMQ: {str(e)}")
        raise

@app.route('/clientes', methods=['POST'], endpoint='crear_cliente')
@jwt_required()
@manejar_errores
@reintento()
def crear_cliente():
    """Crea un nuevo cliente."""
    data = request.get_json()

    # Validación de datos recibidos
    if not data or 'nombre' not in data or 'email' not in data:
        logging.error("Datos faltantes en la solicitud: %s", data)
        return jsonify({'error': 'Faltan datos'}), 400

    try:
        # Intentar crear un cliente
        nuevo_cliente = Cliente(nombre=data['nombre'], email=data['email'])
        db.session.add(nuevo_cliente)
        db.session.commit()

        # Publicar evento en RabbitMQ
        publicar_evento('cliente_creado', {
            'id_cliente': nuevo_cliente.id_cliente,
            'nombre': nuevo_cliente.nombre,
            'email': nuevo_cliente.email
        })

        return jsonify({"mensaje": "Cliente creado exitosamente"}), 201

    except IntegrityError as e:
        db.session.rollback()
        logging.error(f"Error de integridad al crear cliente: {str(e)}")
        return jsonify({'error': 'El email ya está registrado.'}), 400

    except Exception as e:
        db.session.rollback()
        logging.error(f"Error al crear cliente: {str(e)}")
        return jsonify({'error': 'Ha ocurrido un error interno. Por favor, intenta de nuevo más tarde.'}), 500

@app.route('/clientes/<int:id_cliente>', methods=['GET'], endpoint='obtener_cliente')
@jwt_required()
@manejar_errores
@reintento()
def obtener_cliente(id_cliente):
    """Obtiene un cliente por su ID."""
    cliente = Cliente.query.get_or_404(id_cliente)
    return jsonify({
        "id_cliente": cliente.id_cliente, 
        "nombre": cliente.nombre,
        "email": cliente.email
    }), 200

@app.route('/clientes/<int:id_cliente>', methods=['PUT'], endpoint='actualizar_cliente')
@jwt_required()
@manejar_errores
@reintento()
def actualizar_cliente(id_cliente):
    try:
        cliente = Cliente.query.get_or_404(id_cliente)
        data = request.get_json()

        # Validación de datos recibidos
        if 'nombre' not in data and 'email' not in data:
            logging.error("No se proporcionaron datos para actualizar: %s", data)
            return jsonify({'error': 'No se proporcionaron datos para actualizar.'}), 400

        # Actualizar los campos del cliente
        if 'nombre' in data:
            cliente.nombre = data['nombre']
        if 'email' in data:
            cliente.email = data['email']

        db.session.commit()
        logging.info(f"Cliente {cliente.id_cliente} actualizado: {cliente.nombre}, {cliente.email}")
        
        # Publicar evento de actualización en RabbitMQ
        publicar_evento('cliente_actualizado', {
            'id_cliente': cliente.id_cliente,
            'nombre': cliente.nombre,
            'email': cliente.email
        })

        logging.info(f"Evento cliente_actualizado publicado para el cliente {cliente.id_cliente}")
        return jsonify({"mensaje": "Cliente actualizado exitosamente"}), 200

    except IntegrityError as e:
        db.session.rollback()
        logging.error(f"Error de integridad al actualizar cliente: {str(e)}")
        return jsonify({'error': 'El email ya está registrado.'}), 400

    except Exception as e:
        db.session.rollback()
        logging.error(f"Error al actualizar cliente: {str(e)}")
        return jsonify({'error': 'Ha ocurrido un error interno. Por favor, intenta de nuevo más tarde.'}), 500

def consumir_eventos(evento_nombre, callback):
    """Escucha eventos desde RabbitMQ."""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='clientes_eventos')

        def callback_wrapper(ch, method, properties, body):
            mensaje = json.loads(body)
            if mensaje.get("evento") == evento_nombre:
                callback(mensaje.get("datos"))

        channel.basic_consume(queue='clientes_eventos', on_message_callback=callback_wrapper, auto_ack=True)
        logging.info(f"Esperando eventos '{evento_nombre}'...")
        channel.start_consuming()
    except pika.exceptions.AMQPConnectionError as e:
        logging.error(f"Error al conectar con RabbitMQ: {str(e)}")

if __name__ == '__main__':
    # Iniciar el consumidor en un hilo separado para 'cliente_creado'
    consumidor_hilo_creado = threading.Thread(target=consumir_eventos, args=('cliente_creado', lambda x: None))
    consumidor_hilo_creado.daemon = True
    consumidor_hilo_creado.start()

    # Iniciar el consumidor en un hilo separado para 'cliente_actualizado'
    consumidor_hilo_actualizado = threading.Thread(target=consumir_eventos, args=('cliente_actualizado', lambda x: None))
    consumidor_hilo_actualizado.daemon = True
    consumidor_hilo_actualizado.start()

    # Iniciar la aplicación Flask
    app.run(debug=True, port=5003)
