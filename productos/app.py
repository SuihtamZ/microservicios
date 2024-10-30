import os
import sys
import pika
import json

# Asegura que la carpeta raíz esté en el sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_jwt_extended import jwt_required
from utils.auth import configurar_jwt

# Crear instancia de Flask
app = Flask(__name__)

# Configura la ruta absoluta para la base de datos dentro de la carpeta 'productos'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_DIR, "productos.db")
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{db_path}'

# Inicializa SQLAlchemy y JWT
db = SQLAlchemy(app)
jwt = configurar_jwt(app)

class Producto(db.Model):
    id_producto = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(100), nullable=False)
    precio = db.Column(db.Float, nullable=False)

# Inicializa la base de datos si no existe
with app.app_context():
    db.create_all()

def publicar_evento_producto(evento, datos):
    """
    Publica un evento en RabbitMQ.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='productos_eventos')
    
    mensaje = json.dumps({"evento": evento, "datos": datos})
    channel.basic_publish(exchange='', routing_key='productos_eventos', body=mensaje)
    connection.close()

@app.route('/productos', methods=['POST'])
@jwt_required()
def crear_producto():
    data = request.json
    nuevo_producto = Producto(nombre=data['nombre'], precio=data['precio'])
    db.session.add(nuevo_producto)
    db.session.commit()

    # Publicar evento de creación de producto
    publicar_evento_producto('producto_creado', {
        'id_producto': nuevo_producto.id_producto,
        'nombre': nuevo_producto.nombre,
        'precio': nuevo_producto.precio
    })

    return jsonify({"mensaje": "Producto creado exitosamente"}), 201

@app.route('/productos/<int:id>', methods=['PUT'])
@jwt_required()
def actualizar_producto(id):
    producto = Producto.query.get_or_404(id)
    data = request.json
    producto.nombre = data.get('nombre', producto.nombre)
    producto.precio = data.get('precio', producto.precio)
    db.session.commit()

    # Publicar evento de actualización de producto
    publicar_evento_producto('producto_actualizado', {
        'id_producto': producto.id_producto,
        'nombre': producto.nombre,
        'precio': producto.precio
    })

    return jsonify({"mensaje": "Producto actualizado exitosamente"}), 200

@app.route('/productos/<int:id>', methods=['GET'])
@jwt_required()
def obtener_producto(id):
    producto = Producto.query.get_or_404(id)
    return jsonify({
        'id_producto': producto.id_producto,
        'nombre': producto.nombre,
        'precio': producto.precio
    })

if __name__ == '__main__':
    app.run(debug=True, port=5001)
