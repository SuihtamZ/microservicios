import time
import functools
import logging
import pika
import json

# Configuración básica de logging
logging.basicConfig(level=logging.INFO)

def manejar_errores(func):
    """
    Decorador para manejar excepciones y errores de manera uniforme en todos los endpoints.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Error en la función {func.__name__}: {e}")
            return {"error": "Ha ocurrido un error interno. Por favor, intenta de nuevo más tarde."}, 500
    return wrapper

def reintento(max_reintentos=3, delay=2):
    """
    Decorador para reintentar una operación en caso de error, con un máximo de intentos y un retraso entre ellos.
    """
    def decorador(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            intentos = 0
            while intentos < max_reintentos:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    intentos += 1
                    logging.warning(f"Intento {intentos} fallido para la función {func.__name__}: {e}")
                    if intentos >= max_reintentos:
                        logging.error(f"Se han agotado todos los intentos para la función {func.__name__}")
                        return {"error": "No se pudo completar la operación después de varios intentos."}, 500
                    time.sleep(delay)
        return wrapper
    return decorador

def publicar_evento(evento, mensaje):
    """
    Publica un evento en RabbitMQ para sincronización entre microservicios.
    """
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # Declarar una cola de mensajes (si no existe)
        channel.queue_declare(queue=evento)

        # Publicar mensaje en la cola
        channel.basic_publish(exchange='', routing_key=evento, body=json.dumps(mensaje))

        logging.info(f"Evento publicado: {evento} - Mensaje: {mensaje}")
        connection.close()
    except Exception as e:
        logging.error(f"Error al publicar el evento {evento}: {e}")

def consumir_eventos(evento, callback):
    """
    Configura un consumidor para escuchar eventos de RabbitMQ.
    """
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # Escuchar la cola especificada
        channel.queue_declare(queue=evento)

        def on_message(ch, method, properties, body):
            logging.info(f"Evento recibido: {body}")
            callback(json.loads(body))

        channel.basic_consume(queue=evento, on_message_callback=on_message, auto_ack=True)
        logging.info(f"Esperando eventos en la cola: {evento}")
        channel.start_consuming()
    except Exception as e:
        logging.error(f"Error al consumir el evento {evento}: {e}")
