from flask_jwt_extended import JWTManager, create_access_token, jwt_required
from werkzeug.security import check_password_hash, generate_password_hash
from datetime import timedelta

# Usuarios de prueba
USUARIOS = {
    "usuario": generate_password_hash("contraseña123")  # Almacena una contraseña en hash para el usuario
}
def configurar_jwt(app):
    app.config['JWT_SECRET_KEY'] = 'super-secret-key'
    jwt = JWTManager(app)
    return jwt

def generar_token(usuario_id):
    return create_access_token(identity=usuario_id, expires_delta=timedelta(minutes=30))

def autenticar_usuario(usuario, password):
    """Verifica las credenciales del usuario."""
    if usuario in USUARIOS and check_password_hash(USUARIOS[usuario], password):
        return True
    return False
