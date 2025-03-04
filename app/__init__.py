from flask import Flask
from .config import Config
from .extensions import session, login_manager, oauth


# Initialize the user dictionary
user_dict = {}

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)



    # Initialize extensions
    session.init_app(app)
    oauth.init_app(app)
    login_manager.login_view = "auth.login"
    login_manager.init_app(app)


    # Configure Google OAuth
    oauth.register(
        name='google',
        client_id=app.config['GOOGLE_CLIENT_ID'],
        client_secret=app.config['GOOGLE_CLIENT_SECRET'],
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        api_base_url='https://www.googleapis.com/oauth2/v1/',
        client_kwargs={'scope': 'openid email profile'}
    )

    # Import and register blueprints
    from .routes import auth, dashboard, stock
    app.register_blueprint(auth.bp)
    app.register_blueprint(dashboard.bp)
    app.register_blueprint(stock.bp)

    return app

@login_manager.user_loader
def load_user(user_id):
    # Replace with your method of retrieving a user by ID
    return user_dict.get(user_id)