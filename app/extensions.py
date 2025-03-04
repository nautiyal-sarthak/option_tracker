from flask_session import Session
from flask_login import LoginManager
from authlib.integrations.flask_client import OAuth


session = Session()
login_manager = LoginManager()
oauth = OAuth()