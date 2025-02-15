from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()  # Create a database instance

def init_db(app):
    """Initialize the database with the Flask app."""
    db.init_app(app)
    with app.app_context():
        db.create_all()  # Create tables if they don't exist
