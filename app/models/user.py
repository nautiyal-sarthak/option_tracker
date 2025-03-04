from flask_login import UserMixin

# Define the User model
class User(UserMixin):
    def __init__(self, id, name, email, token, broker):
        self.id = id
        self.name = name
        self.email = email
        self.token = token
        self.broker = broker

