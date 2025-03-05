from flask import Blueprint, redirect, url_for, session, render_template , g, request, current_app
from flask_login import login_user, logout_user, login_required, current_user
from authlib.integrations.flask_client import OAuth
from supabase import getUserToken, update_refresh_token
from ..models.user import User
from app.extensions import oauth
from app import user_dict
import logging
from logging import Formatter


bp = Blueprint('auth', __name__)

@bp.route('/')
def home():
    current_app.logger.info('loading home page')
    from supabase import check_and_create_table
    check_and_create_table()
    return 'Welcome! <a href="/login">Login with Google</a>'

@bp.route('/login')
def login():
    session.clear()
    return oauth.google.authorize_redirect(url_for('auth.callback', _external=True))

@bp.route('/login/callback')
def callback():
    current_app.logger.info('in callback')
    print("Session state:", session.get("state"))
    print("Request state:", request.args.get("state"))
    token = oauth.google.authorize_access_token()
    user_info = oauth.google.get('userinfo').json()
    user_id = user_info['id']
    token, broker = getUserToken(user_info['email'])
    user = User(id=user_id, name=user_info['name'], email=user_info['email'], token=token, broker=broker)
    login_user(user)
    # Store the user object in the dictionary
    user_dict[user_id] = user
    return redirect(url_for('dashboard.dashboard'))

@bp.route('/logout')
@login_required
def logout():
    current_app.logger.info('logging out')
    logout_user()
    session.clear()
    return redirect(url_for('auth.home'))

@bp.route('/provide_token', methods=['GET', 'POST'])
@login_required
def provide_token():
    if request.method == 'POST':
        current_app.logger.info('Updating the token')
        token = request.form.get('token')
        update_refresh_token(current_user.email, token)
        current_user.token = token
        return redirect(url_for('dashboard.dashboard'))
    return render_template('provide_token.html')