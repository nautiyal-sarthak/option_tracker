from flask import Blueprint, redirect, url_for, session, render_template , g, request, current_app
from flask_login import login_user, logout_user, login_required, current_user
from authlib.integrations.flask_client import OAuth
from supabase import getUserToken, update_refresh_token
from ..models.user import User
from app.extensions import oauth
from app import user_dict
import logging
from logging import Formatter
import secrets



bp = Blueprint('auth', __name__)

@bp.route('/adhoc',methods=['GET', 'POST'])
def adhoc():
    if request.method == 'POST':
        current_app.logger.info('starting the adhoc')
        email = request.form.get('email')
        session['adhoc_email'] = email
        return redirect(url_for('dashboard.dashboard'))
    return render_template('adhoc.html')


@bp.route('/')
def home():
    current_app.logger.info('loading home page')
    from supabase import check_and_create_table
    check_and_create_table()
    return 'Welcome! <a href="/login">Login with Google</a>'

@bp.route('/login')
def login():
    state = secrets.token_urlsafe(16)  # Generate a secure random state
    session['state'] = state  # Store it in the session
    return oauth.google.authorize_redirect(url_for('auth.callback', _external=True), state=state)

@bp.route('/login/callback')
def callback():
    try:
        print('in callback')
        print('Session state: %s', session.get('state'))
        print('Request state: %s', request.args.get('state'))
        g_token = oauth.google.authorize_access_token()
        print('got the token from the callback')
        user_info = oauth.google.get('userinfo').json()
        user_id = user_info['id']
        token, broker = getUserToken(user_info['email'])
        if token is None:
            print('User %s not found in the database. Please contact the administrator.', user_info['email'])
            return 'User ' + user_info['email'] + ' not found in the database. Please contact the administrator.'
        user = User(id=user_id, name=user_info['name'], email=user_info['email'], token=token, broker=broker)
        login_user(user)
        session['master_trade_data'] = None
        session['adhoc_email'] = None
        # Store the user object in the dictionary
        user_dict[user_id] = user
        print('User %s logged in', user_info['email'])
        print('Redirecting to dashboard')
        return redirect(url_for('dashboard.dashboard'))
    except Exception as e:
        print(f"Error during login callback: {e}")
        #destroy the session
        session.clear()
        return 'An error occurred during login. Please try again later.', 500
    finally:
        # Clear the state from the session after the callback
        session.pop('state', None)
        # Clear the token from the session after the callback


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