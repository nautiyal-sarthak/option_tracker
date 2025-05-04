from flask import Blueprint, redirect, url_for, session, render_template, g, request, current_app
from flask_login import login_user, logout_user, login_required, current_user
from authlib.integrations.flask_client import OAuth
from supabase import getUserToken, update_refresh_token, check_and_create_table
from ..models.user import User
from app.extensions import oauth
from app import user_dict
import secrets

bp = Blueprint('auth', __name__)

@bp.route('/adhoc', methods=['GET', 'POST'])
def adhoc():
    if request.method == 'POST':
        email = request.form.get('email')
        if not email:
            current_app.logger.warning("Adhoc form submitted without email")
            return render_template('adhoc.html', error="Email is required.")
        session['adhoc_email'] = email
        current_app.logger.info(f"Starting adhoc login for {email}")
        return redirect(url_for('dashboard.dashboard'))
    return render_template('adhoc.html')

@bp.route('/')
def home():
    current_app.logger.info('Loading home page')
    check_and_create_table()
    return 'Welcome! <a href="/login">Login with Google</a>'

@bp.route('/login')
def login():
    session.clear()
    state = secrets.token_urlsafe(16)
    session['state'] = state
    current_app.logger.info(f"Generated state: {state}")
    return oauth.google.authorize_redirect(url_for('auth.callback', _external=True), state=state)

@bp.route('/login/callback')
def callback():
    current_app.logger.info('in callback')
    current_app.logger.info('Session state: %s', session.get('state'))
    current_app.logger.info('Request state: %s', request.args.get('state'))
    g_token = oauth.google.authorize_access_token()
    current_app.logger.info('got the token from the callback')
    user_info = oauth.google.get('userinfo').json()
    user_id = user_info['id']
    token, broker = getUserToken(user_info['email'])
    if token is None:
        current_app.logger.error('User %s not found in the database. Please contact the administrator.', user_info['email'])
        return 'User ' + user_info['email'] + ' not found in the database. Please contact the administrator.'
    user = User(id=user_id, name=user_info['name'], email=user_info['email'], token=token, broker=broker)
    login_user(user)
    session['master_trade_data'] = None
    session['adhoc_email'] = None
    # Store the user object in the dictionary
    user_dict[user_id] = user
    current_app.logger.info('User %s logged in', user_info['email'])
    current_app.logger.info('Redirecting to dashboard')
    return redirect(url_for('dashboard.dashboard'))

@bp.route('/logout')
@login_required
def logout():
    current_app.logger.info(f"User {current_user.email} logging out")
    logout_user()
    session.clear()
    return redirect(url_for('auth.home'))

@bp.route('/provide_token', methods=['GET', 'POST'])
@login_required
def provide_token():
    if request.method == 'POST':
        token = request.form.get('token')
        if not token:
            current_app.logger.warning("Token submission missing")
            return render_template('provide_token.html', error="Token is required.")
        
        update_refresh_token(current_user.email, token)
        current_user.token = token
        current_app.logger.info(f"Token updated for user {current_user.email}")
        return redirect(url_for('dashboard.dashboard'))

    return render_template('provide_token.html')
