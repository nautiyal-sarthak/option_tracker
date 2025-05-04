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
    state = secrets.token_urlsafe(16)
    session['state'] = state
    current_app.logger.info("Initiating Google OAuth login")
    return oauth.google.authorize_redirect(url_for('auth.callback', _external=True), state=state)

@bp.route('/login/callback')
def callback():
    try:
        session_state = session.get('state')
        request_state = request.args.get('state')

        if session_state != request_state:
            current_app.logger.warning("Invalid OAuth state token")
            return 'Invalid state parameter', 400

        g_token = oauth.google.authorize_access_token()
        user_info = oauth.google.get('userinfo').json()

        email = user_info.get('email')
        user_id = user_info.get('id')
        name = user_info.get('name')

        token, broker = getUserToken(email)
        if token is None:
            msg = f"User {email} not found in the database. Please contact the administrator."
            current_app.logger.warning(msg)
            return msg, 403

        user = User(id=user_id, name=name, email=email, token=token, broker=broker)
        login_user(user)
        session.update({
            'master_trade_data': None,
            'adhoc_email': None
        })
        user_dict[user_id] = user

        current_app.logger.info(f"User {email} logged in successfully")
        return redirect(url_for('dashboard.dashboard'))

    except Exception as e:
        current_app.logger.exception("Error during login callback")
        session.clear()
        return 'An error occurred during login. Please try again later.', 500

    finally:
        session.pop('state', None)

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
