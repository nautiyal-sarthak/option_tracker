from flask import Blueprint, render_template, request, jsonify, redirect, url_for,current_app
from flask_login import login_required, current_user
from ..utils.data import process_trade_data
from flask import session
import json

bp = Blueprint('dashboard', __name__)

@bp.route('/dashboard')
@login_required
def dashboard():
    try:
        current_app.logger.info('loading the dashboard')
        data = process_trade_data(current_user.email, current_user.token, current_user.broker, 'all')
        session['filter_type'] = 'all'

        return render_template('index.html', **data)
    except Exception as e:
        error_message = str(e)
        if "authentication" in error_message.lower():
            return redirect(url_for('auth.provide_token'))
        return jsonify({"error": error_message})

@bp.route('/get_data', methods=['GET'])
@login_required
def get_data():
    try:
        current_app.logger.info('fetching user data with filter' + request.args.get('filter', 'all'))
        filter_type = request.args.get('filter', 'all')
        session['filter_type'] = filter_type
        data = process_trade_data(current_user.email, filter_type=filter_type)
        try:
            parsed_data = json.dumps(data)
            print("Valid JSON")
        except Exception as e:
            print("Invalid JSON")

        return data
    except Exception as e:
        error_message = str(e)
        if "authentication" in error_message.lower():
            return redirect(url_for('auth.provide_token'))
        return jsonify({"error": error_message})