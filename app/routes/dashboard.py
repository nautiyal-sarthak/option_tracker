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
        # Log the filter and grouping parameters
        filter_type = request.args.get('filter', 'all')
        grouping = request.args.get('grouping', 'month')  # Default to 'month'
        current_app.logger.info(f"Fetching user data with filter: {filter_type}, grouping: {grouping}")
        
        # Store filter_type in session
        session['filter_type'] = filter_type
        
        # Pass both filter_type and grouping to process_trade_data
        data = process_trade_data(current_user.email, filter_type=filter_type, grouping=grouping)
        
        # Validate JSON serialization
        try:
            parsed_data = json.dumps(data)
            current_app.logger.debug("Valid JSON")
        except Exception as e:
            current_app.logger.error(f"Invalid JSON: {str(e)}")
        
        return jsonify(data)  # Ensure response is JSON
    except Exception as e:
        error_message = str(e)
        current_app.logger.error(f"Error in get_data: {error_message}")
        if "authentication" in error_message.lower():
            return redirect(url_for('auth.provide_token'))
        return jsonify({"error": error_message}), 500