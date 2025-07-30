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
        data = process_trade_data(current_user.email, current_user.token, current_user.broker)
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
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        grouping = request.args.get('grouping', 'month')  # Default to 'month'
        
        
        # Store filter_type in session
        session['start_date'] = start_date
        session['end_date'] = end_date
        
        # Pass both filter_type and grouping to process_trade_data
        data = process_trade_data(current_user.email, start_date=start_date,end_date=end_date, grouping=grouping)
        
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