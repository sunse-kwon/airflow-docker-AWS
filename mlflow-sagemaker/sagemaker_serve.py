import os
import json
import pickle
import numpy as np
import pandas as pd
import io
import flask
from flask import Flask, Response


app = flask.Flask(__name__)


# inference.py functions
def get_model(model_dir):
    '''Load model from mlflow artifact store'''
    model_path = os.path.join(model_dir,'model.pkl')
    with open(model_path, "rb") as f:
        model = pickle.load(f)
    return model

def prepare_input(request_body, request_content_type):
    '''Process input data'''
    if request_content_type == 'application/json':
        data = pd.read_json(io.StringIO(request_body), orient='split')
        data['delay_hours_log'] = np.log1p(data['delay_hours'])

        data_seoul = data[data['city']=='Seoul'].copy()
        data_seoul['timestamp'] = pd.to_datetime(data_seoul['timestamp'], format='%Y-%m-%d %H:%M:%S')  # Ensure datetime format
        data_seoul.set_index('timestamp', inplace=True)

        features=['pty','reh','rn1','t1h','wsd','day','hour','sin_hour','cos_hour','is_weekend',
            'day_of_week_encoded','pty_lag1','pty_lag2','delay_hours_lag1','delay_hours_lag2']

        X  = data_seoul[features]
        y = data_seoul['delay_hours_log']

        return X, y
    raise ValueError('failed to prepare inputs')
    
    
def get_prediction(input_data, model):
    '''Make predictions using model'''
    prediction = model.predict(input_data)
    return np.expm1(prediction)

def return_output(y_true, y_pred, content_type):
    '''Format the output'''
    if content_type == 'application/json':
        return json.dumps({'y_pred':y_pred.tolist(), 'y_true':y_true.tolist(), 'time':str(y_true.index[0])})
    raise ValueError('failed to send output')


# Load the model at startup
MODEL_PATH = "/opt/ml/model"
model = None

try:
    model = get_model(MODEL_PATH)
except Exception as e:
    print(f"Failed to load model: {str(e)}")

# Ping endpoint
@app.route("/ping", methods=["GET"])
def ping():
    """SageMaker health check endpoint."""
    if model is None:
        return Response(status=500)
    return Response(status=200)

# Invocation endpoint
@app.route("/invocations", methods=["POST"])
def invocations():
    """SageMaker inference endpoint."""
    if model is None:
        return Response(response="Model not loaded", status=500)

    content_type = flask.request.content_type
    try:
        # Read the request body
        request_body = flask.request.get_data(as_text=True)

        # Prepare input
        X, y_true = prepare_input(request_body, content_type)

        # Make predictions
        y_pred = get_prediction(X, model)

        # Format output
        output = return_output(y_true, y_pred, content_type)

        return Response(
            response=output,
            status=200,
            mimetype="application/json"
        )
    except ValueError as ve:
        return Response(response=str(ve), status=400)
    except Exception as e:
        return Response(response=f"Prediction error: {str(e)}", status=500)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)

