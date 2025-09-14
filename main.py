from flask import Flask, request, jsonify, render_template, redirect, url_for, session
from src.engine import ApproximateQueryEngine
import os

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', os.urandom(24))
UPLOAD_FOLDER = 'uploaded_databases'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/', methods=['GET', 'POST'])
def index():
    result = None
    if request.method == 'POST':
        if 'file' in request.files:
            file = request.files['file']
            file_path = os.path.join(UPLOAD_FOLDER, file.filename)
            file.save(file_path)
            result = {'message': 'File uploaded successfully', 'file_path': file_path}
            return render_template('index.html', result=result)
        else:
            db_path = request.form['db_path']
            query_type = request.form['query_type']
            column = request.form['column']
            mode = request.form.get('mode', 'approximate')
            accuracy = float(request.form.get('accuracy', 0.1))

            engine = ApproximateQueryEngine(db_path)
            result = engine.run_query(query_type, column, mode, accuracy)
            session['result'] = result  # Store result in session
            return redirect(url_for('show_result'))

    return render_template('index.html', result=result)

@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files['file']
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(file_path)
    return jsonify({'message': 'File uploaded successfully', 'file_path': file_path})

@app.route('/result')
def show_result():
    result = session.get('result')
    return render_template('result.html', result=result)

@app.route('/query', methods=['POST'])
def run_query():
    data = request.form
    db_path = data['db_path']
    query_type = data['query_type']
    column = data['column']
    mode = data.get('mode', 'approximate')
    accuracy = float(data.get('accuracy', 0.1))

    engine = ApproximateQueryEngine(db_path)
    result = engine.run_query(query_type, column, mode, accuracy)
    session['result'] = result
    return redirect(url_for('show_result'))


if __name__ == '__main__':
    app.run(debug=True)
