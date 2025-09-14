from flask import Flask, request, render_template, redirect, url_for, jsonify
from src.engine import ApproximateQueryEngine
import time
import os
from werkzeug.utils import secure_filename

app = Flask(__name__)

# Configure upload settings
UPLOAD_FOLDER = 'uploaded_databases'
ALLOWED_EXTENSIONS = {'csv'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

DATA_PATHS = [
    "databases/mydata.csv",
    "uploaded_databases/uploaded_file.csv"
]

engine = ApproximateQueryEngine(DATA_PATHS)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'success': False, 'message': 'No file part'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'message': 'No selected file'}), 400
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # Update engine with new file
        global engine
        DATA_PATHS.append(filepath)
        engine = ApproximateQueryEngine(DATA_PATHS)
        
        return jsonify({
            'success': True,
            'message': 'File uploaded successfully',
            'filepath': filepath
        }), 200
    return jsonify({'success': False, 'message': 'Invalid file type'}), 400

@app.route('/run_query', methods=['POST'])
def run_query():
    query_type = request.form.get('query_type')
    column = request.form.get('column')

    start = time.time()
    if column not in engine.df.columns:
        result = {
            'result': f"Column '{column}' not found in database.",
            'time_taken': 0
        }
    else:
        if query_type == 'COUNT':
            res = engine.df[column].count()
        elif query_type == 'SUM':
            res = engine.df[column].sum()
        elif query_type == 'AVG':
            res = engine.df[column].mean()
        else:
            res = "Unsupported query type."
        result = {
            'result': res,
            'time_taken': round(time.time() - start, 4)
        }
    return render_template('result.html', result=result)

if __name__ == '__main__':
    # Create upload folder if it doesn't exist
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    app.run(debug=True)
