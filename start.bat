@echo off
REM Quick start script for the Python Flask AQE application (Windows)

echo 🧠 ML-Powered Approximate Query Engine - Python Flask Setup
echo ============================================================
echo.

REM Check Python version
echo 📋 Checking Python version...
python --version
echo.

REM Install dependencies
echo 📦 Installing dependencies...
pip install -r requirements.txt
echo.

REM Generate sample data
echo 🌱 Generating sample data (200,000 records)...
python seed.py 200000
echo.

REM Start the server
echo 🚀 Starting Flask server...
echo    Access the application at: http://localhost:8080
echo    Press Ctrl+C to stop
echo.
python app.py
