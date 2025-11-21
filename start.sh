#!/bin/bash
# Quick start script for the Python Flask AQE application

echo "🧠 ML-Powered Approximate Query Engine - Python Flask Setup"
echo "============================================================"
echo ""

# Check Python version
echo "📋 Checking Python version..."
python --version

# Install dependencies
echo ""
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Generate sample data
echo ""
echo "🌱 Generating sample data (200,000 records)..."
python seed.py 200000

# Start the server
echo ""
echo "🚀 Starting Flask server..."
echo "   Access the application at: http://localhost:8080"
echo "   Press Ctrl+C to stop"
echo ""
python app.py
