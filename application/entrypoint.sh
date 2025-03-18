#!/bin/sh

echo "📥 Starting data import..."
python import_data.py 
echo "📥 Data import complete."

echo "🚀 Starting Flask application..."
exec python app.py
