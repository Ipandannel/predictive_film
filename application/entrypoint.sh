#!/bin/sh

echo "📥 Starting data import in the background..."
python import_data.py &  # Run import in the background

echo "🚀 Starting Flask application..."
exec python app.py  # Start Flask immediately
