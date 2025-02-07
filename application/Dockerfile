# Use a lightweight Python image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /application

# Copy the requirements file and install Python dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the port that the Flask app will run on
EXPOSE 5000

# Run the Flask app
CMD ["python", "app.py"]
