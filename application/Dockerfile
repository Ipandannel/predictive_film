# Use a lightweight Python image
FROM python:3.9-alpine

WORKDIR /application

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt pandas

# Copy the application
COPY . .

# Expose Flask port
EXPOSE 5000

# Run the import script before starting Flask
CMD ["sh", "-c", "python import_data.py && python app.py"]


