# Use a lightweight Python image
FROM python:3.9-alpine

WORKDIR /application

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt pandas

# Copy the application files
COPY . .

# Copy and make entrypoint.sh executable
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose Flask port
EXPOSE 5000

# Use the entrypoint script
CMD ["/entrypoint.sh"]
