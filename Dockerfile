# python 3.10 slim image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy the requirements first to leverage Docker cache
COPY requirements.txt /app/requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files
COPY . /app