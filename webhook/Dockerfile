# Use an official lightweight Python image.
FROM python:3.9-slim

# Set the working directory in the container.
WORKDIR /app

# Copy the requirements file and install dependencies.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code.
COPY . .

# Set the command to run the application.
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:app"]