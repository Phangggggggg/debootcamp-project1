# Use an official Python runtime as a parent image
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    gdal-bin \
    libgdal-dev \
    libproj-dev

# Copy the current directory contents into the container at /app
COPY . .

# Upgrade pip to the latest version
RUN pip install --upgrade pip

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Run pipeline.py when the container launches
CMD ["python", "-m", "etl_project.pipeline"]
