FROM ubuntu:22.04

# Avoid prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    openjdk-11-jdk \
    wget \
    curl \
    postgresql-client \
    kafkacat \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set up Python environment
WORKDIR /app

# Install Python packages
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Set up environment variables
ENV PYTHONPATH=${PYTHONPATH}:/app
ENV PATH=${PATH}:/app

# Copy application code
COPY . .

# Set default command
CMD ["bash"]