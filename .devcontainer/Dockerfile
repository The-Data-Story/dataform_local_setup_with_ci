# Use an official Node.js runtime as the base image
FROM node:18-slim

# Set working directory
WORKDIR /usr/src/app

# Install Python 3.11 and necessary dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    build-essential \
    python3.11 python3.11-distutils python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Ensure Python 3.11 is the default
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 \
    && update-alternatives --config python3

# Copy Python requirements file to the working directory
COPY requirements.txt .

# Install Python dependencies with the necessary flag to bypass external management restriction
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

# Install both Dataform Core and CLI globally
RUN npm install -g @dataform/core@3.0.0 @dataform/cli@3.0.0

# Copy the dataform_exec script to the container
COPY src/local_run_commands/dataform_exec /usr/local/bin/dataform_exec

# Copy the switch_env script to the container
COPY src/local_run_commands/switch_env /usr/local/bin/switch_env

# Ensure the script is executable
RUN chmod +x /usr/local/bin/dataform_exec
RUN chmod +x /usr/local/bin/switch_env

# Add /usr/local/bin to PATH
ENV PATH="/usr/local/bin:$PATH"

# Optional: Verify installation by showing CLI help
RUN dataform help || true

# Set an entry point to allow passing commands
ENTRYPOINT ["dataform"]