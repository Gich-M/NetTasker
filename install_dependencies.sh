#!/usr/bin/env bash

# This script installs necessary dependencies for the project.
# It checks if each required tool is already installed and installs it if missing.
# Note: This script requires sudo privileges to install packages.

# Exit immediately if a command exits with a non-zero status.

set -e

# Function to install a package if it's not already installed
# Usage: install_if_missing <package_name>

install_if_missing() {
    if ! command -v "$1" &> /dev/null; then
        echo "Installing $1..."
        sudo apt-get install -y "$1"
    else
        echo "$1 is already installed."
    fi
}

# Install required tools

echo "Checking and installing required tools..."
install_if_missing nmap        # Network exploration tool and security scanner
install_if_missing whois       # Client for the whois directory service
install_if_missing host        # DNS lookup utility
install_if_missing traceroute  # Print the route packets trace to network host
install_if_missing netcat      # Networking utility for reading/writing network connections
install_if_missing python3     # Python programming language interpreter
install_if_missing gcc         # GNU Compiler Collection
install_if_missing nginx       # High-performance HTTP server and reverse proxy

echo "All dependencies installed successfully."
