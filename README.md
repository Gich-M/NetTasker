# NetTasker

A scalable and efficient distributed task processing system implemented in Python and C, designed for handling various network-related tasks.

## Table of Contents

1. [Overview](#overview)
2. [Features](#features)
3. [System Architecture](#system-architecture)
4. [Prerequisites](#prerequisites)
5. [Installation](#installation)
6. [Usage](#usage)
7. [Configuration](#configuration)
8. [Development](#development)
9. [Testing](#testing)
10. [Contributing](#contributing)
11. [License](#license)

## Overview

The NetTasker is a robust system that efficiently processes network-related tasks in a distributed manner. It combines Python and C for optimal performance and flexibility.

## Features

- Distributed task processing
- Support for multiple task types (IP data, port scan, OS fingerprint, etc.)
- Dynamic worker scaling
- Health monitoring and automatic recovery
- RESTful API for task submission and result retrieval
- Nginx integration for load balancing and reverse proxy
- CLI monitor for easy interaction
- Comprehensive logging and alerting system

## System Architecture

The system consists of the following main components:

1. Node Manager: Coordinates workers and distributes tasks
2. Workers: Process tasks and return results
3. HTTP Server: Handles API requests and communication
4. Task Distributor: Manages task distribution to workers
5. Health Checker: Monitors worker health
6. Performance Monitor: Tracks system performance
7. Scaler: Dynamically adjusts the number of workers based on load
8. CLI Monitor: Provides a command-line interface for task submission and result retrieval

## Prerequisites

- Python 3.11+
- GCC
- Nginx
- nmap
- whois
- host
- traceroute
- netcat

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/Gich-M/NetTasker.git
   cd NetTasker
   ```

2. Install dependencies:
   ```
   ./install_dependencies.sh
   ```

3. Set up the Python environment and install requirements:
   ```
   python3 -m venv env
   source env/bin/activate
   pip install -r requirements.txt
   ```


## Usage

To run the entire system, use the provided script:

