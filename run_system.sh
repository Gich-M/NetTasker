#!/usr/bin/env bash

# run_system.sh
#
# This script sets up and runs the entire system, including:
# - Checking for required tools
# - Setting up the Python environment
# - Compiling the C worker
# - Configuring and starting Nginx
# - Starting the Node Manager
# - Processing tasks
# - Starting the CLI Monitor
#
# Usage: ./run_system.sh

set -e

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Checks for required tools
check_required_tools() {
    local tools=("nmap" "whois" "host" "traceroute" "nc" "python3" "gcc" "nginx")
    for tool in "${tools[@]}"; do
        if ! command_exists "$tool"; then
            echo "$tool is required but not installed." >&2
            echo "Please run './install_dependencies.sh' to install all required dependencies." >&2
            exit 1
        fi
    done
}

# Sets up a Python environment
setup_python_env() {
    local current_dir
    current_dir=$(pwd)
    export PYTHONPATH="$current_dir:$PYTHONPATH"

    if [ ! -d "venv" ]; then
        echo "Creating virtual environment..."
        python3 -m venv env
    fi

    # shellcheck source=/dev/null
    source env/bin/activate
}

# Installs Python requirements
install_python_requirements() {
    echo "Installing Python requirements..."
    if ! pip install -r requirements.txt > /dev/null 2>&1; then
        echo "Failed to install Python requirements" >&2
        exit 1
    fi
}

# Compiles the C worker
compile_c_worker() {
    echo "Compiling C worker..."
    if ! gcc -fstack-protector -fsanitize=address -o src/workers/worker src/workers/main.c src/workers/network.c src/workers/python_interface.c \
        -I"$(python3 -c "import sysconfig; print(sysconfig.get_path('include'))")" \
        -L"$(python3 -c "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))")" \
        -lpython3.11 -lpthread -ljson-c -lcurl; then
        echo "Failed to compile C worker" >&2
        exit 1
    fi
}

# Configure and start Nginx
configure_and_start_nginx() {
    NGINX_CONF="/tmp/nginx_load_balancer.conf"
    cat << EOF > $NGINX_CONF
events {
    worker_connections 1024;
}
http {
    upstream nodemanager {
        server 127.0.0.1:8081;
    }

    server {
        listen 8080;
        
        location / {
            proxy_pass http://nodemanager;
            proxy_set_header Host \$host;
            proxy_set_header X-Real-IP \$remote_addr;
            proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto \$scheme;
        }

        location /nginx_health {
            proxy_pass http://nodemanager/nginx_health;
        }

        location /health {
            proxy_pass http://nodemanager/health;
        }
    }
}
EOF

    echo "Starting Nginx load balancer..."
    if ! sudo nginx -c $NGINX_CONF; then
        echo "Failed to start Nginx" >&2
        exit 1
    fi
}

# Starts the Node Manager
start_node_manager() {
    echo "Starting Node Manager..."
    python3 src/node_manager/node_manager.py > node_manager.log 2>&1 &
    NODE_MANAGER_PID=$!
    sleep 2
    if ! ps -p $NODE_MANAGER_PID > /dev/null; then
        echo "Node Manager failed to start. Check node_manager.log for details."
        exit 1
    fi
}

# Waits for the Node Manager to initialize
wait_for_node_manager() {
    echo "Waiting for Node Manager to initialize..."
    sleep 5
    retries=0
    max_retries=30
    while ! curl -s http://127.0.0.1:8081/health > /dev/null && [ "$retries" -lt "$max_retries" ]; do
        sleep 1
        ((retries++))
    done

    if [ "$retries" -eq "$max_retries" ]; then
        echo "Node Manager failed to start within the expected time. Exiting." >&2
        exit 1
    fi
}

# Starts the CLI Monitor
start_cli_monitor() {
    echo "Starting CLI Monitor..."
    python3 src/cli_monitor.py &
    CLI_MONITOR_PID=$!
}

# Processes tasks from the shared file
process_tasks() {
    echo "Processing tasks from data/200k.txt..."
    while IFS= read -r line
    do
        echo "Sending task: $line"
        curl -X POST -H "Content-Type: application/json" -d "{\"task\": \"$line\"}" http://127.0.0.1:8080/receive_task
        sleep 0.1
    done < data/200k.txt

    echo "All tasks sent. Press Ctrl+C to exit."
    wait
}

# Cleans up resources on exit
cleanup() {
    echo "Shutting down..."
    sudo nginx -s stop
    kill "$NODE_MANAGER_PID" "${WORKER_PIDS[@]}" "$CLI_MONITOR_PID" 2>/dev/null
    wait "$NODE_MANAGER_PID" "${WORKER_PIDS[@]}" "$CLI_MONITOR_PID" 2>/dev/null
    deactivate
    rm $NGINX_CONF
}

# Orchestrates the script execution
main() {
    trap cleanup EXIT

    check_required_tools
    setup_python_env
    install_python_requirements
    compile_c_worker
    configure_and_start_nginx
    start_node_manager
    wait_for_node_manager
    process_tasks
    start_cli_monitor
}

# Execute the main function
main