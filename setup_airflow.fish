#!/usr/bin/fish

# ==============================================================================
#
#           `./setup_airflow.fish` -- Airflow on k3s Setup Script
#
#   This script automates the setup and management of a local Airflow
#   environment running on a k3s cluster. It provides functions to install
#   dependencies, configure the cluster, deploy Airflow, and manage the
#   environment with a set of convenient flags.
#
#   ** Author: Wes H.
#   ** Version: 1.2.0 (Fix: Helm upgrade syntax and other improvements)
#
# ==============================================================================

# --- Configuration ---
set -g AIRFLOW_NAMESPACE "airflow"
set -g HELM_RELEASE_NAME "airflow"
set -g LOCAL_REGISTRY "localhost:5001"
set -g DOCKER_IMAGE_NAME "my-local-airflow"
set -g DOCKER_IMAGE_TAG "latest"
set -g TAR_FILE_NAME "my-local-airflow.tar"

# --- Helper Functions ---

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#   _log <level> <message>
#
#   Logs a message to the console with a specified log level.
#
#   - $level: The log level (e.g., INFO, WARN, ERROR)
#   - $message: The message to log
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
function _log
    set -l level $argv[1]
    set -l message $argv[2]
    echo "[$level] $message"
end

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#   _check_dependency <command> <package>
#
#   Checks if a command-line tool is installed and installs it via yay if
#   it's not.
#
#   - $command: The command to check for
#   - $package: The package name to install if the command is not found
# - - - - - - - - - - - - - - - - - - - - - - - - -- - - - - - - - - - - - - - -
function _check_dependency
    set -l cmd $argv[1]
    set -l pkg $argv[2]

    if not command -v $cmd &>/dev/null
        _log "INFO" "$cmd not found. Installing $pkg..."
        yay -S --noconfirm $pkg
    else
        _log "INFO" "$cmd is already installed."
    end
end

# --- Setup Functions ---

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#   install_k3s
#
#   Installs k3s if it's not already installed and ensures it's running.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
function install_k3s
    _log "INFO" "Checking for k3s installation..."
    _check_dependency "k3s" "k3s"

    _log "INFO" "Ensuring k3s service is running..."
    sudo systemctl enable --now k3s

    _log "INFO" "Waiting for k3s to be ready..."
    while not sudo k3s kubectl get nodes &>/dev/null
        sleep 1
    end
    _log "INFO" "k3s is up and running."
end

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#   setup_local_registry
#
#   Sets up a local Docker registry to store the custom Airflow image. This
#   is necessary to make the image available to k3s without pushing it to a
#   public registry.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
function setup_local_registry
    _log "INFO" "Setting up local Docker registry..."

    if not test -f /etc/rancher/k3s/registries.yaml
        _log "INFO" "Configuring k3s to use the local registry..."
        echo "mirrors:
    \"$LOCAL_REGISTRY\":
        endpoint:
            - \"http://$LOCAL_REGISTRY\"" | sudo tee /etc/rancher/k3s/registries.yaml >/dev/null
        _log "INFO" "Restarting k3s to apply registry configuration..."
        sudo systemctl restart k3s
    end

    if not docker ps -a --format '{{.Names}}' | grep -q "^local-registry\$"
        _log "INFO" "Starting local registry container..."
        docker run -d --name local-registry -p 5001:5000 --restart always registry:2
    else
        _log "INFO" "Local registry is already running."
    end
end

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#   build_and_push_image
#
#   Builds the custom Airflow Docker image, creates a tarball, and pushes
#   it to the k3s container runtime.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
function build_and_push_image
    _log "INFO" "Building Docker image..."
    docker build -t "$DOCKER_IMAGE_NAME:$DOCKER_IMAGE_TAG" .

    _log "INFO" "Creating tar file from Docker image..."
    docker save "$DOCKER_IMAGE_NAME:$DOCKER_IMAGE_TAG" -o "$TAR_FILE_NAME"

    _log "INFO" "Importing image into k3s..."
    sudo k3s ctr images import "$TAR_FILE_NAME"
end

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#   deploy_airflow
#
#   Deploys Airflow to the k3s cluster using the official Helm chart and a
#   custom `airflow-values.yaml` file.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
function deploy_airflow
    _log "INFO" "Adding the Apache Airflow Helm repository..."
    helm repo add apache-airflow https://airflow.apache.org
    helm repo update

    _log "INFO" "Creating the Airflow namespace..."
    sudo k3s kubectl create namespace $AIRFLOW_NAMESPACE --dry-run=client -o yaml | sudo k3s kubectl apply -f -

    _log "INFO" "Deploying Airflow with Helm..."
    helm upgrade --install $HELM_RELEASE_NAME apache-airflow/airflow \
        --namespace $AIRFLOW_NAMESPACE \
        --values airflow-values.yaml
end

# --- Management Functions ---

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#   refresh_values
#
#   Refreshes the Airflow Helm release with the latest `airflow-values.yaml`.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
function refresh_values
    _log "INFO" "Updating Helm repositories..."
    helm repo update
    _log "INFO" "Refreshing airflow-values.yaml..."
    helm upgrade --install $HELM_RELEASE_NAME apache-airflow/airflow \
        --namespace $AIRFLOW_NAMESPACE \
        --values airflow-values.yaml
end

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#   get_dashboard_secret
#
#   Retrieves the Kubernetes Dashboard secret token and proxies the dashboard
#   in a detached process.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
function get_dashboard_secret
    _log "INFO" "Getting Kubernetes Dashboard secret..."
    kubectl -n kubernetes-dashboard create token admin-user

    _log "INFO" "Proxying Kubernetes Dashboard in the background..."
    nohup sudo k3s kubectl proxy &
    _log "INFO" "Dashboard is available at http://localhost:8001/api/v1/namespaces/kube-system/services/https:kubernetes-dashboard:/proxy/"
end

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#   proxy_airflow_api
#
#   Forwards the Airflow API to localhost for easy access.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
function proxy_airflow_api
    _log "INFO" "Proxying Airflow API to localhost:8080..."
    nohup kubectl port-forward svc/airflow-api-server 8080:8080 --namespace airflow &
end

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#   delete_and_rebuild_airflow
#
#   Deletes the Airflow Helm release and redeploys it.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
function delete_and_rebuild_airflow
    _log "INFO" "Deleting Airflow deployment..."
    helm uninstall $HELM_RELEASE_NAME --namespace $AIRFLOW_NAMESPACE
    sudo k3s kubectl delete namespace $AIRFLOW_NAMESPACE
    _log "INFO" "Rebuilding Airflow..."
    deploy_airflow
end

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#   delete_and_rebuild_k3s
#
#   Completely removes the k3s cluster and rebuilds it from scratch,
#   including the local registry and Airflow deployment.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
function delete_and_rebuild_k3s
    _log "INFO" "Deleting k3s cluster..."
    /usr/local/bin/k3s-uninstall.sh
    _log "INFO" "Rebuilding k3s and Airflow from scratch..."
    setup_all
end

# --- Main Logic ---

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#   usage
#
#   Displays the help message for the script.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
function usage
    echo "Usage: ./setup_airflow.fish [FLAG]"
    echo ""
    echo "Manages the local Airflow on k3s environment."
    echo ""
    echo "Flags:"
    echo "  --refresh-values              Refresh the airflow-values.yaml only"
    echo "  --get-dashboard-secret        Get the Kubernetes Dashboard secret and proxy"
    echo "  --proxy-airflow-api           Proxy the Airflow API"
    echo "  --delete-and-rebuild-airflow  Delete and rebuild the Airflow deployment"
    echo "  --delete-and-rebuild-k3s      Delete the entire k3s cluster and rebuild"
    echo "  --rebuild-docker-image        Rebuild the Docker image and push to k3s"
    echo "  --help                        Show this help message"
end

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#   setup_all
#
#   The main setup function that runs all the necessary steps to get Airflow
#   up and running.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
function setup_all
    # Check for root privileges
    if test (id -u) -ne 0
        _log "ERROR" "This script must be run with sudo or as root."
        exit 1
    end

    _check_dependency "helm" "helm"
    _check_dependency "docker" "docker"
    install_k3s
    setup_local_registry
    build_and_push_image
    deploy_airflow
end

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#   main
#
#   Parses command-line arguments and calls the appropriate functions.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
function main
    if test (count $argv) -eq 0
        setup_all
        exit 0
    end

    switch $argv[1]
        case "--refresh-values"
            refresh_values
        case "--get-dashboard-secret"
            get_dashboard_secret
        case "--proxy-airflow-api"
            proxy_airflow_api
        case "--delete-and-rebuild-airflow"
            delete_and_rebuild_airflow
        case "--delete-and-rebuild-k3s"
            delete_and_rebuild_k3s
        case "--rebuild-docker-image"
            build_and_push_image
        case "--help"
            usage
        case "*"
            _log "ERROR" "Invalid flag: $argv[1]"
            usage
            exit 1
    end
end

main $argv
