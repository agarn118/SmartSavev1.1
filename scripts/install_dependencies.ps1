# Grocery Scraper - Automated Dependency Installer
# This script installs all required Python packages and Playwright browsers

# Script configuration
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

# Color output functions
function Write-Header {
    param([string]$Message)
    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host "  $Message" -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor Cyan
}

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "[STEP] $Message" -ForegroundColor Yellow
}

function Write-Success {
    param([string]$Message)
    Write-Host "[OK] $Message" -ForegroundColor Green
}

function Write-Error {
    param([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

function Write-Info {
    param([string]$Message)
    Write-Host "[INFO] $Message" -ForegroundColor Gray
}

# Main installation process
try {
    Write-Header "GROCERY SCRAPER - DEPENDENCY INSTALLER"

    # Check if Python is installed
    Write-Step "Checking Python installation..."
    try {
        $pythonVersion = python --version 2>&1
        Write-Success "Python is installed: $pythonVersion"
    }
    catch {
        Write-Error "Python is not installed or not in PATH"
        Write-Info "Please install Python 3.8+ from https://www.python.org/"
        exit 1
    }

    # Check if pip is available
    Write-Step "Checking pip installation..."
    try {
        $pipVersion = pip --version 2>&1
        Write-Success "pip is available: $pipVersion"
    }
    catch {
        Write-Error "pip is not installed or not in PATH"
        Write-Info "Please ensure pip is installed with your Python installation"
        exit 1
    }

    # Upgrade pip (optional but recommended)
    Write-Step "Upgrading pip to latest version..."
    try {
        python -m pip install --upgrade pip | Out-Null
        Write-Success "pip upgraded successfully"
    }
    catch {
        Write-Info "Could not upgrade pip (not critical, continuing...)"
    }

    # Install Python packages from requirements.txt
    Write-Step "Installing Python packages from requirements.txt..."
    if (Test-Path "requirements.txt") {
        try {
            pip install -r requirements.txt
            if ($LASTEXITCODE -eq 0) {
                Write-Success "All Python packages installed successfully"
            }
            else {
                Write-Error "Some packages failed to install"
                Write-Info "Check the error messages above for details"
                exit 1
            }
        }
        catch {
            Write-Error "Failed to install packages from requirements.txt"
            throw
        }
    }
    else {
        Write-Error "requirements.txt not found in current directory"
        Write-Info "Please run this script from the project root directory"
        exit 1
    }

    # Install Playwright browsers
    Write-Step "Installing Playwright Chromium browser..."
    Write-Info "This may take a few minutes as it downloads the browser (~100MB)..."
    try {
        playwright install chromium
        if ($LASTEXITCODE -eq 0) {
            Write-Success "Playwright Chromium browser installed successfully"
        }
        else {
            Write-Error "Playwright browser installation failed"
            Write-Info "You can try installing manually with: playwright install chromium"
            exit 1
        }
    }
    catch {
        Write-Error "Failed to install Playwright browsers"
        Write-Info "Make sure Playwright package is installed first"
        throw
    }

    # Verify installation
    Write-Header "VERIFICATION"
    Write-Step "Verifying installation..."

    Write-Info "Running dependency checker..."
    python check_dependencies.py

    if ($LASTEXITCODE -eq 0) {
        Write-Header "INSTALLATION COMPLETE"
        Write-Success "All dependencies installed and verified successfully!"
        Write-Host ""
        Write-Host "You can now run the scrapers:" -ForegroundColor Cyan
        Write-Host "  python -m scrapers.run --site realcanadiansuperstore --query 'milk'" -ForegroundColor White
        Write-Host "  python -m scrapers.run --site nofrills --query 'bread'" -ForegroundColor White
        Write-Host "  python -m scrapers.run --site safeway --query 'eggs'" -ForegroundColor White
        Write-Host "  python -m scrapers.run --site sobeys --query 'cheese'" -ForegroundColor White
        Write-Host ""
    }
    else {
        Write-Header "INSTALLATION COMPLETE WITH WARNINGS"
        Write-Info "Some dependencies may not have installed correctly."
        Write-Info "Check the output above for details."
        Write-Host ""
    }

}
catch {
    Write-Header "INSTALLATION FAILED"
    Write-Error "An error occurred during installation"
    Write-Info "Error details: $_"
    Write-Host ""
    Write-Host "Troubleshooting:" -ForegroundColor Yellow
    Write-Host "  1. Make sure you're running this from the project root directory" -ForegroundColor White
    Write-Host "  2. Check that Python and pip are in your PATH" -ForegroundColor White
    Write-Host "  3. Try installing packages manually:" -ForegroundColor White
    Write-Host "     pip install -r requirements.txt" -ForegroundColor Gray
    Write-Host "     playwright install chromium" -ForegroundColor Gray
    Write-Host ""
    exit 1
}
