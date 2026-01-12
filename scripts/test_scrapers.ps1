# Comprehensive Scraper Test Script
# Tests all 4 grocery scrapers and validates output

# Script configuration
$ErrorActionPreference = "Continue"  # Continue on errors to test all scrapers
$testStartTime = Get-Date

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

# Test configuration
$sites = @(
    @{name="realcanadiansuperstore"; query="milk"; needs_playwright=$false},
    @{name="nofrills"; query="bread"; needs_playwright=$false},
    @{name="safeway"; query="eggs"; needs_playwright=$true},
    @{name="sobeys"; query="cheese"; needs_playwright=$true}
)

$maxPages = 1  # Test with 1 page only
$testResults = @()

# Validation helper functions
function Test-RecentFile {
    param(
        [string]$FilePath,
        [int]$MaxAgeSeconds = 30
    )

    if (-not (Test-Path $FilePath)) {
        return $false
    }

    $fileModified = (Get-Item $FilePath).LastWriteTime
    $ageSeconds = (New-TimeSpan -Start $fileModified -End (Get-Date)).TotalSeconds

    return $ageSeconds -le $MaxAgeSeconds
}

function Get-JsonlRecordCount {
    param([string]$FilePath)

    if (-not (Test-Path $FilePath)) {
        return 0
    }

    try {
        $lineCount = (Get-Content $FilePath | Measure-Object -Line).Lines
        return $lineCount
    }
    catch {
        return 0
    }
}

function Test-ScraperOutput {
    param(
        [string]$SiteName,
        [string]$OutputPath,
        [string]$LogOutput,
        [int]$ExpectedMinRecords = 1
    )

    $result = @{
        site = $SiteName
        success = $false
        records = 0
        file_created = $false
        file_recent = $false
        errors = @()
    }

    # Check if file exists
    if (-not (Test-Path $OutputPath)) {
        $result.errors += "Output file not created"
        return $result
    }
    $result.file_created = $true

    # Check if file was recently modified (within 30 seconds of test start)
    if (-not (Test-RecentFile $OutputPath -MaxAgeSeconds 60)) {
        $fileModified = (Get-Item $OutputPath).LastWriteTime
        $result.errors += "File not recently modified (last modified: $fileModified)"
        return $result
    }
    $result.file_recent = $true

    # Count records in JSONL file
    $recordCount = Get-JsonlRecordCount $OutputPath
    $result.records = $recordCount

    if ($recordCount -lt $ExpectedMinRecords) {
        $result.errors += "Too few records ($recordCount, expected at least $ExpectedMinRecords)"
        return $result
    }

    # Check for errors in log output
    if ($LogOutput -match "ERROR|FAILED|Exception|Traceback") {
        $result.errors += "Errors found in command output"
        return $result
    }

    # All checks passed
    $result.success = $true
    return $result
}

# Main test execution
Write-Header "GROCERY SCRAPER COMPREHENSIVE TEST"
Write-Info "Test started at: $testStartTime"
Write-Info "Testing with --max-pages $maxPages"

# Pre-flight checks
Write-Step "Running pre-flight validation..."
try {
    python validate_setup.py
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Setup validation found issues, but continuing with tests..."
    }
}
catch {
    Write-Warning "Could not run validate_setup.py (continuing anyway)"
}

# Test each scraper
foreach ($site in $sites) {
    $siteName = $site.name
    $query = $site.query

    Write-Header "Testing: $siteName"
    Write-Info "Search query: '$query'"

    # Define output path
    $outputPath = "data\raw\$siteName\${siteName}_products.jsonl"

    # Delete existing output file to ensure fresh test
    if (Test-Path $outputPath) {
        Write-Info "Deleting existing output file for clean test..."
        Remove-Item $outputPath -Force
    }

    # Build command
    $command = "python -m scrapers.run --site $siteName --query `"$query`" --max-pages $maxPages"

    Write-Step "Running scraper..."
    Write-Info "Command: $command"

    # Execute scraper and capture output
    $scrapeStartTime = Get-Date
    $output = & python -m scrapers.run --site $siteName --query $query --max-pages $maxPages 2>&1 | Out-String
    $scrapeEndTime = Get-Date
    $scrapeDuration = (New-TimeSpan -Start $scrapeStartTime -End $scrapeEndTime).TotalSeconds

    Write-Info "Scraping completed in $([math]::Round($scrapeDuration, 2)) seconds"

    # Validate output
    Write-Step "Validating output..."
    $testResult = Test-ScraperOutput -SiteName $siteName -OutputPath $outputPath -LogOutput $output

    # Display results
    if ($testResult.success) {
        Write-Success "$siteName - PASSED"
        Write-Info "Records scraped: $($testResult.records)"
    }
    else {
        Write-Error "$siteName - FAILED"
        Write-Info "File created: $($testResult.file_created)"
        Write-Info "File recent: $($testResult.file_recent)"
        Write-Info "Records found: $($testResult.records)"
        foreach ($error in $testResult.errors) {
            Write-Error "  - $error"
        }

        # Show last 20 lines of output for debugging
        Write-Info "Last 20 lines of output:"
        $output -split "`n" | Select-Object -Last 20 | ForEach-Object {
            Write-Host "  $_" -ForegroundColor DarkGray
        }
    }

    # Add to results
    $testResults += $testResult

    # Pause between tests
    if ($site -ne $sites[-1]) {
        Write-Info "Waiting 2 seconds before next test..."
        Start-Sleep -Seconds 2
    }
}

# Summary
Write-Header "TEST SUMMARY"

$passedCount = ($testResults | Where-Object { $_.success }).Count
$failedCount = ($testResults | Where-Object { -not $_.success }).Count
$totalRecords = ($testResults | Measure-Object -Property records -Sum).Sum

Write-Host ""
Write-Host "Total Tests: $($testResults.Count)" -ForegroundColor Cyan
Write-Host "Passed: $passedCount" -ForegroundColor Green
Write-Host "Failed: $failedCount" -ForegroundColor Red
Write-Host "Total Records: $totalRecords" -ForegroundColor Cyan
Write-Host ""

# Detailed results table
Write-Host "Detailed Results:" -ForegroundColor Cyan
Write-Host ""
$resultsTable = $testResults | ForEach-Object {
    [PSCustomObject]@{
        Site = $_.site
        Status = if ($_.success) { "PASS" } else { "FAIL" }
        Records = $_.records
        FileCreated = $_.file_created
        FileRecent = $_.file_recent
    }
}
$resultsTable | Format-Table -AutoSize

# Failed tests details
if ($failedCount -gt 0) {
    Write-Host ""
    Write-Host "Failed Tests Details:" -ForegroundColor Red
    Write-Host ""

    foreach ($result in $testResults | Where-Object { -not $_.success }) {
        Write-Host "  $($result.site):" -ForegroundColor Yellow
        foreach ($error in $result.errors) {
            Write-Host "    - $error" -ForegroundColor Red
        }
    }

    Write-Host ""
    Write-Host "Troubleshooting Tips:" -ForegroundColor Yellow
    Write-Host "  1. Check log files in data/logs/<site_name>.log" -ForegroundColor White
    Write-Host "  2. Run validate_setup.py to check dependencies" -ForegroundColor White
    Write-Host "  3. Try running a single scraper manually:" -ForegroundColor White
    Write-Host "     python -m scrapers.run --site <site_name> --query 'test' --max-pages 1 --log-level DEBUG" -ForegroundColor Gray
    Write-Host ""
}

# Exit code
$testEndTime = Get-Date
$totalDuration = (New-TimeSpan -Start $testStartTime -End $testEndTime).TotalSeconds

Write-Info "Total test duration: $([math]::Round($totalDuration, 2)) seconds"

if ($failedCount -eq 0) {
    Write-Success "All tests PASSED!"
    exit 0
}
else {
    Write-Error "$failedCount test(s) FAILED"
    exit 1
}
