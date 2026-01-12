# Complete Test & Fix Script for Grocery Scrapers
# This script will:
# 1. Check dependencies
# 2. Install missing dependencies if needed
# 3. Run all scrapers with proper module syntax
# 4. Provide detailed diagnostics

$ErrorActionPreference = "Continue"

Write-Host "`n============================================================" -ForegroundColor Cyan
Write-Host "  GROCERY SCRAPER - COMPREHENSIVE TEST & FIX" -ForegroundColor Cyan
Write-Host "============================================================`n" -ForegroundColor Cyan

# Change to project root
$projectRoot = "C:\Users\ashto\Desktop\First_claude\test_claude"
Set-Location $projectRoot

Write-Host "[STEP 1] Checking current directory..." -ForegroundColor Yellow
Write-Host "Current directory: $(Get-Location)" -ForegroundColor Gray

# Check dependencies
Write-Host "`n[STEP 2] Checking dependencies..." -ForegroundColor Yellow

$missingDeps = @()

# Check Python packages
$packages = @("requests", "beautifulsoup4", "lxml", "playwright")
foreach ($package in $packages) {
    $installed = python -c "import $($package.Replace('beautifulsoup4', 'bs4'))" 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[X] $package is NOT installed" -ForegroundColor Red
        $missingDeps += $package
    } else {
        Write-Host "[OK] $package is installed" -ForegroundColor Green
    }
}

# Check Playwright browsers
if ("playwright" -notin $missingDeps) {
    Write-Host "`nChecking Playwright browsers..." -ForegroundColor Gray
    $browserCheck = python -c "from playwright.sync_api import sync_playwright; p = sync_playwright().start(); p.chromium.launch(headless=True); p.stop()" 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[X] Playwright browsers NOT installed" -ForegroundColor Red
        Write-Host "    (This is needed for Safeway and Sobeys)" -ForegroundColor Gray
        $missingDeps += "playwright-browsers"
    } else {
        Write-Host "[OK] Playwright browsers installed" -ForegroundColor Green
    }
}

# Install missing dependencies if any
if ($missingDeps.Count -gt 0) {
    Write-Host "`n[STEP 3] Installing missing dependencies..." -ForegroundColor Yellow
    Write-Host "Missing: $($missingDeps -join ', ')" -ForegroundColor Red
    
    $response = Read-Host "`nInstall missing dependencies now? (y/n)"
    if ($response -eq 'y') {
        # Install Python packages
        $pythonPackages = $missingDeps | Where-Object { $_ -ne "playwright-browsers" }
        if ($pythonPackages.Count -gt 0) {
            Write-Host "Installing Python packages..." -ForegroundColor Gray
            pip install $pythonPackages
        }
        
        # Install Playwright browsers
        if ("playwright-browsers" -in $missingDeps) {
            Write-Host "Installing Playwright browsers (this may take a few minutes)..." -ForegroundColor Gray
            playwright install chromium
        }
        
        Write-Host "[OK] Dependencies installed" -ForegroundColor Green
    } else {
        Write-Host "[WARNING] Skipping installation. Safeway and Sobeys will fail without Playwright." -ForegroundColor Yellow
    }
} else {
    Write-Host "`n[STEP 3] All dependencies are installed!" -ForegroundColor Green
}

# Test scrapers
Write-Host "`n[STEP 4] Testing all scrapers..." -ForegroundColor Yellow

$sites = @(
    @{name="realcanadiansuperstore"; query="milk"; needsPlaywright=$false},
    @{name="nofrills"; query="milk"; needsPlaywright=$false},
    @{name="safeway"; query="milk"; needsPlaywright=$true},
    @{name="sobeys"; query="milk"; needsPlaywright=$true}
)

$results = @()

foreach ($site in $sites) {
    $siteName = $site.name
    Write-Host "`n------------------------------------------------------------" -ForegroundColor Cyan
    Write-Host "Testing: $siteName" -ForegroundColor Yellow
    Write-Host "------------------------------------------------------------" -ForegroundColor Cyan
    
    # Skip Playwright-dependent scrapers if browsers not installed
    if ($site.needsPlaywright -and "playwright-browsers" -in $missingDeps) {
        Write-Host "[SKIP] $siteName requires Playwright browsers (not installed)" -ForegroundColor Yellow
        $results += @{
            site = $siteName
            status = "SKIPPED"
            records = 0
            error = "Playwright browsers not installed"
        }
        continue
    }
    
    # Clear old output
    $jsonlPath = "data\raw\$siteName\${siteName}_products.jsonl"
    if (Test-Path $jsonlPath) {
        Remove-Item $jsonlPath -Force
        Write-Host "[INFO] Cleared old output file" -ForegroundColor Gray
    }
    
    # Run scraper using CORRECT module syntax
    Write-Host "[INFO] Running: python -m scrapers.run --site $siteName --query 'milk' --max-pages 1" -ForegroundColor Gray
    
    $output = python -m scrapers.run --site $siteName --query $site.query --max-pages 1 2>&1
    $exitCode = $LASTEXITCODE
    
    # Check results
    if (Test-Path $jsonlPath) {
        $recordCount = (Get-Content $jsonlPath).Count
        if ($recordCount -gt 0) {
            Write-Host "[SUCCESS] $siteName - $recordCount records scraped" -ForegroundColor Green
            $results += @{
                site = $siteName
                status = "SUCCESS"
                records = $recordCount
                error = $null
            }
        } else {
            Write-Host "[FAIL] $siteName - 0 records scraped" -ForegroundColor Red
            Write-Host "[DEBUG] Last 10 lines of output:" -ForegroundColor Gray
            $output | Select-Object -Last 10 | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
            $results += @{
                site = $siteName
                status = "FAIL"
                records = 0
                error = "0 records scraped"
            }
        }
    } else {
        Write-Host "[FAIL] $siteName - No output file created" -ForegroundColor Red
        Write-Host "[DEBUG] Exit code: $exitCode" -ForegroundColor Gray
        Write-Host "[DEBUG] Last 10 lines of output:" -ForegroundColor Gray
        $output | Select-Object -Last 10 | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
        $results += @{
            site = $siteName
            status = "FAIL"
            records = 0
            error = "No output file created"
        }
    }
}

# Summary
Write-Host "`n============================================================" -ForegroundColor Cyan
Write-Host "  TEST SUMMARY" -ForegroundColor Cyan
Write-Host "============================================================`n" -ForegroundColor Cyan

$successCount = ($results | Where-Object { $_.status -eq "SUCCESS" }).Count
$failCount = ($results | Where-Object { $_.status -eq "FAIL" }).Count
$skipCount = ($results | Where-Object { $_.status -eq "SKIPPED" }).Count
$totalRecords = ($results | Measure-Object -Property records -Sum).Sum

Write-Host "Total Tests: $($results.Count)" -ForegroundColor White
Write-Host "Success: $successCount" -ForegroundColor Green
Write-Host "Failed: $failCount" -ForegroundColor Red
Write-Host "Skipped: $skipCount" -ForegroundColor Yellow
Write-Host "Total Records: $totalRecords`n" -ForegroundColor White

# Detailed results
$results | ForEach-Object {
    $status = $_.status
    $color = switch ($status) {
        "SUCCESS" { "Green" }
        "FAIL" { "Red" }
        "SKIPPED" { "Yellow" }
    }
    Write-Host "[$status] $($_.site) - $($_.records) records" -ForegroundColor $color
    if ($_.error) {
        Write-Host "    Error: $($_.error)" -ForegroundColor Gray
    }
}

# Recommendations
Write-Host "`n============================================================" -ForegroundColor Cyan
Write-Host "  RECOMMENDATIONS" -ForegroundColor Cyan
Write-Host "============================================================`n" -ForegroundColor Cyan

if ($failCount -gt 0 -or $skipCount -gt 0) {
    Write-Host "Issues detected. Here's what to do:`n" -ForegroundColor Yellow
    
    if ("playwright-browsers" -in $missingDeps) {
        Write-Host "1. Install Playwright browsers for Safeway/Sobeys:" -ForegroundColor White
        Write-Host "   playwright install chromium`n" -ForegroundColor Gray
    }
    
    Write-Host "2. Check individual scraper logs:" -ForegroundColor White
    Write-Host "   Get-Content data\logs\<site_name>.log`n" -ForegroundColor Gray
    
    Write-Host "3. Run a single scraper with debug logging:" -ForegroundColor White
    Write-Host "   python -m scrapers.run --site safeway --query 'test' --max-pages 1 --log-level DEBUG --headful`n" -ForegroundColor Gray
    
    Write-Host "4. View this test script results:" -ForegroundColor White
    Write-Host "   The detailed output above shows what failed and why.`n" -ForegroundColor Gray
} else {
    Write-Host "All scrapers working correctly! You're ready to go.`n" -ForegroundColor Green
    Write-Host "Next steps:" -ForegroundColor White
    Write-Host "  - Run larger scrapes: python -m scrapers.run --site realcanadiansuperstore --query 'milk' --max-pages 10" -ForegroundColor Gray
    Write-Host "  - Export to CSV: python -m scrapers.run --site nofrills --query 'bread' --output-format csv`n" -ForegroundColor Gray
}

Write-Host "============================================================`n" -ForegroundColor Cyan