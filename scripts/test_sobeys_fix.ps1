# Test Sobeys Scraper Fix
# This script tests the fixed Sobeys scraper with enhanced diagnostics

Write-Host "Testing Sobeys scraper fix..." -ForegroundColor Cyan
Write-Host ""

# Test with headful mode for visual debugging
Write-Host "Running Sobeys scraper (headful mode for debugging)..." -ForegroundColor Yellow
python -m scrapers.run --site sobeys --query "milk" --max-pages 1 --headful --log-level DEBUG

Write-Host ""
Write-Host "Checking results..." -ForegroundColor Cyan

$jsonlFile = "data\sobeys\sobeys_products.jsonl"
$timestamp = (Get-Date).AddSeconds(-120)  # File must be from last 2 minutes

if (Test-Path $jsonlFile) {
    $fileTime = (Get-Item $jsonlFile).LastWriteTime

    if ($fileTime -gt $timestamp) {
        $recordCount = (Get-Content $jsonlFile | Measure-Object -Line).Lines

        if ($recordCount -gt 0) {
            Write-Host "SUCCESS! Scraped $recordCount products" -ForegroundColor Green
            Write-Host ""
            Write-Host "Sample product:" -ForegroundColor Cyan
            Get-Content $jsonlFile -First 1 | ConvertFrom-Json | Format-List
        } else {
            Write-Host "FAILED - JSONL file is empty" -ForegroundColor Red
            Write-Host "Check debug files in data\sobeys\ for screenshots and HTML dumps" -ForegroundColor Yellow
        }
    } else {
        Write-Host "FAILED - File exists but was not updated (old data)" -ForegroundColor Red
        Write-Host "File timestamp: $fileTime" -ForegroundColor Gray
        Write-Host "Check debug files in data\sobeys\ for screenshots and HTML dumps" -ForegroundColor Yellow
    }
} else {
    Write-Host "FAILED - No output file created" -ForegroundColor Red
    Write-Host "Check debug files in data\sobeys\ for screenshots and HTML dumps" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Looking for debug files..." -ForegroundColor Cyan
$debugFiles = Get-ChildItem "data\sobeys\debug_*" -ErrorAction SilentlyContinue | Where-Object { $_.LastWriteTime -gt $timestamp }
if ($debugFiles) {
    Write-Host "Debug files created (products didn't load):" -ForegroundColor Yellow
    $debugFiles | ForEach-Object { Write-Host "  - $($_.Name)" -ForegroundColor Gray }
} else {
    Write-Host "No debug files (products loaded successfully)" -ForegroundColor Green
}
