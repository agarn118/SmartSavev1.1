# Test Script for Stealth Google Navigation
# This script verifies the enhanced Safeway/Sobeys scrapers are working correctly

$ErrorActionPreference = "Continue"

Write-Host "`n============================================================" -ForegroundColor Cyan
Write-Host "  STEALTH SCRAPER - GOOGLE NAVIGATION TEST" -ForegroundColor Cyan
Write-Host "============================================================`n" -ForegroundColor Cyan

# Configuration
$projectRoot = "C:\Users\ashto\Desktop\First_claude\test_claude"
Set-Location $projectRoot

Write-Host "[INFO] Current directory: $(Get-Location)" -ForegroundColor Gray
Write-Host "[INFO] Testing enhanced Safeway and Sobeys scrapers with Google navigation`n" -ForegroundColor Gray

# Function to run a test
function Test-Scraper {
    param(
        [string]$Site,
        [string]$TestName,
        [string]$Command
    )
    
    Write-Host "`n------------------------------------------------------------" -ForegroundColor Yellow
    Write-Host "TEST: $TestName" -ForegroundColor Yellow
    Write-Host "SITE: $Site" -ForegroundColor Yellow
    Write-Host "------------------------------------------------------------" -ForegroundColor Yellow
    Write-Host "Command: $Command`n" -ForegroundColor Gray
    
    $startTime = Get-Date
    
    # Execute command
    Invoke-Expression $Command
    
    $endTime = Get-Date
    $duration = ($endTime - $startTime).TotalSeconds
    
    Write-Host "`n[INFO] Test completed in $([math]::Round($duration, 1)) seconds" -ForegroundColor Gray
    
    # Check output
    $outputFile = "data\raw\$Site\${Site}_products.jsonl"
    
    if (Test-Path $outputFile) {
        $recordCount = (Get-Content $outputFile | Measure-Object -Line).Lines
        
        if ($recordCount -gt 0) {
            Write-Host "[SUCCESS] Found $recordCount products in output file" -ForegroundColor Green
            
            # Show first product
            Write-Host "`n[SAMPLE] First product:" -ForegroundColor Cyan
            Get-Content $outputFile | Select-Object -First 1 | ConvertFrom-Json | Format-List
            
            return $true
        } else {
            Write-Host "[FAIL] Output file exists but contains 0 products" -ForegroundColor Red
            return $false
        }
    } else {
        Write-Host "[FAIL] Output file not found: $outputFile" -ForegroundColor Red
        return $false
    }
}

# Test 1: Safeway with Google navigation (visible browser)
Write-Host "`n================================================================" -ForegroundColor Cyan
Write-Host "TEST 1: Safeway Product Search with Google Navigation (Visible)" -ForegroundColor Cyan
Write-Host "================================================================`n" -ForegroundColor Cyan
Write-Host "[INFO] This test will:" -ForegroundColor Yellow
Write-Host "  1. Open a visible Chrome window" -ForegroundColor White
Write-Host "  2. Navigate to Google.com" -ForegroundColor White
Write-Host "  3. Search for 'safeway'" -ForegroundColor White
Write-Host "  4. Click the first organic result" -ForegroundColor White
Write-Host "  5. Handle popups automatically" -ForegroundColor White
Write-Host "  6. Select Airdrie store" -ForegroundColor White
Write-Host "  7. Search for 'milk' and scrape 1 page" -ForegroundColor White
Write-Host "`n[ACTION] Watch the browser to see stealth features in action!" -ForegroundColor Green
Write-Host "[ACTION] Press any key to start..." -ForegroundColor Green
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

$test1Result = Test-Scraper `
    -Site "safeway" `
    -TestName "Safeway Search with Visible Browser" `
    -Command "python -m scrapers.run --site safeway --query 'milk' --max-pages 1 --headful --log-level INFO"

# Test 2: Sobeys with Google navigation (headless)
Write-Host "`n================================================================" -ForegroundColor Cyan
Write-Host "TEST 2: Sobeys Category Scrape with Google Navigation (Headless)" -ForegroundColor Cyan
Write-Host "================================================================`n" -ForegroundColor Cyan
Write-Host "[INFO] This test will run in the background (headless mode)" -ForegroundColor Yellow
Write-Host "[INFO] This is faster and uses less resources" -ForegroundColor Yellow
Write-Host "`n[ACTION] Press any key to start..." -ForegroundColor Green
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

$test2Result = Test-Scraper `
    -Site "sobeys" `
    -TestName "Sobeys Category with Headless Browser" `
    -Command "python -m scrapers.run --site sobeys --query 'cheese' --max-pages 1 --headless --log-level INFO"

# Test 3: Verify configuration
Write-Host "`n================================================================" -ForegroundColor Cyan
Write-Host "TEST 3: Configuration Verification" -ForegroundColor Cyan
Write-Host "================================================================`n" -ForegroundColor Cyan

$safewayConfig = Get-Content "configs\safeway.json" | ConvertFrom-Json
$sobeysConfig = Get-Content "configs\sobeys.json" | ConvertFrom-Json

Write-Host "[CHECK] Safeway Configuration:" -ForegroundColor Yellow
Write-Host "  use_google_navigation: $($safewayConfig.use_google_navigation)" -ForegroundColor White
Write-Host "  google_search_term: $($safewayConfig.google_search_term)" -ForegroundColor White
Write-Host "  store_postal_code: $($safewayConfig.store_postal_code)" -ForegroundColor White
Write-Host "  store_name_filter: $($safewayConfig.store_name_filter)" -ForegroundColor White

Write-Host "`n[CHECK] Sobeys Configuration:" -ForegroundColor Yellow
Write-Host "  use_google_navigation: $($sobeysConfig.use_google_navigation)" -ForegroundColor White
Write-Host "  google_search_term: $($sobeysConfig.google_search_term)" -ForegroundColor White
Write-Host "  store_postal_code: $($sobeysConfig.store_postal_code)" -ForegroundColor White
Write-Host "  store_name_filter: $($sobeysConfig.store_name_filter)" -ForegroundColor White

$configOk = ($safewayConfig.use_google_navigation -eq $true) -and ($sobeysConfig.use_google_navigation -eq $true)

if ($configOk) {
    Write-Host "`n[SUCCESS] Google navigation is enabled for both sites" -ForegroundColor Green
} else {
    Write-Host "`n[WARNING] Google navigation may be disabled" -ForegroundColor Yellow
}

# Summary
Write-Host "`n============================================================" -ForegroundColor Cyan
Write-Host "  TEST SUMMARY" -ForegroundColor Cyan
Write-Host "============================================================`n" -ForegroundColor Cyan

$totalTests = 3
$passedTests = 0

if ($test1Result) { 
    $passedTests++ 
    Write-Host "[PASS] Test 1: Safeway visible browser test" -ForegroundColor Green
} else {
    Write-Host "[FAIL] Test 1: Safeway visible browser test" -ForegroundColor Red
}

if ($test2Result) { 
    $passedTests++ 
    Write-Host "[PASS] Test 2: Sobeys headless browser test" -ForegroundColor Green
} else {
    Write-Host "[FAIL] Test 2: Sobeys headless browser test" -ForegroundColor Red
}

if ($configOk) { 
    $passedTests++ 
    Write-Host "[PASS] Test 3: Configuration verification" -ForegroundColor Green
} else {
    Write-Host "[FAIL] Test 3: Configuration verification" -ForegroundColor Red
}

Write-Host "`n[RESULT] $passedTests/$totalTests tests passed`n" -ForegroundColor Cyan

# Recommendations
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  RECOMMENDATIONS" -ForegroundColor Cyan
Write-Host "============================================================`n" -ForegroundColor Cyan

if ($passedTests -eq $totalTests) {
    Write-Host "âœ… All tests passed! Your stealth scrapers are working perfectly." -ForegroundColor Green
    Write-Host "`nNext steps:" -ForegroundColor White
    Write-Host "  1. Try scraping more pages: --max-pages 10" -ForegroundColor Gray
    Write-Host "  2. Use categories: --category-url '/shop/aisles/dairy-eggs'" -ForegroundColor Gray
    Write-Host "  3. Export to CSV: --output-format both" -ForegroundColor Gray
    Write-Host "`nExample:" -ForegroundColor White
    Write-Host "  python -m scrapers.run --site safeway --query 'bread' --max-pages 5 --output-format csv" -ForegroundColor Cyan
} else {
    Write-Host "âš  Some tests failed. Here's what to do:" -ForegroundColor Yellow
    Write-Host "`n1. Check the logs:" -ForegroundColor White
    Write-Host "   Get-Content data\logs\safeway.log | Select-Object -Last 50" -ForegroundColor Gray
    Write-Host "   Get-Content data\logs\sobeys.log | Select-Object -Last 50" -ForegroundColor Gray
    Write-Host "`n2. Verify Playwright is installed:" -ForegroundColor White
    Write-Host "   playwright install chromium" -ForegroundColor Gray
    Write-Host "`n3. Try with debug logging:" -ForegroundColor White
    Write-Host "   python -m scrapers.run --site safeway --query 'test' --max-pages 1 --headful --log-level DEBUG" -ForegroundColor Gray
    Write-Host "`n4. Disable Google navigation temporarily:" -ForegroundColor White
    Write-Host "   Edit configs\safeway.json and set 'use_google_navigation: false'" -ForegroundColor Gray
}

Write-Host "`n============================================================`n" -ForegroundColor Cyan

# Check logs location
Write-Host "[INFO] Log files location:" -ForegroundColor Yellow
Get-ChildItem "data\logs" -Filter "*.log" | ForEach-Object {
    $size = [math]::Round($_.Length / 1KB, 2)
    Write-Host "  $($_.Name) - ${size} KB" -ForegroundColor Gray
}

Write-Host "`n[INFO] Output files location:" -ForegroundColor Yellow
Get-ChildItem "data\raw\*\*.jsonl" | ForEach-Object {
    $lines = (Get-Content $_.FullName | Measure-Object -Line).Lines
    Write-Host "  $($_.Name) - $lines products" -ForegroundColor Gray
}

Write-Host "`n[DONE] Test script completed!`n" -ForegroundColor Cyan