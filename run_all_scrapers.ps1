# run_all_scrapers.ps1
# Launches each grocery scraper in its own terminal window
#
# Usage:
#   .\run_all_scrapers.ps1                    # Run all scrapers with default query "milk" (fresh start)
#   .\run_all_scrapers.ps1 -Query "bread"     # Run all scrapers with custom query
#   .\run_all_scrapers.ps1 -Sites "safeway,sobeys"  # Run specific sites only
#   .\run_all_scrapers.ps1 -NoFresh           # Run without clearing old data

param(
    [string]$Query = "milk",
    [string]$Sites = "safeway,sobeys,nofrills,realcanadiansuperstore",
    [int]$MaxPages = 10,
    [switch]$NoFresh = $false
)

$FreshFlag = if ($NoFresh) { "" } else { "--fresh" }

$ProjectRoot = $PSScriptRoot
$VenvActivate = "C:\Users\ashto\Desktop\First_claude\.venv\Scripts\Activate.ps1"

# Parse sites
$SiteList = $Sites -split ","

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Grocery Scraper Launcher" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Query: $Query"
Write-Host "Max Pages: $MaxPages"
Write-Host "Fresh Start: $(if ($FreshFlag) { 'Yes' } else { 'No' })"
Write-Host "Sites: $($SiteList -join ', ')"
Write-Host ""

foreach ($site in $SiteList) {
    $site = $site.Trim()

    $cmd = @"
Set-Location '$ProjectRoot'
& '$VenvActivate'
Write-Host '========================================' -ForegroundColor Green
Write-Host '  Starting $site scraper' -ForegroundColor Green
Write-Host '  Query: $Query' -ForegroundColor Green
Write-Host '  Fresh Start: $(if ($FreshFlag) { 'Yes' } else { 'No' })' -ForegroundColor Green
Write-Host '========================================' -ForegroundColor Green
python -m scrapers.run --site $site --query "$Query" --max-pages $MaxPages $FreshFlag
Write-Host ''
Write-Host 'Press any key to close...' -ForegroundColor Yellow
`$null = `$Host.UI.RawUI.ReadKey('NoEcho,IncludeKeyDown')
"@

    Write-Host "Launching $site scraper..." -ForegroundColor Green
    Start-Process powershell -ArgumentList "-NoExit", "-Command", $cmd

    # Small delay between launches to avoid overwhelming the system
    Start-Sleep -Milliseconds 500
}

Write-Host ""
Write-Host "All scrapers launched in separate windows!" -ForegroundColor Cyan
Write-Host "Check each window for progress." -ForegroundColor Cyan
