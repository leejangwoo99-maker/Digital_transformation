param([switch]$AcceptLicenses)
$ErrorActionPreference = 'Stop'
$sdk = Join-Path $env:LOCALAPPDATA 'Android\Sdk'
$manager = Join-Path $sdk 'cmdline-tools\latest\bin\sdkmanager.bat'
if (!(Test-Path -LiteralPath $manager)) {
    $cache = Join-Path $PSScriptRoot 'downloads'
    New-Item -ItemType Directory -Force -Path $cache | Out-Null
    [xml]$repository = (Invoke-WebRequest 'https://dl.google.com/android/repository/repository2-1.xml' -UseBasicParsing).Content
    $package = $repository.SelectNodes("//*[local-name()='remotePackage']") |
        Where-Object { $_.path -like 'cmdline-tools;*' -and $_.revision.major -and !$_.revision.preview } |
        Sort-Object { [int]$_.revision.major } -Descending | Select-Object -First 1
    $archive = $package.archives.archive | Where-Object { $_.'host-os' -eq 'windows' } | Select-Object -First 1
    $zip = Join-Path $cache 'command-line-tools.zip'
    & curl.exe -L --fail --output $zip ('https://dl.google.com/android/repository/' + $archive.complete.url)
    if ($LASTEXITCODE) { throw 'SDK download failed' }
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $unpacked = Join-Path 'C:\vision_inspection' ('sdk-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
    [IO.Compression.ZipFile]::ExtractToDirectory($zip, $unpacked)
    New-Item -ItemType Directory -Force -Path (Join-Path $sdk 'cmdline-tools') | Out-Null
    Move-Item -LiteralPath (Join-Path $unpacked 'cmdline-tools') -Destination (Join-Path $sdk 'cmdline-tools\latest')
}
$env:JAVA_HOME = Split-Path (Split-Path (Get-Command java.exe).Source)
if ($AcceptLicenses) { 1..20 | ForEach-Object { 'y' } | & $manager --sdk_root=$sdk --licenses }
else { & $manager --sdk_root=$sdk --licenses }
& $manager --sdk_root=$sdk 'platform-tools' 'platforms/android-35' 'build-tools/35.0.0'
if ($LASTEXITCODE) { throw 'SDK installation failed' }
Write-Output "Android SDK: $sdk"
