$ErrorActionPreference = 'Stop'
$sdk = Join-Path $env:LOCALAPPDATA 'Android\Sdk'
$tools = Join-Path $sdk 'build-tools\35.0.0'
$jar = Join-Path $sdk 'platforms\android-35\android.jar'
$build = Join-Path $PSScriptRoot ('build-' + (Get-Date -Format 'yyyyMMdd_HHmmss'))
$classes = Join-Path $build 'classes'
$dex = Join-Path $build 'dex'
New-Item -ItemType Directory -Force -Path $classes,$dex | Out-Null
& javac --release 8 -encoding UTF-8 -classpath $jar -d $classes (Join-Path $PSScriptRoot 'src\com\visioninspection\phone\MainActivity.java')
if ($LASTEXITCODE) { throw 'Java compilation failed' }
& jar cf (Join-Path $build 'classes.jar') -C $classes .
& java -cp (Join-Path $tools 'lib\d8.jar') com.android.tools.r8.D8 --min-api 30 --lib $jar --output $dex (Join-Path $build 'classes.jar')
if ($LASTEXITCODE) { throw 'DEX compilation failed' }
$unsigned = Join-Path $build 'unsigned.apk'
& (Join-Path $tools 'aapt2.exe') link -I $jar --manifest (Join-Path $PSScriptRoot 'AndroidManifest.xml') -o $unsigned
if ($LASTEXITCODE) { throw 'APK packaging failed' }
& jar uf $unsigned -C $dex classes.dex
$aligned = Join-Path $build 'aligned.apk'
& (Join-Path $tools 'zipalign.exe') -f -p 4 $unsigned $aligned
$key = Join-Path $PSScriptRoot 'development.keystore'
if (!(Test-Path -LiteralPath $key)) {
    & keytool -genkeypair -keystore $key -storepass android -keypass android -alias vision -keyalg RSA -keysize 2048 -validity 3650 -dname 'CN=Vision Inspection Development'
}
$apk = Join-Path $PSScriptRoot 'VisionUsbCamera.apk'
& (Join-Path $tools 'apksigner.bat') sign --ks $key --ks-key-alias vision --ks-pass pass:android --key-pass pass:android --out $apk $aligned
if ($LASTEXITCODE) { throw 'APK signing failed' }
& (Join-Path $tools 'apksigner.bat') verify --verbose $apk
if ($LASTEXITCODE) { throw 'APK verification failed' }
Write-Output $apk
