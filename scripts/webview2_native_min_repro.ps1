param(
    [string]$ArtifactsDir = ".codex-tmp\webview2-native-min-repro",
    [string]$RuntimePath = "",
    [int]$TimeoutSeconds = 6,
    [ValidateSet("html", "url")]
    [string]$Mode = "html"
)

$ErrorActionPreference = 'Stop'

function Write-Event {
    param(
        [string]$ArtifactsDir,
        [string]$EventName,
        [hashtable]$Fields = @{}
    )

    $row = @{
        ts = [DateTimeOffset]::UtcNow.ToString("o")
        event = $EventName
        fields = $Fields
    }
    $path = Join-Path $ArtifactsDir "repro-events.jsonl"
    $json = $row | ConvertTo-Json -Compress -Depth 8
    Add-Content -Path $path -Value $json -Encoding UTF8
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$artifactsDirAbs = [System.IO.Path]::GetFullPath((Join-Path $repoRoot $ArtifactsDir))
$null = New-Item -ItemType Directory -Force -Path $artifactsDirAbs
$storageDir = Join-Path $artifactsDirAbs "local-user-data\webview"
$null = New-Item -ItemType Directory -Force -Path $storageDir
$stderrPath = Join-Path $artifactsDirAbs "repro-error.txt"
$htmlPath = Join-Path $artifactsDirAbs "repro.html"
$startedAt = [System.Diagnostics.Stopwatch]::StartNew()

Write-Event -ArtifactsDir $artifactsDirAbs -EventName "repro_launch_start" -Fields @{
    runtimePath = $RuntimePath
    mode = $Mode
}

$html = @"
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Native WebView2 Minimal Repro</title>
</head>
<body data-repro-ready="1">
  <h1>Native WebView2 Minimal Repro</h1>
  <p id="ready">ready</p>
</body>
</html>
"@
Set-Content -Path $htmlPath -Value $html -Encoding UTF8
Write-Event -ArtifactsDir $artifactsDirAbs -EventName "repro_storage_ready" -Fields @{
    storagePath = $storageDir
}

Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
$webviewLib = Join-Path $env:APPDATA "Python\Python313\site-packages\webview\lib"
Add-Type -Path (Join-Path $webviewLib "Microsoft.Web.WebView2.Core.dll")
Add-Type -Path (Join-Path $webviewLib "Microsoft.Web.WebView2.WinForms.dll")
Write-Event -ArtifactsDir $artifactsDirAbs -EventName "repro_webview_loaded" -Fields @{
    elapsedMs = [int]$startedAt.ElapsedMilliseconds
}

$form = New-Object System.Windows.Forms.Form
$form.Text = "Baluffo Native WebView2 Repro"
$form.Width = 1100
$form.Height = 720

$webview = New-Object Microsoft.Web.WebView2.WinForms.WebView2
$webview.Dock = [System.Windows.Forms.DockStyle]::Fill
$form.Controls.Add($webview)

$null = $form.add_Shown({
    Write-Event -ArtifactsDir $artifactsDirAbs -EventName "repro_window_shown" -Fields @{
        elapsedMs = [int]$startedAt.ElapsedMilliseconds
    }
})

$null = $webview.add_CoreWebView2InitializationCompleted({
    param($sender, $eventArgs)
    Write-Event -ArtifactsDir $artifactsDirAbs -EventName "repro_core_initialized" -Fields @{
        elapsedMs = [int]$startedAt.ElapsedMilliseconds
        isSuccess = [bool]$eventArgs.IsSuccess
        exception = if ($eventArgs.InitializationException) { $eventArgs.InitializationException.ToString() } else { "" }
    }
    if ($eventArgs.IsSuccess) {
        if ($Mode -eq "url") {
            $sender.Source = [Uri]((New-Object System.Uri($htmlPath)).AbsoluteUri)
        } else {
            $sender.NavigateToString($html)
        }
    }
})

$null = $webview.add_NavigationCompleted({
    param($sender, $eventArgs)
    Write-Event -ArtifactsDir $artifactsDirAbs -EventName "repro_navigation_completed" -Fields @{
        elapsedMs = [int]$startedAt.ElapsedMilliseconds
        isSuccess = [bool]$eventArgs.IsSuccess
        webErrorStatus = $eventArgs.WebErrorStatus.ToString()
    }
})

try {
    Write-Event -ArtifactsDir $artifactsDirAbs -EventName "repro_env_create_start" -Fields @{
        elapsedMs = [int]$startedAt.ElapsedMilliseconds
    }
    $browserExecutableFolder = if ($RuntimePath) { $RuntimePath } else { $null }
    $createTask = [Microsoft.Web.WebView2.Core.CoreWebView2Environment]::CreateAsync($browserExecutableFolder, $storageDir, $null)
    $createTask.Wait()
    $environment = $createTask.Result
    Write-Event -ArtifactsDir $artifactsDirAbs -EventName "repro_env_create_done" -Fields @{
        elapsedMs = [int]$startedAt.ElapsedMilliseconds
        browserVersion = $environment.BrowserVersionString
    }

    if ($RuntimePath) {
        Write-Event -ArtifactsDir $artifactsDirAbs -EventName "repro_runtime_configured" -Fields @{
            elapsedMs = [int]$startedAt.ElapsedMilliseconds
            runtimePath = $RuntimePath
            storagePath = $storageDir
        }
    }

    Write-Event -ArtifactsDir $artifactsDirAbs -EventName "repro_ensure_start" -Fields @{
        elapsedMs = [int]$startedAt.ElapsedMilliseconds
    }
    $ensureTask = $webview.EnsureCoreWebView2Async($environment)
    $form.Show()
    $deadline = [DateTime]::UtcNow.AddSeconds([Math]::Max(1, $TimeoutSeconds))
    $closeLogged = $false
    $ensureHandled = $false

    while (-not $form.IsDisposed) {
        [System.Windows.Forms.Application]::DoEvents()

        if (-not $closeLogged -and [DateTime]::UtcNow -ge $deadline) {
            $closeLogged = $true
            Write-Event -ArtifactsDir $artifactsDirAbs -EventName "repro_close_requested" -Fields @{
                elapsedMs = [int]$startedAt.ElapsedMilliseconds
            }
            $form.Close()
        }

        if (-not $ensureHandled -and $ensureTask.IsCompleted) {
            $ensureHandled = $true
            if ($ensureTask.IsFaulted) {
                $errorText = $ensureTask.Exception.ToString()
                Write-Event -ArtifactsDir $artifactsDirAbs -EventName "repro_ensure_faulted" -Fields @{
                    elapsedMs = [int]$startedAt.ElapsedMilliseconds
                    error = $errorText
                }
                Set-Content -Path $stderrPath -Value $errorText -Encoding UTF8
                throw $ensureTask.Exception
            }
            Write-Event -ArtifactsDir $artifactsDirAbs -EventName "repro_ensure_returned" -Fields @{
                elapsedMs = [int]$startedAt.ElapsedMilliseconds
            }
            $ensureTask.GetAwaiter().GetResult() | Out-Null
        }

        Start-Sleep -Milliseconds 50
    }

    Write-Event -ArtifactsDir $artifactsDirAbs -EventName "repro_run_returned" -Fields @{
        elapsedMs = [int]$startedAt.ElapsedMilliseconds
    }
}
catch {
    Write-Event -ArtifactsDir $artifactsDirAbs -EventName "repro_error" -Fields @{
        elapsedMs = [int]$startedAt.ElapsedMilliseconds
        error = $_.Exception.ToString()
    }
    Set-Content -Path $stderrPath -Value $_.Exception.ToString() -Encoding UTF8
    throw
}
