param()

$ErrorActionPreference = 'Stop'

if (-not $env:TELEGRAM_TOKEN -and -not $env:BOT_TOKEN -and (Test-Path '.\.env')) {
    Get-Content '.\.env' | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith('#') -or -not $line.Contains('=')) {
            return
        }

        $parts = $line.Split('=', 2)
        $key = $parts[0].Trim()
        $value = $parts[1].Trim().Trim('"').Trim("'")

        if ($key -eq 'TELEGRAM_TOKEN' -and $value) {
            $env:TELEGRAM_TOKEN = $value
        }
        elseif ($key -eq 'BOT_TOKEN' -and $value) {
            $env:BOT_TOKEN = $value
        }
    }
}

if (-not $env:TELEGRAM_TOKEN -and -not $env:BOT_TOKEN) {
    $secureToken = Read-Host -Prompt 'Enter Telegram token' -AsSecureString
    $tokenPtr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureToken)
    try {
        $token = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($tokenPtr)
        $env:TELEGRAM_TOKEN = $token
    }
    finally {
        if ($tokenPtr -ne [IntPtr]::Zero) {
            [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($tokenPtr)
        }
    }
}

python bot.py