param([Parameter(Mandatory=$true)][string]$Destination)
$ErrorActionPreference = 'Stop'
# Run from Anaconda Prompt or after activating the existing Anaconda installation.
& python "$PSScriptRoot/install.py" --bundle $PSScriptRoot --destination $Destination
if ($LASTEXITCODE -ne 0) { throw 'Offline installation or verification failed' }
