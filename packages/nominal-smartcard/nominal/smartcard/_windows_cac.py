from __future__ import annotations

import base64
import gzip
import json
import os
import subprocess
import sys
from typing import Any

import requests
from requests.adapters import CaseInsensitiveDict

from nominal.core._utils.networking import GZIP_COMPRESSION_LEVEL, HeaderProviderSession

NOMINAL_WINDOWS_CERT_THUMBPRINT_ENV_VAR = "NOMINAL_WINDOWS_CERT_THUMBPRINT"
NOMINAL_WINDOWS_REQUIRE_PRIVATE_KEY_PROOF_ENV_VAR = "NOMINAL_WINDOWS_REQUIRE_PRIVATE_KEY_PROOF"
NOMINAL_WINDOWS_VERBOSE_CAC_LOG_ENV_VAR = "NOMINAL_WINDOWS_VERBOSE_CAC_LOG"
NOMINAL_WINDOWS_TEST_PIN_ENV_VAR = "NOMINAL_WINDOWS_TEST_PIN"

# Headers that requests/urllib3 manage internally and must not be forwarded to PowerShell.
_SKIP_HEADERS: frozenset[str] = frozenset(
    {"content-length", "host", "connection", "transfer-encoding", "accept-encoding"}
)

# PowerShell script that sends an HTTP request via Windows HttpClient + Schannel.
# Receives a JSON envelope on stdin; writes a JSON envelope to stdout.
# CAC certificate selection happens automatically via the Windows certificate store
# (CurrentUser\My) unless NOMINAL_WINDOWS_CERT_THUMBPRINT overrides it.
_WINDOWS_CAC_POWERSHELL = r"""
$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.Net.Http
Add-Type -AssemblyName System.Security

$requestJson = [Console]::In.ReadToEnd()
$request = $requestJson | ConvertFrom-Json
$cacEvents = [System.Collections.Generic.List[string]]::new()
$verboseCacLog = [bool]$request.verbose_cac_log

function Add-CacEvent {
    param([string]$Message)
    [void]$cacEvents.Add($Message)
    if ($verboseCacLog) {
        [Console]::Error.WriteLine("[nominal-cac] " + $Message)
    }
}

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
Add-CacEvent ("bridge started; process64={0}; powershell={1}" -f `
    [Environment]::Is64BitProcess, $PSVersionTable.PSVersion)

function Find-ClientAuthCertificate {
    param([string]$RequestedThumbprint)

    $store = [System.Security.Cryptography.X509Certificates.X509Store]::new(
        [System.Security.Cryptography.X509Certificates.StoreName]::My,
        [System.Security.Cryptography.X509Certificates.StoreLocation]::CurrentUser
    )
    $store.Open([System.Security.Cryptography.X509Certificates.OpenFlags]::ReadOnly)
    try {
        if ($RequestedThumbprint) {
            $thumbprint = $RequestedThumbprint.Replace(" ", "").ToUpperInvariant()
            Add-CacEvent ("certificate lookup: thumbprint override={0}" -f $thumbprint)
            $matches = $store.Certificates.Find(
                [System.Security.Cryptography.X509Certificates.X509FindType]::FindByThumbprint,
                $thumbprint,
                $false
            )
            if ($matches.Count -lt 1) {
                throw "Could not find client certificate thumbprint $thumbprint in CurrentUser\My."
            }
            Add-CacEvent ("certificate selected: subject={0}; thumbprint={1}; not_after={2:o}" -f `
                $matches[0].Subject, $matches[0].Thumbprint, $matches[0].NotAfter)
            return $matches[0]
        }

        Add-CacEvent "certificate lookup: automatic CurrentUser\My client-auth private-key search"
        $clientAuthOid = "1.3.6.1.5.5.7.3.2"
        $candidates = @(
            $store.Certificates | Where-Object {
                $_.HasPrivateKey -and
                ($_.EnhancedKeyUsageList | Where-Object { $_.ObjectId -eq $clientAuthOid })
            } | Sort-Object NotAfter -Descending
        )
        if ($candidates.Count -lt 1) {
            throw "Could not find a CurrentUser\My client-auth certificate with a private key."
        }
        Add-CacEvent ("certificate candidates found: {0}" -f $candidates.Count)
        Add-CacEvent ("certificate selected: subject={0}; thumbprint={1}; not_after={2:o}" -f `
            $candidates[0].Subject, $candidates[0].Thumbprint, $candidates[0].NotAfter)
        return $candidates[0]
    } finally {
        $store.Close()
    }
}

function Set-TestPinIfRequested {
    param(
        [object]$KeyObject,
        [string]$SubmittedPin
    )

    if ([string]::IsNullOrEmpty($SubmittedPin)) {
        return
    }

    Add-CacEvent "explicit test PIN supplied; submitting it to the Windows private-key provider"
    $pinBytes = [Text.Encoding]::Unicode.GetBytes($SubmittedPin + [char]0)
    $property = [System.Security.Cryptography.CngProperty]::new(
        "SmartCardPin",
        $pinBytes,
        [System.Security.Cryptography.CngPropertyOptions]::None
    )

    if ($KeyObject -is [System.Security.Cryptography.RSACng]) {
        $KeyObject.Key.SetProperty($property)
        Add-CacEvent "explicit test PIN attached to RSACng key"
        return
    }
    if ($KeyObject -is [System.Security.Cryptography.ECDsaCng]) {
        $KeyObject.Key.SetProperty($property)
        Add-CacEvent "explicit test PIN attached to ECDsaCng key"
        return
    }

    throw ("Explicit PIN testing requires a CNG private key. Actual key type: {0}" -f $KeyObject.GetType().FullName)
}

function Assert-PrivateKeyUsable {
    param(
        [System.Security.Cryptography.X509Certificates.X509Certificate2]$Certificate,
        [string]$SubmittedPin
    )

    $data = [Text.Encoding]::UTF8.GetBytes("nominal-cac-private-key-proof")
    Add-CacEvent "private-key proof requested"
    $rsa = [System.Security.Cryptography.X509Certificates.RSACertificateExtensions]::GetRSAPrivateKey($Certificate)
    if ($null -ne $rsa) {
        try {
            Add-CacEvent ("private-key object type: {0}" -f $rsa.GetType().FullName)
            Set-TestPinIfRequested $rsa $SubmittedPin
            [void]$rsa.SignData(
                $data,
                [System.Security.Cryptography.HashAlgorithmName]::SHA256,
                [System.Security.Cryptography.RSASignaturePadding]::Pkcs1
            )
            Add-CacEvent "private-key proof succeeded: RSA SHA256 PKCS1 signature created"
            return
        } finally {
            $rsa.Dispose()
        }
    }

    $ecdsa = [System.Security.Cryptography.X509Certificates.ECDsaCertificateExtensions]::GetECDsaPrivateKey(
        $Certificate)
    if ($null -ne $ecdsa) {
        try {
            Add-CacEvent ("private-key object type: {0}" -f $ecdsa.GetType().FullName)
            Set-TestPinIfRequested $ecdsa $SubmittedPin
            [void]$ecdsa.SignData($data, [System.Security.Cryptography.HashAlgorithmName]::SHA256)
            Add-CacEvent "private-key proof succeeded: ECDSA SHA256 signature created"
            return
        } finally {
            $ecdsa.Dispose()
        }
    }

    throw "Selected certificate does not expose an RSA or ECDSA private key."
}

$selectedCertificate = $null
if ($request.cert_thumbprint -or $request.require_private_key_proof) {
    $selectedCertificate = Find-ClientAuthCertificate ([string]$request.cert_thumbprint)
}
if ($request.require_private_key_proof) {
    Assert-PrivateKeyUsable $selectedCertificate ([string]$request.test_pin)
} else {
    Add-CacEvent "private-key proof not requested"
}

$handler = [System.Net.Http.HttpClientHandler]::new()
$handler.UseProxy = $true
$handler.Proxy = [System.Net.WebRequest]::DefaultWebProxy
$handler.ClientCertificateOptions = [System.Net.Http.ClientCertificateOption]::Automatic
$handler.AutomaticDecompression = (
    [System.Net.DecompressionMethods]::GZip -bor
    [System.Net.DecompressionMethods]::Deflate
)
# AutomaticDecompression causes HttpClient to add its own Accept-Encoding header and
# decompress the response transparently. Do not forward Accept-Encoding from Python.

if ($null -ne $selectedCertificate) {
    $handler.ClientCertificateOptions = [System.Net.Http.ClientCertificateOption]::Manual
    [void]$handler.ClientCertificates.Add($selectedCertificate)
    Add-CacEvent "manual client certificate attached to HttpClientHandler"
} else {
    Add-CacEvent "HttpClientHandler automatic client certificate selection enabled"
}

$client = [System.Net.Http.HttpClient]::new($handler)
try {
    $client.Timeout = [TimeSpan]::FromSeconds([double]$request.timeout_seconds)
    $message = [System.Net.Http.HttpRequestMessage]::new(
        [System.Net.Http.HttpMethod]::new([string]$request.method),
        [Uri]([string]$request.url)
    )

    if ($request.body_b64) {
        $bodyBytes = [Convert]::FromBase64String([string]$request.body_b64)
        $message.Content = [System.Net.Http.ByteArrayContent]::new($bodyBytes)
    }

    $skipHeaders = @("Content-Length", "Host", "Connection", "Transfer-Encoding", "Accept-Encoding")
    if ($request.headers) {
        foreach ($header in $request.headers.PSObject.Properties) {
            $name = [string]$header.Name
            $value = [string]$header.Value
            if ($skipHeaders -contains $name) { continue }
            if (-not $message.Headers.TryAddWithoutValidation($name, $value)) {
                if ($null -ne $message.Content) {
                    [void]$message.Content.Headers.TryAddWithoutValidation($name, $value)
                }
            }
        }
    }

    Add-CacEvent ("http request sending: {0} {1}" -f $request.method, $request.url)
    $response = $client.SendAsync($message).GetAwaiter().GetResult()
    Add-CacEvent ("http response received: status={0}" -f [int]$response.StatusCode)
    $responseBytes = $response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult()
    $headers = [ordered]@{}
    foreach ($header in $response.Headers) {
        $headers[$header.Key] = ($header.Value -join ", ")
    }
    foreach ($header in $response.Content.Headers) {
        $headers[$header.Key] = ($header.Value -join ", ")
    }

    [pscustomobject]@{
        status_code = [int]$response.StatusCode
        reason      = $response.ReasonPhrase
        headers     = $headers
        body_b64    = [Convert]::ToBase64String($responseBytes)
        url         = $response.RequestMessage.RequestUri.AbsoluteUri
        cac_events  = $cacEvents.ToArray()
    } | ConvertTo-Json -Depth 8 -Compress
} finally {
    $client.Dispose()
    $handler.Dispose()
}
"""


def _timeout_to_seconds(timeout: object) -> float:
    if isinstance(timeout, tuple):
        values = [v for v in timeout if v is not None]
        return max(float(max(values)) if values else 300.0, 300.0)
    if timeout is None:
        return 300.0
    return max(float(timeout), 300.0)


def _encode_powershell_command(script: str) -> str:
    return base64.b64encode(script.encode("utf-16le")).decode("ascii")


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "") not in {"", "0", "false", "False", "FALSE"}


def _cac_log(message: str) -> None:
    print(f"[nominal-cac] {message}", file=sys.stderr)


class WindowsCacSession(HeaderProviderSession):
    r"""requests.Session backed by Windows HttpClient + Schannel CAC transport.

    Sends each HTTP request via a PowerShell subprocess that uses the Windows
    .NET HttpClient. The Windows certificate store (CurrentUser\My) supplies the
    CAC certificate automatically; Schannel handles PIN prompting natively through
    the Windows credential UI, so no PIN entry is required in Python.

    Request bodies are gzip-compressed before forwarding (mirroring the behaviour
    of NominalRequestsAdapter for the non-Windows path).
    """

    def send(
        self,
        request: requests.PreparedRequest,
        **kwargs: Any,
    ) -> requests.Response:
        # Compress the request body, mirroring NominalRequestsAdapter for non-streaming requests.
        body = request.body
        if body is not None and not kwargs.get("stream"):
            raw: bytes = body if isinstance(body, bytes) else body.encode("utf-8")
            compressed = gzip.compress(raw, compresslevel=GZIP_COMPRESSION_LEVEL)
            request.headers["Content-Encoding"] = "gzip"
            request.headers["Content-Length"] = str(len(compressed))
            body_b64 = base64.b64encode(compressed).decode("ascii")
        else:
            body_b64 = ""

        headers: dict[str, str] = {
            (k.decode("ascii", errors="replace") if isinstance(k, bytes) else str(k)): (
                v.decode("ascii", errors="replace") if isinstance(v, bytes) else str(v)
            )
            for k, v in request.headers.items()
            if (k.lower() if isinstance(k, bytes) else k.lower()) not in _SKIP_HEADERS
        }

        verbose = _env_flag(NOMINAL_WINDOWS_VERBOSE_CAC_LOG_ENV_VAR)
        require_proof = _env_flag(NOMINAL_WINDOWS_REQUIRE_PRIVATE_KEY_PROOF_ENV_VAR)
        timeout_seconds = _timeout_to_seconds(kwargs.get("timeout"))

        envelope = {
            "method": request.method,
            "url": request.url,
            "headers": headers,
            "body_b64": body_b64,
            "timeout_seconds": timeout_seconds,
            "cert_thumbprint": os.environ.get(NOMINAL_WINDOWS_CERT_THUMBPRINT_ENV_VAR, ""),
            "require_private_key_proof": require_proof,
            "verbose_cac_log": verbose,
            "test_pin": os.environ.get(NOMINAL_WINDOWS_TEST_PIN_ENV_VAR, ""),
        }

        if verbose:
            _cac_log(f"Windows native transport: {request.method} {request.url}")
            _cac_log(f"private-key proof required: {require_proof}")
            _cac_log(f"certificate thumbprint override: {bool(envelope['cert_thumbprint'])}")

        encoded_command = _encode_powershell_command(_WINDOWS_CAC_POWERSHELL)
        try:
            completed = subprocess.run(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-EncodedCommand",
                    encoded_command,
                ],
                check=False,
                input=json.dumps(envelope),
                text=True,
                capture_output=True,
                timeout=timeout_seconds + 60,
            )
        except subprocess.TimeoutExpired as exc:
            raise requests.exceptions.Timeout(
                f"Windows CAC request timed out after {timeout_seconds:g}s: {request.method} {request.url}"
            ) from exc
        except OSError as exc:
            raise requests.exceptions.SSLError(
                "Failed to start powershell.exe for Windows CAC transport. "
                "Ensure PowerShell is available and the CAC middleware is installed."
            ) from exc

        if completed.returncode != 0:
            detail = (
                completed.stderr.strip()
                or completed.stdout.strip()
                or f"powershell.exe exited with code {completed.returncode}"
            )
            raise requests.exceptions.SSLError(f"Windows CAC request failed: {detail}")

        try:
            payload = json.loads(completed.stdout.strip())
        except json.JSONDecodeError as exc:
            raise requests.exceptions.SSLError(
                f"Windows CAC transport returned invalid JSON: {completed.stdout!r}"
            ) from exc

        if verbose:
            for event in payload.get("cac_events") or ():
                _cac_log(str(event))
            _cac_log(f"Windows CAC transport completed: HTTP {payload.get('status_code')}")

        response = requests.Response()
        response.status_code = int(payload.get("status_code", 0))
        response.reason = payload.get("reason") or ""
        response.headers = CaseInsensitiveDict(payload.get("headers") or {})
        response._content = base64.b64decode(payload.get("body_b64") or "")  # type: ignore[attr-defined]
        response.url = payload.get("url") or str(request.url)
        response.request = request
        response.encoding = requests.utils.get_encoding_from_headers(response.headers)
        return response
