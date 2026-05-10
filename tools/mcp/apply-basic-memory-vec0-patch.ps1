[CmdletBinding()]
param(
    [switch]$Apply,
    [string]$PackageRoot = "$env:APPDATA\uv\tools\basic-memory\Lib\site-packages\basic_memory",
    [string]$BackupRoot = "C:\tmp"
)

$ErrorActionPreference = "Stop"

function Stop-WithMessage {
    param([string]$Message)
    Write-Error $Message
    exit 1
}

function Read-Text {
    param([string]$Path)
    return [System.IO.File]::ReadAllText($Path)
}

function Write-Text {
    param(
        [string]$Path,
        [string]$Text
    )
    $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
    [System.IO.File]::WriteAllText($Path, $Text, $utf8NoBom)
}

$versionOutput = (& basic-memory --version) -join "`n"
if ($versionOutput -notmatch "Basic Memory version:\s+0\.20\.3") {
    Stop-WithMessage "Refusing to patch: expected Basic Memory 0.20.3, got: $versionOutput"
}

$repoPath = Join-Path $PackageRoot "repository\sqlite_search_repository.py"
$servicePath = Join-Path $PackageRoot "services\search_service.py"

foreach ($path in @($repoPath, $servicePath)) {
    if (-not (Test-Path -LiteralPath $path)) {
        Stop-WithMessage "Refusing to patch: expected installed source file not found: $path"
    }
}

$repoText = Read-Text $repoPath
$serviceText = Read-Text $servicePath

$repoPatched = $repoText.Contains("async def purge_stale_vector_embeddings(self) -> None:")
$servicePatched = $serviceText.Contains("await self.repository.purge_stale_vector_embeddings()")

if ($repoPatched -and $servicePatched) {
    Write-Host "Basic Memory vec0 patch is already applied."
    exit 0
}

if ($repoPatched -xor $servicePatched) {
    Stop-WithMessage "Refusing to patch: partial vec0 patch detected. Restore from backup or inspect installed files manually."
}

if ($repoText -notmatch "async def _ensure_sqlite_vec_loaded\(self, session\) -> None:" -or
    $repoText -notmatch "async def _ensure_vector_tables\(self\) -> None:") {
    Stop-WithMessage "Refusing to patch: sqlite_search_repository.py source shape does not match expected Basic Memory 0.20.3 anchors."
}

if ($serviceText -notmatch "async def _purge_stale_search_rows\(self\) -> None:" -or
    $serviceText -notmatch "DELETE FROM search_vector_embeddings WHERE rowid IN") {
    Stop-WithMessage "Refusing to patch: search_service.py source shape does not match expected Basic Memory 0.20.3 anchors."
}

if (-not $Apply) {
    Write-Host "Basic Memory vec0 patch is missing."
    Write-Host "Re-run with -Apply to back up and patch the installed Basic Memory 0.20.3 tool."
    exit 2
}

$repoOld = @'
            await driver_connection.load_extension(sqlite_vec.loadable_path())
            await driver_connection.enable_load_extension(False)
            await session.execute(text("SELECT vec_version()"))

    # ------------------------------------------------------------------
    # Abstract hook implementations (vector/semantic, SQLite-specific)
    # ------------------------------------------------------------------
'@

$repoNew = @'
            await driver_connection.load_extension(sqlite_vec.loadable_path())
            await driver_connection.enable_load_extension(False)
            await session.execute(text("SELECT vec_version()"))

    async def purge_stale_vector_embeddings(self) -> None:
        """Delete stale SQLite vec0 rows after vector tables and extension are ready.

        sqlite-vec registers the vec0 module per SQLite connection. Generic repository
        utility queries open their own session, so touching search_vector_embeddings
        there can fail with "no such module: vec0" even when sqlite-vec is installed.
        """
        await self._ensure_vector_tables()
        async with db.scoped_session(self.session_maker) as session:
            await self._ensure_sqlite_vec_loaded(session)
            await session.execute(
                text(
                    "DELETE FROM search_vector_embeddings WHERE rowid IN ("
                    "SELECT id FROM search_vector_chunks "
                    "WHERE project_id = :project_id "
                    "AND entity_id NOT IN ("
                    "SELECT id FROM entity WHERE project_id = :project_id))"
                ),
                {"project_id": self.project_id},
            )

    # ------------------------------------------------------------------
    # Abstract hook implementations (vector/semantic, SQLite-specific)
    # ------------------------------------------------------------------
'@

if (-not $repoText.Contains($repoOld)) {
    Stop-WithMessage "Refusing to patch: sqlite_search_repository.py insertion point not found."
}

$servicePattern = '(?s)        # SQLite vec has no CASCADE.*?        # Postgres CASCADE handles embedding deletion automatically'
$serviceReplacement = @'
        # SQLite vec has no CASCADE - must delete embeddings before chunks
        if isinstance(self.repository, SQLiteSearchRepository):
            await self.repository.purge_stale_vector_embeddings()

        # Postgres CASCADE handles embedding deletion automatically
'@

$updatedServiceText = [System.Text.RegularExpressions.Regex]::Replace(
    $serviceText,
    $servicePattern,
    $serviceReplacement,
    1
)

if ($updatedServiceText -eq $serviceText) {
    Stop-WithMessage "Refusing to patch: search_service.py vector cleanup block not replaced."
}

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$backup = Join-Path $BackupRoot "basic-memory-vec0-backup-$stamp"
New-Item -ItemType Directory -Force -Path $backup | Out-Null
Copy-Item -LiteralPath $repoPath -Destination (Join-Path $backup "sqlite_search_repository.py")
Copy-Item -LiteralPath $servicePath -Destination (Join-Path $backup "search_service.py")

Write-Text $repoPath ($repoText.Replace($repoOld, $repoNew))
Write-Text $servicePath $updatedServiceText

Write-Host "Applied Basic Memory vec0 patch."
Write-Host "Backup: $backup"
