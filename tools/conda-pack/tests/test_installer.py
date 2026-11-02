import hashlib
import importlib.util
import io
import tarfile

import pytest

from nominal_conda_pack.cli import RESOURCES

spec = importlib.util.spec_from_file_location("installer", RESOURCES / "install.py")
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)


def test_reject_traversal(tmp_path):
    """Reject archive members that would write outside the destination."""
    archive = tmp_path / "bad.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        item = tarfile.TarInfo("../outside")
        item.size = 1
        tar.addfile(item, io.BytesIO(b"x"))
    with pytest.raises(ValueError, match="Unsafe"):
        installer.extract(archive, tmp_path / "dest")
    assert not (tmp_path / "outside").exists()


def test_reject_escaping_symlink(tmp_path):
    """Reject archive symlinks that escape the destination."""
    archive = tmp_path / "bad.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        item = tarfile.TarInfo("link")
        item.type = tarfile.SYMTYPE
        item.linkname = "../../outside"
        tar.addfile(item)
    with pytest.raises(ValueError, match="Unsafe"):
        installer.extract(archive, tmp_path / "dest")


def test_checksum_tampering(tmp_path):
    """Detect payload corruption before extracting any environment files."""
    names = [
        "environment.tar.gz",
        "manifest.json",
        "smoke.py",
        "example.ipynb",
        "inventory.py",
        "install.py",
        "launch.py",
    ]
    for name in names:
        (tmp_path / name).write_bytes(b"original")
    (tmp_path / "SHA256SUMS").write_text(
        "".join(f"{hashlib.sha256(b'original').hexdigest()}  {name}\n" for name in names)
    )
    installer.check(tmp_path)
    (tmp_path / "environment.tar.gz").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="Checksum mismatch"):
        installer.check(tmp_path)


def test_missing_payload(tmp_path):
    """Reject checksum manifests missing required installation payloads."""
    (tmp_path / "SHA256SUMS").write_text("")
    with pytest.raises(ValueError, match="Incomplete"):
        installer.check(tmp_path)
