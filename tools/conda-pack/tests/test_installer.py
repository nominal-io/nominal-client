import hashlib
import importlib.util
import io
import sys
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


def test_extract_nested_file(tmp_path):
    """Keep ordinary nested environment files readable after safe extraction."""
    archive = tmp_path / "valid.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        item = tarfile.TarInfo("lib/payload")
        item.size = 7
        tar.addfile(item, io.BytesIO(b"payload"))
    destination = tmp_path / "environment"
    installer.extract(archive, destination)
    assert (destination / "lib/payload").read_bytes() == b"payload"


@pytest.mark.parametrize("link_type", [tarfile.LNKTYPE, tarfile.SYMTYPE])
def test_extract_internal_links(tmp_path, link_type):
    """Preserve internal Conda links without permitting links outside the environment."""
    if sys.platform == "win32" and link_type == tarfile.SYMTYPE:
        pytest.skip("Creating symbolic links requires additional Windows privileges")
    archive = tmp_path / "valid.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        item = tarfile.TarInfo("lib/payload")
        item.size = 7
        tar.addfile(item, io.BytesIO(b"payload"))
        link = tarfile.TarInfo("lib/alias")
        link.type = link_type
        link.linkname = "payload" if link_type == tarfile.SYMTYPE else "lib/payload"
        tar.addfile(link)
    destination = tmp_path / "environment"
    installer.extract(archive, destination)
    assert (destination / "lib/alias").read_bytes() == b"payload"


@pytest.mark.parametrize("member_type", [tarfile.FIFOTYPE, tarfile.CHRTYPE])
def test_reject_special_files(tmp_path, member_type):
    """Reject pipes and devices rather than creating them in an environment."""
    archive = tmp_path / "bad.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        item = tarfile.TarInfo("special")
        item.type = member_type
        tar.addfile(item)
    with pytest.raises(ValueError, match="Unsafe"):
        installer.extract(archive, tmp_path / "environment")
    assert not (tmp_path / "environment/special").exists()
