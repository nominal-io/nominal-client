"""Focused public modules and package imports share the same API objects."""

from importlib import import_module

from nominal.experimental import extractor as ex


def test_public_modules_own_their_package_exports():
    exports = {
        "decorators": ("input", "parameter", "error", "manifest_extractor", "single_file_extractor"),
        "context": ("ExtractorContext", "ManifestExtractorContext", "SingleFileExtractorContext"),
        "runner": ("Extractor",),
        "types": ("BadParameter", "Choice", "IntRange", "FloatRange"),
    }
    for name, symbols in exports.items():
        module = import_module(f"nominal.experimental.extractor.{name}")
        assert set(module.__all__) == set(symbols)
        for symbol in symbols:
            exported = vars(module)[symbol]
            assert exported is vars(ex)[symbol]
            assert exported.__module__ == module.__name__
