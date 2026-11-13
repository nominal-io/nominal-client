# Upstream recommendation

This branch hosts the standalone prototype in the SDK repository. Keep it unmerged until the required validation and supported-platform decisions are complete.

The live streaming wheel workflow at commit `f1153fa26970dd4f0a7476fa085b33cc02a0ff1d` defines eight targets: Windows x64, macOS ARM64, glibc Linux x64/ARM64/ARMv7, and musl Linux x64/ARM64/ARMv7. It does not define macOS Intel. A wheel build matrix does not establish Conda runtime coverage.

The checked-in availability report evaluates Python 3.11 through 3.15. All 20 binary dependency resolutions passed for the four targets with a supported current Conda runtime mapping. The other 20 combinations are unavailable in this prototype because ARMv7 and musl lack supported Conda runtime mappings. No availability result is an execution pass. Python 3.15 remains experimental in this prototype regardless of availability.

Before upstreaming:

1. Require offline and staging e2e results for each advertised supported platform/Python pair. Define unsupported combinations explicitly rather than making the streaming wheel matrix an unconditional Conda support promise.
2. Place the builder under `tools/conda-pack/` as an independent uv project, excluded from the SDK workspace and distributions. Keep runtime SDK dependencies unchanged.
3. Invoke the bundle workflow after successful stable main-package PyPI publication. Use the exact published SDK version, freshly resolve compatible stable analysis packages, and retain precise inventories.
4. Attach only verified archives and checksums to the corresponding public SDK release. Give each rebuild an immutable bundle revision. Keep platform failures separate from SDK publication success.
5. Use same-repository release-write permissions only in the upload job. No cross-repository dispatch or new App credentials are needed.
6. Keep customer identifiers and configuration out of public artifacts. Staging credentials belong only in the separate test job after packaging.

Deployment acceptance requires customer execution on its actual infrastructure. That remains separate from CI validation and from permission to import packages through the approved encrypted-media process.
