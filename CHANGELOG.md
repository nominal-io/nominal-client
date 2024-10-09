# Changelog

## [1.6.0](https://github.com/nominal-io/nominal-client/compare/v1.5.0...v1.6.0) (2024-10-09)


### Features

* allow users to get channel metadata and set channel types from SDK ([#68](https://github.com/nominal-io/nominal-client/issues/68)) ([04c915b](https://github.com/nominal-io/nominal-client/commit/04c915b58c349cf851f0129b33c74102437f8f90))


### Bug Fixes

* remove component from release please tag ([#75](https://github.com/nominal-io/nominal-client/issues/75)) ([3bb709c](https://github.com/nominal-io/nominal-client/commit/3bb709c785bd62cf34d4716cfebfbc329386e0b9))
* update release please config ([#73](https://github.com/nominal-io/nominal-client/issues/73)) ([ed5a5f4](https://github.com/nominal-io/nominal-client/commit/ed5a5f4ceb1a407492b4a3bedcb7864df3f27710))

## [1.5.0](https://github.com/nominal-io/nominal-client/compare/v1.4.1...v1.5.0) (2024-10-07)


### Features

* add access to the units endpoint to the python client ([#65](https://github.com/nominal-io/nominal-client/issues/65)) ([06d3962](https://github.com/nominal-io/nominal-client/commit/06d39626c1f5aa6bc9166775daaf1b55c963533e))
* checklist support ([#61](https://github.com/nominal-io/nominal-client/issues/61)) ([f9105d9](https://github.com/nominal-io/nominal-client/commit/f9105d9ced054cae535a95d0aa0bf19f4cdf801e))

## [1.4.1](https://github.com/nominal-io/nominal-client/compare/v1.4.0...v1.4.1) (2024-10-03)


### Bug Fixes

* change default connection timeout to 30s (up from 10s) ([#63](https://github.com/nominal-io/nominal-client/issues/63)) ([19531dd](https://github.com/nominal-io/nominal-client/commit/19531dd8b83563ca870269dba60b61368f27c28b))

## [1.4.0](https://github.com/nominal-io/nominal-client/compare/v1.3.0...v1.4.0) (2024-10-02)


### Features

* cleanup & simplify timestamp handling ([#54](https://github.com/nominal-io/nominal-client/issues/54)) ([2211070](https://github.com/nominal-io/nominal-client/commit/2211070fc3c93a781a36298451cdb106a4e7ed2e))
* get current user ([#62](https://github.com/nominal-io/nominal-client/issues/62)) ([44c3270](https://github.com/nominal-io/nominal-client/commit/44c327006cb7254fa6db1f14bca06299f46dbb60))

## [1.3.0](https://github.com/nominal-io/nominal-client/compare/v1.2.0...v1.3.0) (2024-09-27)


### Features

* add function to poll ingestion status for multiple datasets ([#58](https://github.com/nominal-io/nominal-client/issues/58)) ([a6327e2](https://github.com/nominal-io/nominal-client/commit/a6327e2c69c9c7d626c78a91ca4d01c31651cddf))
* add start/end to Run.update() ([#55](https://github.com/nominal-io/nominal-client/issues/55)) ([98c4adb](https://github.com/nominal-io/nominal-client/commit/98c4adb0505ad033d78245a40fe234711a583912))


### Bug Fixes

* search_runs parameter rename: exact_name -&gt; name_substring ([#59](https://github.com/nominal-io/nominal-client/issues/59)) ([f2f2b7d](https://github.com/nominal-io/nominal-client/commit/f2f2b7dc994787df980b594886a8172f5eb8e4c9))

## [1.2.0](https://github.com/nominal-io/nominal-client/compare/v1.1.0...v1.2.0) (2024-09-25)


### Features

* all get* methods to use only RIDs ([#46](https://github.com/nominal-io/nominal-client/issues/46)) ([0a7abca](https://github.com/nominal-io/nominal-client/commit/0a7abca422966ad8d699e11992bee734e2519e93))
* support log sets ([#53](https://github.com/nominal-io/nominal-client/issues/53)) ([cb1cda3](https://github.com/nominal-io/nominal-client/commit/cb1cda3900c08f27cdf8ba8368ddd658b5630a23))

## [1.1.0](https://github.com/nominal-io/nominal-client/compare/v1.0.0...v1.1.0) (2024-09-17)


### Features

* add bulk add datasource method to run ([#47](https://github.com/nominal-io/nominal-client/issues/47)) ([f874dda](https://github.com/nominal-io/nominal-client/commit/f874dda882fa1637662f7794ff883fd81bcc8f50))
* add csv to dataset method ([#38](https://github.com/nominal-io/nominal-client/issues/38)) ([f28efe4](https://github.com/nominal-io/nominal-client/commit/f28efe4961d39fa2fff436f7d36726a1c4f949b3))
* add video support ([#42](https://github.com/nominal-io/nominal-client/issues/42)) ([b09532a](https://github.com/nominal-io/nominal-client/commit/b09532ab4aac76e794ab95cdd139b67f2bf4c5d0))


### Bug Fixes

* detect .csv.gz and add helper method for initial csv creation ([#41](https://github.com/nominal-io/nominal-client/issues/41)) ([8cc307d](https://github.com/nominal-io/nominal-client/commit/8cc307d9234f913c5f5d8fa4a180641416fe2ab9))
* file path extension check fails for paths with other "."s in them ([#45](https://github.com/nominal-io/nominal-client/issues/45)) ([9e2d735](https://github.com/nominal-io/nominal-client/commit/9e2d73555659b2f1cef2da84e091ca4efd6d1ee0))


### Documentation

* update repeated start typo for Create a Run ([#43](https://github.com/nominal-io/nominal-client/issues/43)) ([6513b96](https://github.com/nominal-io/nominal-client/commit/6513b96ccaf55334dd276c3ca58e2be7ea960fc5))

## [1.0.0](https://github.com/nominal-io/nominal-client/compare/v1.0.0-beta...v1.0.0) (2024-09-13)


### Features

* add docs, rename sdk submodule -&gt; core ([#32](https://github.com/nominal-io/nominal-client/issues/32)) ([77ff5e0](https://github.com/nominal-io/nominal-client/commit/77ff5e0c447190e2ac3c79ae3e25bf22c45d0d78))


### Bug Fixes

* better token error message ([#37](https://github.com/nominal-io/nominal-client/issues/37)) ([9386375](https://github.com/nominal-io/nominal-client/commit/9386375ef44dfa44a710739161c9b65b70f9e988))


### Miscellaneous Chores

* release 1.0.0 ([32724fb](https://github.com/nominal-io/nominal-client/commit/32724fb0aa453f009c8af772361bfddced3d36a6))

## [1.0.0-beta](https://github.com/nominal-io/nominal-client/compare/v0.5.0...v1.0.0-beta) (2024-09-12)


### Miscellaneous Chores

* release 1.0.0-beta ([1e5fe1a](https://github.com/nominal-io/nominal-client/commit/1e5fe1a8839ee230beeddb85c135fc4cbc768889))
* release 1.0.0-beta ([f636885](https://github.com/nominal-io/nominal-client/commit/f6368850ad0eabff577533f162a0f94c43d3f5ac))

## 0.5.0 (2024-09-12)


### Features

* lint for python formatting ([7ed7cef](https://github.com/nominal-io/nominal-client/commit/7ed7cef6c9c9393178f07ce55759f07d9378057f))
* update runs ([2bbfcb8](https://github.com/nominal-io/nominal-client/commit/2bbfcb8bfef528e9ae5e064a71893f363bde1ca7))
* use poetry ([8b8b924](https://github.com/nominal-io/nominal-client/commit/8b8b92400eded6478b2c4043f996bcdbcef3eafc))


### Bug Fixes

* add isort linting and instructions for configuration in vscode ([3f348c5](https://github.com/nominal-io/nominal-client/commit/3f348c53a7da4467abeadb4d3a68048d80917f96))
* reformat ([caac5f1](https://github.com/nominal-io/nominal-client/commit/caac5f16725437cb7de3b60bcd3c5365eb9d83c2))
* respect run properties ([927b50d](https://github.com/nominal-io/nominal-client/commit/927b50d708450087b384706e8bfa7674f6f8f7e2))


### Miscellaneous Chores

* release 0.5.0 ([348c00a](https://github.com/nominal-io/nominal-client/commit/348c00a64ca5df63ff7ab24287233ebb8956932b))
