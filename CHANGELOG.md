# Changelog

## [1.12.0](https://github.com/nominal-io/nominal-client/compare/v1.11.0...v1.12.0) (2024-11-21)


### Features

* add content to Asset ([#125](https://github.com/nominal-io/nominal-client/issues/125)) ([de09dee](https://github.com/nominal-io/nominal-client/commit/de09deee1fa29cedf2bedbddcf813f0e3e1cf1f0))
* add enqueue_batch to NominalWriteStream ([#138](https://github.com/nominal-io/nominal-client/issues/138)) ([447bfe9](https://github.com/nominal-io/nominal-client/commit/447bfe95c8f97d5cac79a8e89c4f2c80d3f4c700))
* allow users to interact with videos and runs ([#129](https://github.com/nominal-io/nominal-client/issues/129)) ([5b43856](https://github.com/nominal-io/nominal-client/commit/5b43856895c48581c2112c8f847b31c6d0baca03))
* expose listing connections and logsets from a run object ([#128](https://github.com/nominal-io/nominal-client/issues/128)) ([14d4c36](https://github.com/nominal-io/nominal-client/commit/14d4c36d21140a8ff1f967c06b1f0e6c36b9fb2e))
* make polling for io completion update dataset metadata ([#133](https://github.com/nominal-io/nominal-client/issues/133)) ([ac19e54](https://github.com/nominal-io/nominal-client/commit/ac19e54a0de072053af2f39233881a7cd757c47b))
* propagate requests.Session with cert file ([#139](https://github.com/nominal-io/nominal-client/issues/139)) ([efdf843](https://github.com/nominal-io/nominal-client/commit/efdf843065602f70fb615038e759a2a524439361))


### Bug Fixes

* create_logs_set function is missing on nominal ([#140](https://github.com/nominal-io/nominal-client/issues/140)) ([5c7fbcb](https://github.com/nominal-io/nominal-client/commit/5c7fbcb69bd78c4673e38f270a1e154848e0a6df))

## [1.11.0](https://github.com/nominal-io/nominal-client/compare/v1.10.0...v1.11.0) (2024-11-13)


### Features

* add connections to run ([#123](https://github.com/nominal-io/nominal-client/issues/123)) ([07ea6f3](https://github.com/nominal-io/nominal-client/commit/07ea6f3df8f00ed1c5ba274fbad251039aeb384b))
* add create streaming connection ([#119](https://github.com/nominal-io/nominal-client/issues/119)) ([36b64e4](https://github.com/nominal-io/nominal-client/commit/36b64e454013f30a18f1900603ba5be74ea83414))
* add create/get asset ([#120](https://github.com/nominal-io/nominal-client/issues/120)) ([181f459](https://github.com/nominal-io/nominal-client/commit/181f459fc10690d639f87ce1dfdd2cb81f689b5b))
* add function to create workbook from template ([#122](https://github.com/nominal-io/nominal-client/issues/122)) ([27f6d46](https://github.com/nominal-io/nominal-client/commit/27f6d46a36c88667453da43294568e9377ca847c))
* add method to get decimated data to Channel ([#118](https://github.com/nominal-io/nominal-client/issues/118)) ([c2c9ff5](https://github.com/nominal-io/nominal-client/commit/c2c9ff535160662d68f832cd4d5db69a5c569d0f))
* allow archiving / unarchiving videos ([#117](https://github.com/nominal-io/nominal-client/issues/117)) ([76d7028](https://github.com/nominal-io/nominal-client/commit/76d70289f31c4aca9b4d0026f78e79a41405da38))
* **docs:** slate-muted color scheme ([#115](https://github.com/nominal-io/nominal-client/issues/115)) ([7cec7f5](https://github.com/nominal-io/nominal-client/commit/7cec7f55c304aec628ecf03ec5c7a298ea2779d6))
* hdf5 extra ([#94](https://github.com/nominal-io/nominal-client/issues/94)) ([b21674e](https://github.com/nominal-io/nominal-client/commit/b21674e05aa3bbde5a0f7f8d6a26c3fd96ebadef))
* remove data sources from run ([#124](https://github.com/nominal-io/nominal-client/issues/124)) ([b44078a](https://github.com/nominal-io/nominal-client/commit/b44078a12a8dba10c0e98c8ad8bf01de191b0be4))
* write stream ([#106](https://github.com/nominal-io/nominal-client/issues/106)) ([952f944](https://github.com/nominal-io/nominal-client/commit/952f9441da11080246167b2fe2bd4bef372513f7))

## [1.10.0](https://github.com/nominal-io/nominal-client/compare/v1.9.0...v1.10.0) (2024-10-30)


### Features

* add bounds to dataset class ([#110](https://github.com/nominal-io/nominal-client/issues/110)) ([f111bca](https://github.com/nominal-io/nominal-client/commit/f111bca5f71aa60e8f1514dfef077574ee2968fa))
* add nominal_url property to dataset ([#111](https://github.com/nominal-io/nominal-client/issues/111)) ([28a3b5a](https://github.com/nominal-io/nominal-client/commit/28a3b5a2e4fec2505464cf12482cb3e172332695))
* add run number to nominal Run class ([#109](https://github.com/nominal-io/nominal-client/issues/109)) ([26fc1bb](https://github.com/nominal-io/nominal-client/commit/26fc1bb2f5a413469083a99aaac631f899b6c3d7))
* allow end=None when creating runs for streaming ([#114](https://github.com/nominal-io/nominal-client/issues/114)) ([e89d9bc](https://github.com/nominal-io/nominal-client/commit/e89d9bc5f175c08f649d076d9edfec2a12c7e69c))
* ingest video from MCAP file ([#105](https://github.com/nominal-io/nominal-client/issues/105)) ([75e7011](https://github.com/nominal-io/nominal-client/commit/75e7011eb1a3006b447c069d6dd12c9c9b893882))

## [1.9.0](https://github.com/nominal-io/nominal-client/compare/v1.8.0...v1.9.0) (2024-10-23)


### Features

* add Channel and Connection to top level ([#100](https://github.com/nominal-io/nominal-client/issues/100)) ([353e5f7](https://github.com/nominal-io/nominal-client/commit/353e5f7fa95e1ba0df820c3def904d147d7c40d8))
* expose index_channel_prefix_tree for datasets ([#102](https://github.com/nominal-io/nominal-client/issues/102)) ([bd963ad](https://github.com/nominal-io/nominal-client/commit/bd963ad9b0d96cc1ca3c646b03028d4185ee2415))


### Bug Fixes

* value in enum classes check for python &lt; 3.12 ([#104](https://github.com/nominal-io/nominal-client/issues/104)) ([2fc4d8f](https://github.com/nominal-io/nominal-client/commit/2fc4d8fd514a236fd00284d717987ae11a57de28))

## [1.8.0](https://github.com/nominal-io/nominal-client/compare/v1.7.1...v1.8.0) (2024-10-21)


### Features

* add connections ([#92](https://github.com/nominal-io/nominal-client/issues/92)) ([a8940b0](https://github.com/nominal-io/nominal-client/commit/a8940b0aabc66b5052198259cb01ac6bcb664196))
* download dataset ([#93](https://github.com/nominal-io/nominal-client/issues/93)) ([76f8748](https://github.com/nominal-io/nominal-client/commit/76f8748b216041f5cef03b773ff3e1f2a6be708b))
* hide archetype series from channel abstraction ([#91](https://github.com/nominal-io/nominal-client/issues/91)) ([37f768b](https://github.com/nominal-io/nominal-client/commit/37f768b2ef1a39eead1c5ea5117211f8f8ac09e9))
* validate login flow for new users ([#82](https://github.com/nominal-io/nominal-client/issues/82)) ([930367a](https://github.com/nominal-io/nominal-client/commit/930367aa7de5a9169a873e2cb2844c5de2096902))
* wrap various unit types within the Conjure spec within a single Unit abstraction ([#85](https://github.com/nominal-io/nominal-client/issues/85)) ([ff6f015](https://github.com/nominal-io/nominal-client/commit/ff6f015bfba7a95617985763df1df4871eb8baee))


### Bug Fixes

* early return in set_units, add coverage + unit tests ([#97](https://github.com/nominal-io/nominal-client/issues/97)) ([874e8dc](https://github.com/nominal-io/nominal-client/commit/874e8dc7b4ae971abdb5d0fe828531d7a692646d))

## [1.7.1](https://github.com/nominal-io/nominal-client/compare/v1.7.0...v1.7.1) (2024-10-16)


### Bug Fixes

* remove subscript type specification for inheritance, not supported in &lt;=3.10 ([#86](https://github.com/nominal-io/nominal-client/issues/86)) ([10e7304](https://github.com/nominal-io/nominal-client/commit/10e73044ce555447526f8c10687c258b6b9872de))

## [1.7.0](https://github.com/nominal-io/nominal-client/compare/v1.6.1...v1.7.0) (2024-10-15)


### Features

* add CLI endpoint to summarize an existing dataset ([#70](https://github.com/nominal-io/nominal-client/issues/70)) ([4ebb203](https://github.com/nominal-io/nominal-client/commit/4ebb20384c5c4e845cf3a0a8067cdb4dacdf9f54))
* expose set_token ([#84](https://github.com/nominal-io/nominal-client/issues/84)) ([9b12674](https://github.com/nominal-io/nominal-client/commit/9b126740e4e5c8a5a26a6901d16f0ca0a36d1fb5))
* get channel data as a pandas series ([#81](https://github.com/nominal-io/nominal-client/issues/81)) ([e38d4b0](https://github.com/nominal-io/nominal-client/commit/e38d4b05773e49c5925ac3c7020f0826855b3603))
* implement global options / decorators for client + debug + logging within CLI ([#71](https://github.com/nominal-io/nominal-client/issues/71)) ([1424957](https://github.com/nominal-io/nominal-client/commit/1424957f92563d9277d89c675e70219d67532589))
* native tdms support ([#80](https://github.com/nominal-io/nominal-client/issues/80)) ([867cd1b](https://github.com/nominal-io/nominal-client/commit/867cd1b80ce9cf215b4c15a8ab7cf88991807cec))


### Bug Fixes

* revert to prior release please config ([#78](https://github.com/nominal-io/nominal-client/issues/78)) ([48fac13](https://github.com/nominal-io/nominal-client/commit/48fac131dc7dd360965aa0108fa7fb31b26c4845))

## [1.6.1](https://github.com/nominal-io/nominal-client/compare/v1.6.0...v1.6.1) (2024-10-09)


### Chores

* add package-name to config ([#76](https://github.com/nominal-io/nominal-client/issues/76)) ([40c616a](https://github.com/nominal-io/nominal-client/commit/40c616ae9ec079e985d13c4fbd54a2792b9b193d))

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
