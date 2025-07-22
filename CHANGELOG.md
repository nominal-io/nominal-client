# Changelog

## [1.68.0](https://github.com/nominal-io/nominal-client/compare/v1.67.0...v1.68.0) (2025-07-22)


### Features

* allow archiving events ([#415](https://github.com/nominal-io/nominal-client/issues/415)) ([ef816e4](https://github.com/nominal-io/nominal-client/commit/ef816e4df87a39fbb0dedb92cf5322bf153e0a5d))

## [1.67.0](https://github.com/nominal-io/nominal-client/compare/v1.66.0...v1.67.0) (2025-07-18)


### Features

* allow downloading dataset files ([#411](https://github.com/nominal-io/nominal-client/issues/411)) ([c80c95a](https://github.com/nominal-io/nominal-client/commit/c80c95aaee2bf5e692a339e7151fcc5c3c3c2b89))
* allow searching datasets ([#413](https://github.com/nominal-io/nominal-client/issues/413)) ([f854c49](https://github.com/nominal-io/nominal-client/commit/f854c498dcd3fe70aeaa0fd93a7105ebba78e5b8))

## [1.66.0](https://github.com/nominal-io/nominal-client/compare/v1.65.0...v1.66.0) (2025-07-14)


### Features

* add nominal log handler for pythonically uploading logs to nominal ([#410](https://github.com/nominal-io/nominal-client/issues/410)) ([aee908d](https://github.com/nominal-io/nominal-client/commit/aee908d274964c1774fceb3e6234a97bb046f844))
* export logs from log channels, and allow searching for channels of a given type(s) ([#405](https://github.com/nominal-io/nominal-client/issues/405)) ([877c736](https://github.com/nominal-io/nominal-client/commit/877c7365dc64aa4ef867e9b074eefbc970b5b3cf))
* expose workbooks and workbook templates more fully ([#407](https://github.com/nominal-io/nominal-client/issues/407)) ([6bfb75a](https://github.com/nominal-io/nominal-client/commit/6bfb75a1d3660edd3cab16e15ab813a267575f50))


### Bug Fixes

* pagination in search users would infinitely loop with sufficient user count ([78ae4b9](https://github.com/nominal-io/nominal-client/commit/78ae4b905bf675599bad715793d031502bf5d877))

## [1.65.0](https://github.com/nominal-io/nominal-client/compare/v1.64.0...v1.65.0) (2025-07-03)


### Features

* add description and rid to events ([#402](https://github.com/nominal-io/nominal-client/issues/402)) ([7c138fb](https://github.com/nominal-io/nominal-client/commit/7c138fbc442ed5be64bc6900079d2a9559a4423c))
* clean up DataReview API usage ([#406](https://github.com/nominal-io/nominal-client/issues/406)) ([4574297](https://github.com/nominal-io/nominal-client/commit/45742978068f20e6bc628104791933937a224966))
* delete deprecated channel methods ([#403](https://github.com/nominal-io/nominal-client/issues/403)) ([dd4cff9](https://github.com/nominal-io/nominal-client/commit/dd4cff99f2293a7d0a4cf012493f434c3fd8a1d4))

## [1.64.0](https://github.com/nominal-io/nominal-client/compare/v1.63.0...v1.64.0) (2025-06-30)


### Features

* allow streaming ints with vanilla streaming ([#396](https://github.com/nominal-io/nominal-client/issues/396)) ([fdb6861](https://github.com/nominal-io/nominal-client/commit/fdb6861e9b53047849a665e9deb84d7089d2fc66))
* allow streaming intsn with vanilla streaming ([fdb6861](https://github.com/nominal-io/nominal-client/commit/fdb6861e9b53047849a665e9deb84d7089d2fc66))
* consolidate utilities in _utils subpackages, add timing utilities ([#399](https://github.com/nominal-io/nominal-client/issues/399)) ([4173af4](https://github.com/nominal-io/nominal-client/commit/4173af4ad16fdfa27444dfcfbe87f827b91acfb4))
* delete methods that have been deprecated for several months ([#400](https://github.com/nominal-io/nominal-client/issues/400)) ([97ce525](https://github.com/nominal-io/nominal-client/commit/97ce52592fc6182ff2211357b95629e6bcff457d))

## [1.63.0](https://github.com/nominal-io/nominal-client/compare/v1.62.0...v1.63.0) (2025-06-25)


### Features

* allow exporting a single channels data with gzip ([#395](https://github.com/nominal-io/nominal-client/issues/395)) ([182a952](https://github.com/nominal-io/nominal-client/commit/182a952e184ba907c9cdf3879309513101482943))
* deprecate all top level functions in nominal.py ([#391](https://github.com/nominal-io/nominal-client/issues/391)) ([91c3a51](https://github.com/nominal-io/nominal-client/commit/91c3a510114acf676d0e33f2a0735904ec07edbf))

## [1.62.0](https://github.com/nominal-io/nominal-client/compare/v1.61.0...v1.62.0) (2025-06-24)


### Features

* compose app base url from clients bunch ([#392](https://github.com/nominal-io/nominal-client/issues/392)) ([3f9b722](https://github.com/nominal-io/nominal-client/commit/3f9b722a0d898b827421a06e2201f8d592c48127))

## [1.61.0](https://github.com/nominal-io/nominal-client/compare/v1.60.0...v1.61.0) (2025-06-11)


### Features

* allow rescaling videos when normalizing, stop normalizing audio ([#388](https://github.com/nominal-io/nominal-client/issues/388)) ([8051b2a](https://github.com/nominal-io/nominal-client/commit/8051b2aa8314646c705b14bd9c7c56f842788238))
* expose searching users ([#384](https://github.com/nominal-io/nominal-client/issues/384)) ([a745a12](https://github.com/nominal-io/nominal-client/commit/a745a123aa31a146fbb571e2437ecd15b2371074))
* standardize TypeAlias on Union over string ([82730a8](https://github.com/nominal-io/nominal-client/commit/82730a8844b0cd47a41725cfac80879aeec32118))


### Bug Fixes

* standardize TypeAlias on Union over string ([#389](https://github.com/nominal-io/nominal-client/issues/389)) ([82730a8](https://github.com/nominal-io/nominal-client/commit/82730a8844b0cd47a41725cfac80879aeec32118))
* timestamps being returned as strings, and with wrong column header ([#386](https://github.com/nominal-io/nominal-client/issues/386)) ([3a3a91b](https://github.com/nominal-io/nominal-client/commit/3a3a91b033ea3307f244c3dfbcc301d437587d31))

## [1.60.0](https://github.com/nominal-io/nominal-client/compare/v1.59.0...v1.60.0) (2025-06-10)


### Features

* allow adding datasources to runs and assets with tags and offsets ([#383](https://github.com/nominal-io/nominal-client/issues/383)) ([e1349e1](https://github.com/nominal-io/nominal-client/commit/e1349e1a921a44e38d6d580f370b5a3d79bb234d))
* allow users to upload tdms files to an existing dataset ([#374](https://github.com/nominal-io/nominal-client/issues/374)) ([85961c0](https://github.com/nominal-io/nominal-client/commit/85961c0472087748ee15540beeefac68f1dd6a13))


### Bug Fixes

* remove h5 dep ([4469947](https://github.com/nominal-io/nominal-client/commit/4469947add564b22dd8f3abee5b2bd882c91ae1e))
* remove hdf5 dependency group ([#380](https://github.com/nominal-io/nominal-client/issues/380)) ([4469947](https://github.com/nominal-io/nominal-client/commit/4469947add564b22dd8f3abee5b2bd882c91ae1e))


### Documentation

* add experimental modules to API reference ([#338](https://github.com/nominal-io/nominal-client/issues/338)) ([b4b280d](https://github.com/nominal-io/nominal-client/commit/b4b280d9ea48956122c1dc68309ee61a755c30b9))

## [1.59.0](https://github.com/nominal-io/nominal-client/compare/v1.58.0...v1.59.0) (2025-06-06)


### Features

* allow creating an attachment by filepath ([#376](https://github.com/nominal-io/nominal-client/issues/376)) ([28b2e24](https://github.com/nominal-io/nominal-client/commit/28b2e241b2748bd42be04ae1b38ea62937f2549c))
* allow exporting data in relative format ([#378](https://github.com/nominal-io/nominal-client/issues/378)) ([1eede44](https://github.com/nominal-io/nominal-client/commit/1eede4471945d16c5da4f6a64c605f7ff4903b4a))
* allow uploading a pandas dataframe to an existing dataset ([#372](https://github.com/nominal-io/nominal-client/issues/372)) ([263d097](https://github.com/nominal-io/nominal-client/commit/263d097393cc1e633c5dbbcbeb3b39e9804626c6))
* allow users to specify file-wide tag during upload ([#377](https://github.com/nominal-io/nominal-client/issues/377)) ([8dddc94](https://github.com/nominal-io/nominal-client/commit/8dddc944fd43d0e8f3219ac415aae69d95ac6483))
* parallelize exporting data to dataframes ([#373](https://github.com/nominal-io/nominal-client/issues/373)) ([ea8f7aa](https://github.com/nominal-io/nominal-client/commit/ea8f7aa4034363cc9c25e1df14e325b53a4c7acb))


### Bug Fixes

* make coverage a dev dep ([#379](https://github.com/nominal-io/nominal-client/issues/379)) ([3d3b8f5](https://github.com/nominal-io/nominal-client/commit/3d3b8f5cb91271728cd93e42275a83fc76494b86))

## [1.58.0](https://github.com/nominal-io/nominal-client/compare/v1.57.1...v1.58.0) (2025-06-06)


### Features

* allow gzipping during pandas export ([#369](https://github.com/nominal-io/nominal-client/issues/369)) ([2d80044](https://github.com/nominal-io/nominal-client/commit/2d800444eb68777817aad791451dc0d54684ca8a))
* bump nominal-api and nominal-api-protos ([#370](https://github.com/nominal-io/nominal-client/issues/370)) ([044ada0](https://github.com/nominal-io/nominal-client/commit/044ada05526c47674dcd90dacf2a00b3bfdabbdc))

## [1.57.1](https://github.com/nominal-io/nominal-client/compare/v1.57.0...v1.57.1) (2025-06-05)


### Bug Fixes

* make dataframe actually use timestamps as the index column in datasource_to_dataframe ([#367](https://github.com/nominal-io/nominal-client/issues/367)) ([cca1748](https://github.com/nominal-io/nominal-client/commit/cca17489ea0696f6332c3515d88643b577d48298))

## [1.57.0](https://github.com/nominal-io/nominal-client/compare/v1.56.0...v1.57.0) (2025-06-04)


### Features

* bump nominal-api-protos version when installing nominal[protos] ([#365](https://github.com/nominal-io/nominal-client/issues/365)) ([558d460](https://github.com/nominal-io/nominal-client/commit/558d460c264c1ca008102814d0cb1f052231b3a2))

## [1.56.0](https://github.com/nominal-io/nominal-client/compare/v1.55.0...v1.56.0) (2025-06-04)


### Features

* allow creating clients by profile in cli decorators ([#364](https://github.com/nominal-io/nominal-client/issues/364)) ([41dcffb](https://github.com/nominal-io/nominal-client/commit/41dcffb97b99d4d4b51113bac1e027be023231c0))
* simplify/cleanup dependencies ([#361](https://github.com/nominal-io/nominal-client/issues/361)) ([9a017b0](https://github.com/nominal-io/nominal-client/commit/9a017b06ad2d9ad484a36a952bd5459979601e67))


### Bug Fixes

* conjure_python_client 2.9.0+ was broken on some windows machines ([#363](https://github.com/nominal-io/nominal-client/issues/363)) ([b983738](https://github.com/nominal-io/nominal-client/commit/b983738c3c5c5bbb622ba9eb7f6e312ae00e5971))

## [1.55.0](https://github.com/nominal-io/nominal-client/compare/v1.54.1...v1.55.0) (2025-06-02)


### Features

* add get or create dataset by refname on assets ([#350](https://github.com/nominal-io/nominal-client/issues/350)) ([e99f52c](https://github.com/nominal-io/nominal-client/commit/e99f52c56ce2cc70db62c8e2fbe7ce27a9f7b665))
* bump nominal-api to 0.703.0 ([#359](https://github.com/nominal-io/nominal-client/issues/359)) ([734a6f0](https://github.com/nominal-io/nominal-client/commit/734a6f0c28ce6cc7fd9f64e16997b0c2f5828f95))
* clean up video api ([#340](https://github.com/nominal-io/nominal-client/issues/340)) ([fabf837](https://github.com/nominal-io/nominal-client/commit/fabf83728bda8c4f41f243a9a6f57b15b95319cd))
* dont track metrics by default ([#360](https://github.com/nominal-io/nominal-client/issues/360)) ([0e12385](https://github.com/nominal-io/nominal-client/commit/0e12385482bb2b13a7acbf939d9069b0f2b3a56a))
* make gzip level a global constant ([#357](https://github.com/nominal-io/nominal-client/issues/357)) ([5b024aa](https://github.com/nominal-io/nominal-client/commit/5b024aa6e6ad29099ef5da97f2954e883a1b3a3c))


### Bug Fixes

* mcap extension checking for video upload ([#356](https://github.com/nominal-io/nominal-client/issues/356)) ([30e4fa4](https://github.com/nominal-io/nominal-client/commit/30e4fa4311b1326ff109a95549b9819f1033b701))


### Documentation

* make `LogPoint` appear in the reference docs ([#347](https://github.com/nominal-io/nominal-client/issues/347)) ([50156ef](https://github.com/nominal-io/nominal-client/commit/50156ef5fca4a7c5f870f4b64c8f623135efdbde))

## [1.54.1](https://github.com/nominal-io/nominal-client/compare/v1.54.0...v1.54.1) (2025-05-29)


### Bug Fixes

* correctly check parquet/csv types for tabular ingest ([#351](https://github.com/nominal-io/nominal-client/issues/351)) ([51967f1](https://github.com/nominal-io/nominal-client/commit/51967f15716be63e94c2d1ec7bb5627c232a6ecb))
* pass workspace rid when creating empty video ([#354](https://github.com/nominal-io/nominal-client/issues/354)) ([1f42e11](https://github.com/nominal-io/nominal-client/commit/1f42e11baddb43743dba2636f064ea6e30f055a3))
* reduce default key frame interval to 2s ([#352](https://github.com/nominal-io/nominal-client/issues/352)) ([a3c7f7c](https://github.com/nominal-io/nominal-client/commit/a3c7f7c7be76f4292f1c901df6c97e6a5d2a7590))

## [1.54.0](https://github.com/nominal-io/nominal-client/compare/v1.53.0...v1.54.0) (2025-05-29)


### Features

* retry read and connection errors ([#349](https://github.com/nominal-io/nominal-client/issues/349)) ([09a6768](https://github.com/nominal-io/nominal-client/commit/09a67685d2a0ba3252f3a86ebc25006b789fa399))
* un-deprecate creating streaming connections ([#345](https://github.com/nominal-io/nominal-client/issues/345)) ([466b588](https://github.com/nominal-io/nominal-client/commit/466b588ecd7dfe6ac77c60baf75faa572fae3940))

## [1.53.0](https://github.com/nominal-io/nominal-client/compare/v1.52.0...v1.53.0) (2025-05-27)


### Features

* bump version to fix workbook templates ([#346](https://github.com/nominal-io/nominal-client/issues/346)) ([99500a9](https://github.com/nominal-io/nominal-client/commit/99500a9e50353ffa7ca2552fd8e88b64424e6469))
* provide profile-based NominalClient configuration ([#211](https://github.com/nominal-io/nominal-client/issues/211)) ([cd93d9e](https://github.com/nominal-io/nominal-client/commit/cd93d9ed6a3fb73af26d8dc1ac86329859f6c5cb))
* refactor client: move pagination & query factories to conjure utils ([#333](https://github.com/nominal-io/nominal-client/issues/333)) ([12e1ff5](https://github.com/nominal-io/nominal-client/commit/12e1ff55877111de10d2a00ee2a678331196268f))


### Bug Fixes

* video conversion when there is no audio ([#339](https://github.com/nominal-io/nominal-client/issues/339)) ([5c527f3](https://github.com/nominal-io/nominal-client/commit/5c527f379fe5e6d35e4ee3f79f2a6c382705c7f0))

## [1.52.0](https://github.com/nominal-io/nominal-client/compare/v1.51.0...v1.52.0) (2025-05-20)


### Features

* gzip post contents by default ([#331](https://github.com/nominal-io/nominal-client/issues/331)) ([b45fb2b](https://github.com/nominal-io/nominal-client/commit/b45fb2b830b7bd21d322f5d5ae133dbb696fc77d))


### Bug Fixes

* print extension, instead of `FileType` tuple, in error message ([#337](https://github.com/nominal-io/nominal-client/issues/337)) ([d47f8f3](https://github.com/nominal-io/nominal-client/commit/d47f8f36355097ed9420b1ffca126de0ceaf01f6))
* remove warning on uploading CSV ([#336](https://github.com/nominal-io/nominal-client/issues/336)) ([744c371](https://github.com/nominal-io/nominal-client/commit/744c3714f39388f27522ff6a87a0e7422b2112c2))


### Documentation

* organize reference docs around namespace packages ([#334](https://github.com/nominal-io/nominal-client/issues/334)) ([310b0a1](https://github.com/nominal-io/nominal-client/commit/310b0a1f16454333d14f9a3f6fe1d293ba5678e7))

## [1.51.0](https://github.com/nominal-io/nominal-client/compare/v1.50.0...v1.51.0) (2025-05-12)


### Features

* add nicer dataset addition methods ([#318](https://github.com/nominal-io/nominal-client/issues/318)) ([d00e78f](https://github.com/nominal-io/nominal-client/commit/d00e78fabcc5832efc8b00b65b72b495b60d0aff))
* create, get, search events ([#236](https://github.com/nominal-io/nominal-client/issues/236)) ([5acc9c5](https://github.com/nominal-io/nominal-client/commit/5acc9c529c4ffdf4b3858e6b05c5891ccc20ac2f))


### Bug Fixes

* fix function argument overwrite (propagate exception) ([#320](https://github.com/nominal-io/nominal-client/issues/320)) ([6f4b9f9](https://github.com/nominal-io/nominal-client/commit/6f4b9f9b6913705ee2f00180eb4cecc62e7f32ec))
* fix incompatible type assignments ([#322](https://github.com/nominal-io/nominal-client/issues/322)) ([c54ef1f](https://github.com/nominal-io/nominal-client/commit/c54ef1fac24a9d43da0557382d199b4e9f461291))

## [1.50.0](https://github.com/nominal-io/nominal-client/compare/v1.49.0...v1.50.0) (2025-04-28)


### Features

* allow parquet and parquet archive ingest ([#317](https://github.com/nominal-io/nominal-client/issues/317)) ([c91ad06](https://github.com/nominal-io/nominal-client/commit/c91ad06fefead70e1212c23d204d7fc30e831b92))
* Expose nominal secrets via client ([98eb1bf](https://github.com/nominal-io/nominal-client/commit/98eb1bf2245c3fbc213f3ee50f2ec79cb0bf76d5))
* expose org-level secrets via client ([#323](https://github.com/nominal-io/nominal-client/issues/323)) ([98eb1bf](https://github.com/nominal-io/nominal-client/commit/98eb1bf2245c3fbc213f3ee50f2ec79cb0bf76d5))

## [1.49.0](https://github.com/nominal-io/nominal-client/compare/v1.48.0...v1.49.0) (2025-04-22)


### Features

* allow creating empty videos ([#311](https://github.com/nominal-io/nominal-client/issues/311)) ([f5e63a1](https://github.com/nominal-io/nominal-client/commit/f5e63a1a476ad6a01e7f50c5e22a4d01bde3d0e0))
* allow listing all ingested files on a dataset ([#316](https://github.com/nominal-io/nominal-client/issues/316)) ([a8f234f](https://github.com/nominal-io/nominal-client/commit/a8f234f5558c63c8efde9fec64b13066b669a2cf))
* tag columns mapping ([#315](https://github.com/nominal-io/nominal-client/issues/315)) ([12b1a18](https://github.com/nominal-io/nominal-client/commit/12b1a18da8ef6a85b1b9f53b87994b570d5d682d))

## [1.48.0](https://github.com/nominal-io/nominal-client/compare/v1.47.1...v1.48.0) (2025-04-17)


### Features

* expose workspaces to client ([#308](https://github.com/nominal-io/nominal-client/issues/308)) ([93549e7](https://github.com/nominal-io/nominal-client/commit/93549e7e9c89dde4b54e69136b51d720a57a896d))

## [1.47.1](https://github.com/nominal-io/nominal-client/compare/v1.47.0...v1.47.1) (2025-04-15)


### Bug Fixes

* tie to a specific version of nominal-api ([#306](https://github.com/nominal-io/nominal-client/issues/306)) ([58f1dee](https://github.com/nominal-io/nominal-client/commit/58f1deea9a167b21355b5de0a49bcd25b74e1a49))

## [1.47.0](https://github.com/nominal-io/nominal-client/compare/v1.46.0...v1.47.0) (2025-04-10)


### Features

* add log streaming, deprecate LogSets ([#300](https://github.com/nominal-io/nominal-client/issues/300)) ([8a95f1c](https://github.com/nominal-io/nominal-client/commit/8a95f1c589fb498de8f1e1abb585de979829cb73))
* add workspace rids ([#299](https://github.com/nominal-io/nominal-client/issues/299)) ([08766ac](https://github.com/nominal-io/nominal-client/commit/08766acba58df4e6bc602dbeed33747c11e8d012))
* enqueue from dict tags ([#304](https://github.com/nominal-io/nominal-client/issues/304)) ([b7189f6](https://github.com/nominal-io/nominal-client/commit/b7189f6fc640a52030fa2401f0c1ea6805b72545))
* expose tags to enqueue_from_dict ([#302](https://github.com/nominal-io/nominal-client/issues/302)) ([42792d8](https://github.com/nominal-io/nominal-client/commit/42792d874a49c74c0961ab95c65d11564dba53dc))

## [1.46.0](https://github.com/nominal-io/nominal-client/compare/v1.45.2...v1.46.0) (2025-04-08)


### Features

* add deprecation notices to consolidate dataset creation / addition methods ([#292](https://github.com/nominal-io/nominal-client/issues/292)) ([13944b1](https://github.com/nominal-io/nominal-client/commit/13944b129b1ed8d0a9b9fc5b4cd6a8307b6d498d))
* allow datasources to create write streams ([#296](https://github.com/nominal-io/nominal-client/issues/296)) ([5314e15](https://github.com/nominal-io/nominal-client/commit/5314e15f3a1e886e359b226b24c9f594dc71cf6f))
* allow users to optionally be warned when setting display-only units ([#284](https://github.com/nominal-io/nominal-client/issues/284)) ([7e0ed0d](https://github.com/nominal-io/nominal-client/commit/7e0ed0df0a09830cfd844985095aed75a594ac46))
* document that tabular data methods accept parquets ([#289](https://github.com/nominal-io/nominal-client/issues/289)) ([561845b](https://github.com/nominal-io/nominal-client/commit/561845bb482493ead34e05035ba10f99d4e40db0))
* plumb column tag keys ([#294](https://github.com/nominal-io/nominal-client/issues/294)) ([58a20a4](https://github.com/nominal-io/nominal-client/commit/58a20a457f11e0a1f9711c5ee7b2d4c22d70939e))
* remove 3mo+ deprecations ([#295](https://github.com/nominal-io/nominal-client/issues/295)) ([18b07f0](https://github.com/nominal-io/nominal-client/commit/18b07f068dfbe52d1d9d7dee719e51408960d591))
* support experimental streaming, handle latent streaming feedback ([#298](https://github.com/nominal-io/nominal-client/issues/298)) ([de49d9d](https://github.com/nominal-io/nominal-client/commit/de49d9db31b11df2debb784d4de7c49f44486db0))
* update nominal-api to 0.618.0 ([#297](https://github.com/nominal-io/nominal-client/issues/297)) ([2afd31d](https://github.com/nominal-io/nominal-client/commit/2afd31dc9abac0adcde677e7e524aa62dcb43a27))


### Bug Fixes

* use nano level precision when getting dataset files from api ([#283](https://github.com/nominal-io/nominal-client/issues/283)) ([26531f1](https://github.com/nominal-io/nominal-client/commit/26531f109e93e471aa101171526d98f65a1e41db))

## [1.45.2](https://github.com/nominal-io/nominal-client/compare/v1.45.1...v1.45.2) (2025-04-01)


### Bug Fixes

* fix nondeterminism in asset list_datasets ([#287](https://github.com/nominal-io/nominal-client/issues/287)) ([79a4eb0](https://github.com/nominal-io/nominal-client/commit/79a4eb00de36b62a6c01578e9711adbf56b89047))

## [1.45.1](https://github.com/nominal-io/nominal-client/compare/v1.45.0...v1.45.1) (2025-04-01)


### Bug Fixes

* poll until ingestion completed exits if status is error ([#285](https://github.com/nominal-io/nominal-client/issues/285)) ([d656bd0](https://github.com/nominal-io/nominal-client/commit/d656bd031132727855c814900209b394b89e121d))

## [1.45.0](https://github.com/nominal-io/nominal-client/compare/v1.44.0...v1.45.0) (2025-03-28)


### Features

* allow listing dataset files ([#281](https://github.com/nominal-io/nominal-client/issues/281)) ([500d301](https://github.com/nominal-io/nominal-client/commit/500d301128f2e7c313303351920ea7a2f2380b91))

## [1.44.0](https://github.com/nominal-io/nominal-client/compare/v1.43.0...v1.44.0) (2025-03-27)


### Features

* add data from mcap io ([#279](https://github.com/nominal-io/nominal-client/issues/279)) ([9e7df67](https://github.com/nominal-io/nominal-client/commit/9e7df675b05cb284793323e3e984f2e5f63c4007))
* create thirdparty subpackage and deprecate existing usage of third party code ([#276](https://github.com/nominal-io/nominal-client/issues/276)) ([de6b5fd](https://github.com/nominal-io/nominal-client/commit/de6b5fdb505c99bd4b8fde8dccba95d853f895b8))

## [1.43.0](https://github.com/nominal-io/nominal-client/compare/v1.42.0...v1.43.0) (2025-03-25)


### Features

* allow custom derived units for channels ([#272](https://github.com/nominal-io/nominal-client/issues/272)) ([f6a1edf](https://github.com/nominal-io/nominal-client/commit/f6a1edf2029ee69e3865a7085480bd81d457e615))
* prevent duplicated file extensions when using upload_multipart_io ([#274](https://github.com/nominal-io/nominal-client/issues/274)) ([3d5f349](https://github.com/nominal-io/nominal-client/commit/3d5f349e9c02dc7796b823a48295722dc59ca81e))

## [1.42.0](https://github.com/nominal-io/nominal-client/compare/v1.41.0...v1.42.0) (2025-03-21)


### Features

* allow multi-file journal json logs ([#270](https://github.com/nominal-io/nominal-client/issues/270)) ([67c98e5](https://github.com/nominal-io/nominal-client/commit/67c98e56c6638f9a8287adc0b8c4a080570f9513))
* use filename for new files when adding data to dataset for csv/parquet ([#273](https://github.com/nominal-io/nominal-client/issues/273)) ([777f7e1](https://github.com/nominal-io/nominal-client/commit/777f7e15e78b8a670b22b202d3fe7a479e554bd8))

## [1.41.0](https://github.com/nominal-io/nominal-client/compare/v1.40.1...v1.41.0) (2025-03-18)


### Features

* add functionality to add a video to an asset ([#267](https://github.com/nominal-io/nominal-client/issues/267)) ([d8c2871](https://github.com/nominal-io/nominal-client/commit/d8c287151c563a9cac885bd245646c69545214ef))
* expose channel update() method ([#234](https://github.com/nominal-io/nominal-client/issues/234)) ([8c0dcdc](https://github.com/nominal-io/nominal-client/commit/8c0dcdc478b3d716b5ff89a4fa9c2cac0fe204b1))
* expose searching for checklists and data reviews ([#259](https://github.com/nominal-io/nominal-client/issues/259)) ([fc7e69c](https://github.com/nominal-io/nominal-client/commit/fc7e69c9c03d68005c0552ce78b20079f44ca4dd))
* make ffmpeg-python a standard dependency instead of extras ([#258](https://github.com/nominal-io/nominal-client/issues/258)) ([abed412](https://github.com/nominal-io/nominal-client/commit/abed412076fd810cdf6db28656f8b320090ea360))
* make utility functions in client to assist in ingesting mcap files ([#268](https://github.com/nominal-io/nominal-client/issues/268)) ([d817fc0](https://github.com/nominal-io/nominal-client/commit/d817fc08585c4ad716805e129a97a73add408190))
* plumb channel prefix for tabular files ([#265](https://github.com/nominal-io/nominal-client/issues/265)) ([2550bd4](https://github.com/nominal-io/nominal-client/commit/2550bd46ac41f07c0fe29b5237ff00522885efa5))
* uniformly expose channel prefix tree delimiter in all create dataset methods ([#269](https://github.com/nominal-io/nominal-client/issues/269)) ([2a66f2b](https://github.com/nominal-io/nominal-client/commit/2a66f2b852386fe7b2968830899f0d0dfc2442ba))
* update docs to reflect h265 support ([#264](https://github.com/nominal-io/nominal-client/issues/264)) ([8707c4b](https://github.com/nominal-io/nominal-client/commit/8707c4bd5c93be1d82be7ff76d427f92f9f9ca31))
* Utilities to fetch scopes by name from assets ([c5eebe5](https://github.com/nominal-io/nominal-client/commit/c5eebe510ea659ea7185c85e572541e14dd433c1))
* utilities to fetch scopes by name from assets ([#266](https://github.com/nominal-io/nominal-client/issues/266)) ([c5eebe5](https://github.com/nominal-io/nominal-client/commit/c5eebe510ea659ea7185c85e572541e14dd433c1))

## [1.40.1](https://github.com/nominal-io/nominal-client/compare/v1.40.0...v1.40.1) (2025-03-14)


### Bug Fixes

* dataset get_channels() ([#260](https://github.com/nominal-io/nominal-client/issues/260)) ([4d67b50](https://github.com/nominal-io/nominal-client/commit/4d67b5061f559c094846831d40cd82505a309254))

## [1.40.0](https://github.com/nominal-io/nominal-client/compare/v1.39.0...v1.40.0) (2025-03-10)


### Features

* add functionality to experimental package for normalizing video ([#256](https://github.com/nominal-io/nominal-client/issues/256)) ([acedb7e](https://github.com/nominal-io/nominal-client/commit/acedb7eceed62870ea6075d0b4e8df26f7255ff4))

## [1.39.0](https://github.com/nominal-io/nominal-client/compare/v1.38.0...v1.39.0) (2025-03-10)


### Features

* add video files support ([#253](https://github.com/nominal-io/nominal-client/issues/253)) ([587a932](https://github.com/nominal-io/nominal-client/commit/587a9322ae41d20460776bae024c003c259d4ca0))


### Bug Fixes

* bump nominal api to latest version ([#255](https://github.com/nominal-io/nominal-client/issues/255)) ([140c16f](https://github.com/nominal-io/nominal-client/commit/140c16fd5458957eb0c237c2dd873519c1de84ab))

## [1.38.0](https://github.com/nominal-io/nominal-client/compare/v1.37.0...v1.38.0) (2025-03-06)


### Features

* support multi-file video from client ([#251](https://github.com/nominal-io/nominal-client/issues/251)) ([9611717](https://github.com/nominal-io/nominal-client/commit/9611717ddca85a55ce12891033e2dad86ff572d0))

## [1.37.0](https://github.com/nominal-io/nominal-client/compare/v1.36.0...v1.37.0) (2025-03-06)


### Features

* use new channel metadata endpoints ([#248](https://github.com/nominal-io/nominal-client/issues/248)) ([00b0801](https://github.com/nominal-io/nominal-client/commit/00b0801b6d6aabb02d2ec9a19e93d21254e138e3))


### Bug Fixes

* list and remove various types of data sources ([#209](https://github.com/nominal-io/nominal-client/issues/209)) ([a018c2c](https://github.com/nominal-io/nominal-client/commit/a018c2cc0d773f92512774bf4e2c1c91cb60a7e1))
* return type of create_streaming_connection and clean .close() and enqueue metric ([#225](https://github.com/nominal-io/nominal-client/issues/225)) ([b97ab06](https://github.com/nominal-io/nominal-client/commit/b97ab062861a4d6da1aadb8fa92f804a81e1900b))
* stop using channel delimiter as channel prefix ([#249](https://github.com/nominal-io/nominal-client/issues/249)) ([0ba556f](https://github.com/nominal-io/nominal-client/commit/0ba556f744141aaa8c6a76e23e99701e9abcf65e))

## [1.36.0](https://github.com/nominal-io/nominal-client/compare/v1.35.0...v1.36.0) (2025-03-01)


### Features

* add execute to the Checklist class ([3efb9c9](https://github.com/nominal-io/nominal-client/commit/3efb9c9205d1a8a746eb64c80861a776ca27c3df))
* add execute_checklist to the Run class ([#246](https://github.com/nominal-io/nominal-client/issues/246)) ([3efb9c9](https://github.com/nominal-io/nominal-client/commit/3efb9c9205d1a8a746eb64c80861a776ca27c3df))
* expose more parameters in top-level `create_run` ([#242](https://github.com/nominal-io/nominal-client/issues/242)) ([d4c0f86](https://github.com/nominal-io/nominal-client/commit/d4c0f8672a8556867a5254d6b6f9225e97dd48be))

## [1.35.0](https://github.com/nominal-io/nominal-client/compare/v1.34.0...v1.35.0) (2025-02-27)


### Features

* support multi-file dataflash datasets ([#241](https://github.com/nominal-io/nominal-client/issues/241)) ([875ff85](https://github.com/nominal-io/nominal-client/commit/875ff85b39f1396c96ea0bbe345deb73fb56c577))

## [1.34.0](https://github.com/nominal-io/nominal-client/compare/v1.33.0...v1.34.0) (2025-02-26)


### Features

* add support for journal json upload ([#240](https://github.com/nominal-io/nominal-client/issues/240)) ([31f7b39](https://github.com/nominal-io/nominal-client/commit/31f7b39e519e2b23f2f0fd5f9225ff0810fd998e))
* expose run `assets` field ([#235](https://github.com/nominal-io/nominal-client/issues/235)) ([8d11a19](https://github.com/nominal-io/nominal-client/commit/8d11a19873fb73806592b8e46d2de19a7131ff05))
* Use ingest V2 endpoint everywhere in python client ([c613263](https://github.com/nominal-io/nominal-client/commit/c6132633d9b6e6884c72827f6ee3607da1185c84))
* use ingest V2 endpoint everywhere in python client ([#233](https://github.com/nominal-io/nominal-client/issues/233)) ([c613263](https://github.com/nominal-io/nominal-client/commit/c6132633d9b6e6884c72827f6ee3607da1185c84))

## [1.33.0](https://github.com/nominal-io/nominal-client/compare/v1.32.1...v1.33.0) (2025-02-25)


### Features

* bump nominal api to 0.565.1 ([#230](https://github.com/nominal-io/nominal-client/issues/230)) ([7445d91](https://github.com/nominal-io/nominal-client/commit/7445d9193d19a3f63c09072ea9860a5852543284))

## [1.32.1](https://github.com/nominal-io/nominal-client/compare/v1.32.0...v1.32.1) (2025-02-21)


### Bug Fixes

* remove compute expression representations from checklists ([#222](https://github.com/nominal-io/nominal-client/issues/222)) ([6a4687e](https://github.com/nominal-io/nominal-client/commit/6a4687e682869a1d0d691d8286e6131bf72e5735))
* remove repr service again ([#227](https://github.com/nominal-io/nominal-client/issues/227)) ([c35c10d](https://github.com/nominal-io/nominal-client/commit/c35c10d30e11ab14fcfddc0cae880572dd191ba5))

## [1.32.0](https://github.com/nominal-io/nominal-client/compare/v1.31.0...v1.32.0) (2025-02-20)


### Features

* metrics on streaming connections ([#220](https://github.com/nominal-io/nominal-client/issues/220)) ([9f23471](https://github.com/nominal-io/nominal-client/commit/9f23471c4ddea312e9cdd4e87625ac9e27a453e4))


### Bug Fixes

* cleanup streaming shutdown ([#219](https://github.com/nominal-io/nominal-client/issues/219)) ([a4a00c8](https://github.com/nominal-io/nominal-client/commit/a4a00c8fea3c1289a9e43979d1b29b45b3fae11f))

## [1.31.0](https://github.com/nominal-io/nominal-client/compare/v1.30.0...v1.31.0) (2025-02-18)


### Features

* rework streaming connections ([#213](https://github.com/nominal-io/nominal-client/issues/213)) ([8bf292b](https://github.com/nominal-io/nominal-client/commit/8bf292b36a616a4ad0c691db6352585281cd6960))

## [1.30.0](https://github.com/nominal-io/nominal-client/compare/v1.29.0...v1.30.0) (2025-02-13)


### Features

* add links to Run.update and Asset.update ([#215](https://github.com/nominal-io/nominal-client/issues/215)) ([9fea28e](https://github.com/nominal-io/nominal-client/commit/9fea28e4eb70085e51ac5e20e6cb78cc039a96dc))

## [1.29.0](https://github.com/nominal-io/nominal-client/compare/v1.28.0...v1.29.0) (2025-02-11)


### Features

* add `nominal.__version__` and `nom --version` ([#202](https://github.com/nominal-io/nominal-client/issues/202)) ([5256aaf](https://github.com/nominal-io/nominal-client/commit/5256aafd72676794754dbf25558970823b357d75))
* add ardupilot dataflash ingest ([#203](https://github.com/nominal-io/nominal-client/issues/203)) ([d5d522f](https://github.com/nominal-io/nominal-client/commit/d5d522f6dbcd1cf7a1e71ca1a1520d5cdcc8b187))
* improve streaming default params ([#210](https://github.com/nominal-io/nominal-client/issues/210)) ([8c99d57](https://github.com/nominal-io/nominal-client/commit/8c99d570034c2cc96d29e16b33f794ea967ab52a))
* protobuf support in the python client for streaming data ([#208](https://github.com/nominal-io/nominal-client/issues/208)) ([6fd94b4](https://github.com/nominal-io/nominal-client/commit/6fd94b46ab2e18ccb72e987d9ccaaac74329a72e))
* update client to support multi file mcap datasets via new ingest endp… ([#212](https://github.com/nominal-io/nominal-client/issues/212)) ([8d99a41](https://github.com/nominal-io/nominal-client/commit/8d99a4119ed98e36f98f96a5018314e344699a5f))


### Bug Fixes

* remove errant commas in markdown, fixing mkdocs documentation generation ([#201](https://github.com/nominal-io/nominal-client/issues/201)) ([066ab2a](https://github.com/nominal-io/nominal-client/commit/066ab2a630557a35e1ee8ca50881c858b2b235c1))

## [1.28.0](https://github.com/nominal-io/nominal-client/compare/v1.27.0...v1.28.0) (2025-01-23)


### Features

* allow --token when default token path doesn't exist in CLI ([#198](https://github.com/nominal-io/nominal-client/issues/198)) ([565ab0c](https://github.com/nominal-io/nominal-client/commit/565ab0c08727ef23fc8f40283ddf03a505d4d2dc))
* forward client CA cert bundle to requests when pushing artifacts to storage ([#200](https://github.com/nominal-io/nominal-client/issues/200)) ([615b405](https://github.com/nominal-io/nominal-client/commit/615b405a60596419775edcb26bc9b9b106f7f58c))

## [1.27.0](https://github.com/nominal-io/nominal-client/compare/v1.26.0...v1.27.0) (2025-01-17)


### Features

* allow searching for multiple properties on an asset ([#194](https://github.com/nominal-io/nominal-client/issues/194)) ([ed0d9f9](https://github.com/nominal-io/nominal-client/commit/ed0d9f9026d801c15751bc31efd3b4bb19eac1b4))
* allow uniformly filtering for multiple labels / properties for runs / assets ([#195](https://github.com/nominal-io/nominal-client/issues/195)) ([4ab9afd](https://github.com/nominal-io/nominal-client/commit/4ab9afd3f6bf7fc38a790da720e1c707ca36d572))
* switch from poetry to uv for environment management, and poetry to hatch for build backend ([#192](https://github.com/nominal-io/nominal-client/issues/192)) ([0300415](https://github.com/nominal-io/nominal-client/commit/030041524fa18e6c5c568922bd203bcb9e3c9a2c))

## [1.26.0](https://github.com/nominal-io/nominal-client/compare/v1.25.0...v1.26.0) (2025-01-14)


### Features

* add data-review/batch-initiate endpoint ([#165](https://github.com/nominal-io/nominal-client/issues/165)) ([3e659f5](https://github.com/nominal-io/nominal-client/commit/3e659f56ed39ca0129ae9dfb375673fe72e60a88))
* allow archiving and unarchiving all possible types from the python SDK ([#191](https://github.com/nominal-io/nominal-client/issues/191)) ([185f789](https://github.com/nominal-io/nominal-client/commit/185f789fc4c148caa1119e9d455a388da4f31003))
* turn scraping back on for nominal data sources ([#181](https://github.com/nominal-io/nominal-client/issues/181)) ([855018d](https://github.com/nominal-io/nominal-client/commit/855018def3d8cb4edc5851fff8fa7acc710a26e6))


### Bug Fixes

* add missing exports ([#190](https://github.com/nominal-io/nominal-client/issues/190)) ([4f1c926](https://github.com/nominal-io/nominal-client/commit/4f1c92625c0e634758c96797568b98168c633457))

## [1.25.0](https://github.com/nominal-io/nominal-client/compare/v1.24.0...v1.25.0) (2025-01-07)


### Features

* migrate to nominal-api package ([#186](https://github.com/nominal-io/nominal-client/issues/186)) ([bbc9336](https://github.com/nominal-io/nominal-client/commit/bbc93369f5420b4862c66cc9c1c99e0ceb04a7ec))

## [1.24.0](https://github.com/nominal-io/nominal-client/compare/v1.23.0...v1.24.0) (2025-01-07)


### Features

* add default day of year to custom timestamp format ([#183](https://github.com/nominal-io/nominal-client/issues/183)) ([686a5a7](https://github.com/nominal-io/nominal-client/commit/686a5a770be7b15997c62541fdda18f32f38c4f3))

## [1.23.0](https://github.com/nominal-io/nominal-client/compare/v1.22.0...v1.23.0) (2025-01-03)


### Features

* add asset via run creation instead ([#185](https://github.com/nominal-io/nominal-client/issues/185)) ([7e57ae4](https://github.com/nominal-io/nominal-client/commit/7e57ae4080987f713f71a04afa2c17b43154e670))
* bump conjure definitions ([#182](https://github.com/nominal-io/nominal-client/issues/182)) ([1987724](https://github.com/nominal-io/nominal-client/commit/1987724f683a44d37ac5979dff6135d045313cbd))


### Bug Fixes

* handle duplicated channels in tdms ([#164](https://github.com/nominal-io/nominal-client/issues/164)) ([08529c0](https://github.com/nominal-io/nominal-client/commit/08529c056454d778f7768296d0ad7874ddbbdee7))

## [1.22.0](https://github.com/nominal-io/nominal-client/compare/v1.21.0...v1.22.0) (2024-12-20)


### Features

* add run.add_asset() ([#180](https://github.com/nominal-io/nominal-client/issues/180)) ([e6a292b](https://github.com/nominal-io/nominal-client/commit/e6a292ba4f3cf84f03126a1e51d2f29d2abfe533))
* add streaming checklists ([#126](https://github.com/nominal-io/nominal-client/issues/126)) ([3093a4c](https://github.com/nominal-io/nominal-client/commit/3093a4c57ed03ec4b56de9f39d09fd86d58a5137))


### Bug Fixes

* fix __from__conjure ([#177](https://github.com/nominal-io/nominal-client/issues/177)) ([c436015](https://github.com/nominal-io/nominal-client/commit/c436015b3ba4b7ab527b9996a93aea6bfd9f0c90))
* issues of post merge review of [#106](https://github.com/nominal-io/nominal-client/issues/106) ([#151](https://github.com/nominal-io/nominal-client/issues/151)) ([e6ff256](https://github.com/nominal-io/nominal-client/commit/e6ff256c5ec90729a6a279c59e90e71e8ea53753))
* remove available_tag_values from create streaming connection and expose get connection ([#179](https://github.com/nominal-io/nominal-client/issues/179)) ([cdea621](https://github.com/nominal-io/nominal-client/commit/cdea6211568b100d5bafa5e2b1feef022cd0a802))

## [1.21.0](https://github.com/nominal-io/nominal-client/compare/v1.20.0...v1.21.0) (2024-12-16)


### Features

* add channel name delimiter to file ingest ([#169](https://github.com/nominal-io/nominal-client/issues/169)) ([be389f4](https://github.com/nominal-io/nominal-client/commit/be389f466437c0633372856d0646276025997cdc))

## [1.20.0](https://github.com/nominal-io/nominal-client/compare/v1.19.0...v1.20.0) (2024-12-16)


### Features

* rename --desc to --description ([#170](https://github.com/nominal-io/nominal-client/issues/170)) ([b9827be](https://github.com/nominal-io/nominal-client/commit/b9827be06d82d462ad30ba7f435f10d72669a04e))
* support wider breadth of filetype extensions ([#174](https://github.com/nominal-io/nominal-client/issues/174)) ([d3d8d8c](https://github.com/nominal-io/nominal-client/commit/d3d8d8c4acb0422f5d098659f04ecc4b2164cde9))


### Bug Fixes

* fix name of argument for trust store cert ([#173](https://github.com/nominal-io/nominal-client/issues/173)) ([9db6a61](https://github.com/nominal-io/nominal-client/commit/9db6a61dc17396cdf57a9f11a8b14c268c89ba0f))
* sanitize PR title before use ([#171](https://github.com/nominal-io/nominal-client/issues/171)) ([a341d1b](https://github.com/nominal-io/nominal-client/commit/a341d1b218146a34f7ad0ce0371d4041b1319a0e))

## [1.19.0](https://github.com/nominal-io/nominal-client/compare/v1.18.0...v1.19.0) (2024-12-12)


### Features

* simplify dev experience for uploading videos ([#161](https://github.com/nominal-io/nominal-client/issues/161)) ([acedbc0](https://github.com/nominal-io/nominal-client/commit/acedbc074d67f4799b7d0e70886dda3ceed7a27b))


### Bug Fixes

* Handle case where data file has extra extension prefixes ([7a0d891](https://github.com/nominal-io/nominal-client/commit/7a0d891f0ad1c5fd45ba995f59f0420d5f5d6390))
* handle case where data file has extra extension prefixes ([#167](https://github.com/nominal-io/nominal-client/issues/167)) ([7a0d891](https://github.com/nominal-io/nominal-client/commit/7a0d891f0ad1c5fd45ba995f59f0420d5f5d6390))

## [1.18.0](https://github.com/nominal-io/nominal-client/compare/v1.17.0...v1.18.0) (2024-12-10)


### Features

* add support for tags in connections and runs ([4003885](https://github.com/nominal-io/nominal-client/commit/4003885d8a664fe9ed8a1c3e0c4deb89be95aa40))
* add support for tags in connections, runs and assets ([#159](https://github.com/nominal-io/nominal-client/issues/159)) ([4003885](https://github.com/nominal-io/nominal-client/commit/4003885d8a664fe9ed8a1c3e0c4deb89be95aa40))
* expose trust store path for client_options decorator ([#162](https://github.com/nominal-io/nominal-client/issues/162)) ([ceede7f](https://github.com/nominal-io/nominal-client/commit/ceede7f52df9d23c73f1bbfb84710516701a6635))
* support string in write stream ([#142](https://github.com/nominal-io/nominal-client/issues/142)) ([b2cbe18](https://github.com/nominal-io/nominal-client/commit/b2cbe182d35e9ac5ea66496e80cd8e4d87f525a2))

## [1.17.0](https://github.com/nominal-io/nominal-client/compare/v1.16.0...v1.17.0) (2024-12-03)


### Features

* allow uploading manually timestamped videos ([#156](https://github.com/nominal-io/nominal-client/issues/156)) ([67f2867](https://github.com/nominal-io/nominal-client/commit/67f28672631aac3958b9d7b9b70f42634a55ef53))
* make FileType(s) public ([#158](https://github.com/nominal-io/nominal-client/issues/158)) ([2358570](https://github.com/nominal-io/nominal-client/commit/2358570a6b95aaac35089a62205f2e285d5aaee1))

## [1.16.0](https://github.com/nominal-io/nominal-client/compare/v1.15.0...v1.16.0) (2024-11-26)


### Features

* add mcap dataset creation ([#155](https://github.com/nominal-io/nominal-client/issues/155)) ([e18c3d7](https://github.com/nominal-io/nominal-client/commit/e18c3d725e06a7e07e7525792ab634205995c233))
* remove pydantic for dataclasses ([#153](https://github.com/nominal-io/nominal-client/issues/153)) ([0cac531](https://github.com/nominal-io/nominal-client/commit/0cac531299b62018e46c17269db42ce8a4ce9a3d))

## [1.15.0](https://github.com/nominal-io/nominal-client/compare/v1.14.0...v1.15.0) (2024-11-25)


### Features

* adding ability to specify time column in TDMS groups ([#146](https://github.com/nominal-io/nominal-client/issues/146)) ([0ae2fa5](https://github.com/nominal-io/nominal-client/commit/0ae2fa57d71f6a5f1c45851651896e79e5d2d97e))

## [1.14.0](https://github.com/nominal-io/nominal-client/compare/v1.13.0...v1.14.0) (2024-11-22)


### Features

* export upload_mcap_video function ([#149](https://github.com/nominal-io/nominal-client/issues/149)) ([d269e4a](https://github.com/nominal-io/nominal-client/commit/d269e4aae57ecd9eeb31e4d2545da47140b36ef2))

## [1.13.0](https://github.com/nominal-io/nominal-client/compare/v1.12.1...v1.13.0) (2024-11-22)


### Features

* update combined -&gt; scout-service-api ([#147](https://github.com/nominal-io/nominal-client/issues/147)) ([a891d3a](https://github.com/nominal-io/nominal-client/commit/a891d3a09e408667afa1c8433b5c38ce24a501c2))

## [1.12.1](https://github.com/nominal-io/nominal-client/compare/v1.12.0...v1.12.1) (2024-11-21)


### Bug Fixes

* bucket workaround not correct for current backend ([#143](https://github.com/nominal-io/nominal-client/issues/143)) ([eac2536](https://github.com/nominal-io/nominal-client/commit/eac2536d4ee01cb66d0f519e9c266117e3f29b68))
* nm.upload_pandas() fails with SSLError ([#144](https://github.com/nominal-io/nominal-client/issues/144)) ([62f2e3c](https://github.com/nominal-io/nominal-client/commit/62f2e3cc96cc99b9b724f725b50ef41fc6dc92cc))

## [1.12.0](https://github.com/nominal-io/nominal-client/compare/v1.11.0...v1.12.0) (2024-11-21)


### Features

* add content to Asset ([#125](https://github.com/nominal-io/nominal-client/issues/125)) ([de09dee](https://github.com/nominal-io/nominal-client/commit/de09deee1fa29cedf2bedbddcf813f0e3e1cf1f0))
* add enqueue_batch to WriteStreamBase ([#138](https://github.com/nominal-io/nominal-client/issues/138)) ([447bfe9](https://github.com/nominal-io/nominal-client/commit/447bfe95c8f97d5cac79a8e89c4f2c80d3f4c700))
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
