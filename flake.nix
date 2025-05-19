{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs";

  outputs = { self, nixpkgs }:
    let
      supportedSystems = [ "x86_64-linux" "x86_64-darwin" "aarch64-linux" "aarch64-darwin" ];
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
      conjure-python-client-package = { lib, fetchPypi, buildPythonPackage, requests }:
        let version = "2.13.0"; in buildPythonPackage {
          inherit version;
          name = "conjure-python-client";
          src = fetchPypi {
            inherit version;
            pname = "conjure-python-client";
            sha256 = "sha256-35b8CCsZn9/SPUu96kOA8QFYoYr5KLOMsMjaTwssLJc=";
          };
          propagatedBuildInputs = [ requests ];
        };
      nominal-api-package = { lib, buildPythonPackage, fetchPypi, requests, conjure-python-client  }:
        let version = "0.653.0"; in buildPythonPackage {
          name = "nominal-api";
          src = fetchPypi {
            inherit version;
            pname = "nominal_api";
            sha256 = "sha256-v/sT05yJEqgNwLxwe9qSPTsenuDDo/xC2YCsvNH/5B8=";
          };
          propagatedBuildInputs = [ requests conjure-python-client ];
        };
      nominal-api-protos-package = { lib, buildPythonPackage, fetchPypi, protobuf }:
        let version = "0.653.0"; in buildPythonPackage {
          name = "nominal-api-protos";
          src = fetchPypi {
            inherit version;
            pname = "nominal_api_protos";
            sha256 = "sha256-azPnqPTq13j1HWnyM/HleXnVAQjQ/+kH2b+3NVzOx2A=";
          };
          propagatedBuildInputs = [ protobuf ];
        };
      nominal-client-package = { lib, buildPythonPackage, hatchling, requests, nominal-api, nominal-api-protos, python-dateutil, conjure-python-client, pandas, typing-extensions, click, pyyaml, tabulate, types-tabulate, ffmpeg-python }:
        buildPythonPackage {
          format = "pyproject";
          version = "1.51.0";
          name = "nominal";
          src = lib.cleanSource ./.;
          nativeBuildInputs = [ hatchling ];
          propagatedBuildInputs = [ requests conjure-python-client nominal-api nominal-api-protos python-dateutil pandas typing-extensions click pyyaml tabulate types-tabulate ffmpeg-python ];
        };

    in rec {
      packages = forAllSystems (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          python = pkgs.python3;
        in rec {
          conjure-python-client = python.pkgs.callPackage conjure-python-client-package { };
          nominal-api = python.pkgs.callPackage nominal-api-package { conjure-python-client = conjure-python-client; };
          nominal-api-protos = python.pkgs.callPackage nominal-api-protos-package { };
          nominal-client = python.pkgs.callPackage nominal-client-package {
            conjure-python-client = conjure-python-client;
            nominal-api = nominal-api;
            nominal-api-protos = nominal-api-protos;
          };
          default = nominal-client;
        }
      );
    devShells.default = packages.default;
    };
}
