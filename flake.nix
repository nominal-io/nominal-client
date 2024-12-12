{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs";
  inputs.poetry2nix.url = "github:nix-community/poetry2nix";

  outputs = { self, nixpkgs, poetry2nix }:
    let
      supportedSystems = [ "x86_64-linux" "x86_64-darwin" "aarch64-linux" "aarch64-darwin" ];
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
      nominalPkg = { lib, buildPythonPackage, requests, python-dateutil, polars, conjure-python-client, pandas, typing-extensions, click, pyyaml, tabulate, types-tabulate, nptdms, }:
        buildPythonPackage {
          pname = "nominal";
          version = "0.1.0";
          src = lib.cleanSource ./.;
          propagatedBuildInputs = [ requests python-dateutil polars conjure-python-client pandas typing-extensions click pyyaml tabulate types-tabulate nptdms ];
        };

    in {
      packages = forAllSystems (system:
        let pkgs = nixpkgs.legacyPackages.${system}; in
        { default = pkgs.python3.pkgs.callPackage nominalPkg { }; }
      );
    };
}
