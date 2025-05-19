{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs";

    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.uv2nix.follows = "uv2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };
  outputs =
    {
      self,
      nixpkgs,
      uv2nix,
      pyproject-nix,
      pyproject-build-systems,
      ...
    }:
    # let
    #   supportedSystems = [ "x86_64-linux" "x86_64-darwin" "aarch64-linux" "aarch64-darwin" ];
    #   forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
    # in {
    #   packages = forAllSystems (system:

    let
      inherit (nixpkgs) lib;

      workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ./.; };
      overlay = workspace.mkPyprojectOverlay {
        sourcePreference = "wheel";
      };

      pyprojectOverrides = final: prev: {
        nptdms = prev.nptdms.overrideAttrs (oldAttrs: {
          buildInputs = oldAttrs.buildInputs or [] ++ [ final.setuptools ];
        });
      };

      pkgs = nixpkgs.legacyPackages.aarch64-darwin;
      python = pkgs.python312;

      pythonSet =
        (pkgs.callPackage pyproject-nix.build.packages {
          inherit python;
        }).overrideScope
          (
            lib.composeManyExtensions [
              pyproject-build-systems.overlays.default
              overlay
              pyprojectOverrides
            ]
          );

    in
    {
      # Package a virtual environment as our main application.
      #
      # Enable no optional dependencies for production build.
      packages.aarch64-darwin.default = pythonSet.nominal;

      # Make hello runnable with `nix run`
      apps.aarch64-darwin = {
        default = {
          type = "app";
          program = "${self.packages.aarch64-darwin.default}/bin/nom";
        };
      };
    };
}
