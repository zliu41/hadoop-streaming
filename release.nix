{ compiler ? "default" }:

let
  pkgs = import <nixpkgs> { };

in
  { hadoop-streaming = pkgs.haskellPackages.callPackage ./default.nix { inherit compiler; };
  }
