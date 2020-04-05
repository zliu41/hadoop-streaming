{ nixpkgs ? import <nixpkgs> {}, compiler ? "default", doBenchmark ? false }:

let

  inherit (nixpkgs) pkgs;

  f = { mkDerivation, base, bytestring, conduit, extra, hspec
      , hspec-discover, stdenv, text
      }:
      mkDerivation {
        pname = "hadoop-streaming";
        version = "0.2.0.1";
        src = ./.;
        libraryHaskellDepends = [ base bytestring conduit extra text ];
        testHaskellDepends = [ base bytestring conduit extra hspec ];
        testToolDepends = [ hspec-discover ];
        homepage = "https://github.com/zliu41/hadoop-streaming";
        description = "A simple Hadoop streaming library";
        license = stdenv.lib.licenses.bsd3;
      };

  haskellPackages = if compiler == "default"
                       then pkgs.haskellPackages
                       else pkgs.haskell.packages.${compiler};

  variant = if doBenchmark then pkgs.haskell.lib.doBenchmark else pkgs.lib.id;

  drv = variant (haskellPackages.callPackage f {});

in

  if pkgs.lib.inNixShell then drv.env else drv
