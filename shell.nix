
{ pkgs ? import <nixpkgs> {} }: let 
    #rocksdb = pkgs.callPackage ./lib/rocksdb.nix {};
in
  pkgs.mkShell {
    shellHook = ''
       export GOPATH=${toString ./.}/go
    '';
    buildInputs = with pkgs; [ git go go2nix ];
  }
