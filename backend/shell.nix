{ pkgs ? import <nixpkgs> {}, unstable ? import <nixos-unstable> {} }: let 
    rocksdb = pkgs.callPackage ./lib/rocksdb.nix {};
in
  pkgs.mkShell {
    buildInputs = with pkgs; [ gcc gdb rocksdb unstable.clang_8 ];
  }
