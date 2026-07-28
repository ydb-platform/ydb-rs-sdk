{
  description = "Development environment for YDB SDK comparison benchmarks";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
    sdkDeps.url = "github:NixOS/nixpkgs/nixos-23.05";
  };

  outputs =
    { nixpkgs, sdkDeps, ... }:
    let
      supportedSystems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-darwin"
        "x86_64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
          sdkDepsPkgs = import sdkDeps { inherit system; };
          sdkPython = sdkDepsPkgs.python3.withPackages (pythonPackages: [ pythonPackages.six ]);
          cppShell = pkgs.mkShell {
            packages = [
              pkgs.clang
              pkgs.clang-tools
              pkgs.cmake
              pkgs.git
              pkgs.just
              pkgs.netcat
              pkgs.ninja
              pkgs.pkg-config
              sdkDepsPkgs.brotli
              sdkDepsPkgs.bzip2
              sdkDepsPkgs.c-ares
              sdkDepsPkgs.double-conversion
              sdkDepsPkgs.grpc
              sdkDepsPkgs.libiconv
              sdkDepsPkgs.libidn
              sdkDepsPkgs.lz4
              sdkDepsPkgs.openssl
              sdkDepsPkgs.protobuf
              sdkDepsPkgs.ragel
              sdkDepsPkgs.rapidjson
              sdkDepsPkgs.re2
              sdkDepsPkgs.snappy
              sdkDepsPkgs.xxHash
              sdkDepsPkgs.yasm
              sdkDepsPkgs.zlib
              sdkDepsPkgs.zstd
              sdkPython
            ];

            SDK_COMPARE_PYTHON_EXECUTABLE = "${sdkPython}/bin/python3";
          };
        in
        {
          default = cppShell;
          cpp = cppShell;
        }
      );

      formatter = forAllSystems (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        pkgs.nixfmt
      );
    };
}
