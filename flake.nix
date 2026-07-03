{
  description = "ctx - local CLI for indexing and searching coding-agent session history";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = import nixpkgs {inherit system;};
        lib = pkgs.lib;
        cliPackage = (fromTOML (builtins.readFile ./crates/ctx-cli/Cargo.toml)).package;

        ctx = pkgs.rustPlatform.buildRustPackage {
          pname = "ctx";
          version = cliPackage.version;
          src = lib.cleanSource ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
          };

          # rusqlite is built with the `bundled` feature; stdenv's C compiler
          # covers it. No network deps, so no openssl.
          buildInputs = lib.optionals pkgs.stdenv.isDarwin [pkgs.libiconv];

          # The CLI integration tests spawn python3 plugin scripts (via the
          # PYTHON env var) and run against per-test temp homes, but the
          # `directories` crate still needs a defined HOME.
          nativeCheckInputs = [pkgs.python3];
          cargoTestFlags = ["--workspace"];
          doCheck = true;
          preCheck = ''
            export HOME="$TMPDIR"
            export PYTHON=${pkgs.python3}/bin/python3
          '';

          meta = {
            description = cliPackage.description;
            homepage = "https://git.sr.ht/~averagechris/ctx";
            license = lib.licenses.asl20;
            mainProgram = "ctx";
          };
        };

        rustToolchain = with pkgs; [
          cargo
          clippy
          rustc
          rustfmt
        ];

        ci-fmt = pkgs.writeShellApplication {
          name = "ci-fmt";
          runtimeInputs = rustToolchain;
          text = ''
            cargo fmt --all --check
          '';
        };

        ci-clippy = pkgs.writeShellApplication {
          name = "ci-clippy";
          runtimeInputs = rustToolchain;
          text = ''
            cargo clippy --locked --all-targets -- -D warnings
          '';
        };

        ci-test = pkgs.writeShellApplication {
          name = "ci-test";
          runtimeInputs = rustToolchain ++ [pkgs.python3];
          text = ''
            cargo test --workspace
          '';
        };

        ci-docs = pkgs.writeShellApplication {
          name = "ci-docs";
          runtimeInputs = with pkgs; [
            bash
            coreutils
            diffutils
            gnugrep
            jq
            python3
            ripgrep
          ];
          text = ''
            exec bash scripts/check-docs.sh
          '';
        };

        fetch-upstream = pkgs.writeShellApplication {
          name = "fetch-upstream";
          runtimeInputs = [pkgs.jujutsu];
          text = ''
            # Refresh GitHub upstream refs. jj exposes Git's
            # refs/remotes/upstream/main as the remote bookmark main@upstream.
            exec jj git fetch --remote upstream "$@"
          '';
        };
      in {
        packages = {
          default = ctx;
          inherit ctx ci-fmt ci-clippy ci-test ci-docs fetch-upstream;
        };

        apps = {
          default = flake-utils.lib.mkApp {
            drv = ctx;
            exePath = "/bin/ctx";
          };
          ci-fmt = flake-utils.lib.mkApp {drv = ci-fmt;};
          ci-clippy = flake-utils.lib.mkApp {drv = ci-clippy;};
          ci-test = flake-utils.lib.mkApp {drv = ci-test;};
          ci-docs = flake-utils.lib.mkApp {drv = ci-docs;};
          fetch-upstream = flake-utils.lib.mkApp {drv = fetch-upstream;};
        };

        checks = {
          build = ctx;
        };

        devShells.default = pkgs.mkShell {
          inputsFrom = [ctx];
          packages =
            rustToolchain
            ++ (with pkgs; [
              python3
              rust-analyzer
            ]);
        };
      }
    );
}
