{
  description = "ctx - local CLI for indexing and searching coding-agent session history";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fleet.url = "github:averagechris/fleet/ab828532afb4cd8fcf2835051d21b4c55b65609d";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    fleet,
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = import nixpkgs {inherit system;};
        lib = pkgs.lib;
        cliPackage = (fromTOML (builtins.readFile ./crates/ctx-cli/Cargo.toml)).package;
        fleetApps = fleet.lib.fleet.presets.rust {
          inherit pkgs self;
          pname = "ctx";
          binaries = ["ctx"];
          subdir = "ctx";
          srhtRepo = "ctx";
          versionMode = "package";
          versionFile = "crates/ctx-cli/Cargo.toml";
          lockPackages = ["ctx"];
          # CLI tests spawn python3 history-source plugin helpers; the nixos
          # CI image has no system python3.
          ciExtraInputs = [pkgs.python3];
          releaseValidationApps = ["ci-docs"];
          releaseBackend = "github";
        };

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
            homepage = "https://github.com/averagechris/ctx";
            license = lib.licenses.asl20;
            mainProgram = "ctx";
          };
        };

        rustToolchain = with pkgs; [
          cargo
          clippy
          rustc
          rustfmt
          # build scripts and libsqlite3-sys (bundled) need a C compiler
          stdenv.cc
        ];

        # On darwin the test binaries link against -liconv; make it findable
        # outside a full stdenv build environment.
        darwinLinkEnv = lib.optionalString pkgs.stdenv.isDarwin ''
          export LIBRARY_PATH="${pkgs.libiconv}/lib''${LIBRARY_PATH:+:$LIBRARY_PATH}"
        '';

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
          inherit ctx ci-docs fetch-upstream;
          release-artifact = fleetApps.releaseArtifact system;
        };

        apps =
          {
            default = flake-utils.lib.mkApp {
              drv = ctx;
              exePath = "/bin/ctx";
            };
            ci-docs = flake-utils.lib.mkApp {drv = ci-docs;};
            fetch-upstream = flake-utils.lib.mkApp {drv = fetch-upstream;};
            inherit (fleetApps.apps) static-checks;
          }
          // fleetApps.apps;

        checks = {
          build = ctx;
          release-contract =
            pkgs.runCommand "ctx-release-contract" {
              nativeBuildInputs = [pkgs.gnugrep];
            } ''
              help="$(${fleetApps.apps.release.program} --help)"
              printf '%s\n' "$help" | grep -Fqx \
                'usage: release --version X.Y.Z [--check] [--allow-downgrade]'
              printf '%s\n' "$help" | grep -Fq -- \
                '--check  nonmutating ref/version preflight only; does not run validation or build artifacts'
              grep -Fqx '    nix run .#release -- --version X.Y.Z --check' ${./AGENTS.md}
              grep -Fqx '    nix run .#release -- --version X.Y.Z' ${./AGENTS.md}

              if printf '%s\n' "$help" | grep -Eq -- '--(skip-(validate|tag|artifact|pages)|publish-pages)'; then
                printf '%s\n' 'release help exposes an obsolete skip/page flag' >&2
                exit 1
              fi
              if grep -Eq -- '--(skip-(validate|tag|artifact|pages)|publish-pages)' ${./AGENTS.md}; then
                printf '%s\n' 'AGENTS.md documents an obsolete skip/page flag' >&2
                exit 1
              fi
              touch "$out"
            '';
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
