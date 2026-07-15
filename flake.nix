{
  description = "ctx - local CLI for indexing and searching coding-agent session history";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fleet.url = "git+https://git.sr.ht/~averagechris/averagechris.srht.site";
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
          srhtPackage = fleet.packages.${system}.srht;
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

        build-pages = pkgs.writeShellApplication {
          name = "build-pages";
          runtimeInputs = with pkgs; [
            coreutils
            git
            gnutar
            gzip
            python3
          ];
          text = ''
            repo_root="$(git rev-parse --show-toplevel)"
            cd "$repo_root"

            domain="averagechris.srht.site"
            subdirectory="/ctx"
            include_existing_downloads=0

            while [[ $# -gt 0 ]]; do
              case "$1" in
                --domain) domain="$2"; shift 2 ;;
                --subdirectory) subdirectory="$2"; shift 2 ;;
                --include-existing-downloads) include_existing_downloads=1; shift ;;
                -h|--help)
                  printf 'usage: build-pages [--domain DOMAIN] [--subdirectory PATH] [--include-existing-downloads]\n'
                  exit 0
                  ;;
                *) printf 'unknown argument: %s\n' "$1" >&2; exit 1 ;;
              esac
            done

            export CTX_PAGES_DOMAIN="$domain"
            export CTX_PAGES_SUBDIRECTORY="$subdirectory"
            export CTX_INCLUDE_EXISTING_DOWNLOADS="$include_existing_downloads"

            exec python3 - <<'PY'
            from __future__ import annotations

            import html
            import json
            import os
            import pathlib
            import re
            import shutil
            import subprocess
            import tomllib
            import urllib.error
            import urllib.request

            repo = pathlib.Path.cwd()
            download_dir = repo / "dist" / "downloads"
            site_dir = repo / "dist" / "pages" / "site"
            pages_tarball = repo / "dist" / "pages" / "ctx-pages.tar.gz"

            # The workspace root Cargo.toml has no [package]; the CLI crate
            # owns the release version.
            with (repo / "crates" / "ctx-cli" / "Cargo.toml").open("rb") as handle:
                version = tomllib.load(handle)["package"]["version"]
            tag = f"v{version}"
            domain = os.environ["CTX_PAGES_DOMAIN"].rstrip("/")
            subdirectory = "/" + os.environ["CTX_PAGES_SUBDIRECTORY"].strip("/")
            base_url = f"https://{domain}{subdirectory}"
            include_existing_downloads = os.environ["CTX_INCLUDE_EXISTING_DOWNLOADS"] == "1"

            download_dir.mkdir(parents=True, exist_ok=True)

            def include_existing_downloads_from_pages() -> None:
                manifest_url = f"{base_url}/manifest.json"
                try:
                    with urllib.request.urlopen(manifest_url, timeout=30) as response:
                        manifest = json.load(response)
                except urllib.error.HTTPError as error:
                    if error.code == 404:
                        return
                    raise
                except urllib.error.URLError as error:
                    raise SystemExit(f"failed to fetch existing downloads manifest {manifest_url}: {error}") from error

                for artifact in manifest.get("artifacts", []):
                    name = artifact.get("name")
                    url = artifact.get("url")
                    if not isinstance(name, str) or not isinstance(url, str):
                        continue
                    artifact_path = download_dir / name
                    checksum_path = download_dir / f"{name}.sha256"
                    if not artifact_path.exists():
                        print(f"fetching existing download {name}")
                        urllib.request.urlretrieve(url, artifact_path)
                    if not checksum_path.exists():
                        sha = artifact.get("sha256")
                        if isinstance(sha, str) and sha:
                            checksum_path.write_text(f"{sha}  {name}\n")
                        else:
                            urllib.request.urlretrieve(f"{url}.sha256", checksum_path)

            if include_existing_downloads:
                include_existing_downloads_from_pages()

            def artifact_sort_key(path: pathlib.Path) -> tuple[int, int, int, str]:
                match = re.match(r"ctx-v(\d+)\.(\d+)\.(\d+)-(.+)\.tar\.gz$", path.name)
                if not match:
                    return (-1, -1, -1, path.name)
                major, minor, patch, platform = match.groups()
                return (int(major), int(minor), int(patch), platform)

            artifacts = sorted(download_dir.glob("*.tar.gz"), key=artifact_sort_key, reverse=True)
            if not artifacts:
                raise SystemExit(f"no download artifacts found in {download_dir}; run nix build .#release-artifact first")

            if site_dir.exists():
                shutil.rmtree(site_dir)
            (site_dir / "downloads").mkdir(parents=True)
            pages_tarball.parent.mkdir(parents=True, exist_ok=True)

            for path in sorted(download_dir.iterdir()):
                if path.is_file():
                    shutil.copy2(path, site_dir / "downloads" / path.name)

            def current_changelog() -> str:
                path = repo / "CHANGELOG.md"
                if not path.exists():
                    return "- See the tagged commit history for this release."
                text = path.read_text()
                match = re.search(rf"(?m)^## {re.escape(tag)}(?:\s+-\s+.*)?\s*$", text)
                if not match:
                    return "- See the tagged commit history for this release."
                next_match = re.search(r"(?m)^## ", text[match.end():])
                end = match.end() + next_match.start() if next_match else len(text)
                body = text[match.end():end].strip()
                return body or "- Maintenance release."

            def markdownish_to_html(markdown: str) -> str:
                lines = markdown.splitlines()
                out: list[str] = []
                in_list = False
                for line in lines:
                    if line.startswith("### "):
                        if in_list:
                            out.append("</ul>")
                            in_list = False
                        out.append(f"<h3>{html.escape(line[4:])}</h3>")
                    elif line.startswith("- "):
                        if not in_list:
                            out.append("<ul>")
                            in_list = True
                        out.append(f"<li>{html.escape(line[2:])}</li>")
                    elif line.strip():
                        if in_list:
                            out.append("</ul>")
                            in_list = False
                        out.append(f"<p>{html.escape(line.strip())}</p>")
                if in_list:
                    out.append("</ul>")
                return "\n".join(out)

            def artifact_info(path: pathlib.Path) -> dict[str, str]:
                match = re.match(r"ctx-(v\d+\.\d+\.\d+)-(.+)\.tar\.gz$", path.name)
                if not match:
                    return {"version": "other", "platform": path.name.removesuffix(".tar.gz")}
                release_version, platform = match.groups()
                return {"version": release_version, "platform": platform}

            def platform_label(platform: str) -> str:
                labels = {
                    "aarch64-darwin": "macOS Apple silicon",
                    "x86_64-linux": "Linux x86_64",
                }
                return labels.get(platform, platform.replace("-", " "))

            def build_count_label(count: int) -> str:
                return f"{count} build" if count == 1 else f"{count} builds"

            latest = artifacts[0]
            latest_checksum = latest.name + ".sha256"
            artifact_groups: dict[str, list[dict[str, str]]] = {}
            manifest = {"version": tag, "artifacts": []}
            for artifact in artifacts:
                checksum_path = download_dir / f"{artifact.name}.sha256"
                if not checksum_path.exists():
                    raise SystemExit(f"missing checksum for {artifact.name}: {checksum_path}")
                sha = checksum_path.read_text().split()[0]
                info = artifact_info(artifact)
                artifact_groups.setdefault(info["version"], []).append({
                    "name": artifact.name,
                    "platform": info["platform"],
                    "sha": sha,
                })
                manifest["artifacts"].append({
                    "name": artifact.name,
                    "url": f"{base_url}/downloads/{artifact.name}",
                    "sha256": sha,
                })

            latest_version = tag if tag in artifact_groups else artifact_info(latest)["version"]

            def render_build(build: dict[str, str]) -> str:
                name = build["name"]
                sha = build["sha"]
                return f"""
                  <article class="build">
                    <h4>{html.escape(platform_label(build['platform']))}</h4>
                    <p class="filename"><code>{html.escape(name)}</code></p>
                    <p class="download-links">
                      <a class="primary-link" href="downloads/{html.escape(name)}">Download tarball</a>
                      <a href="downloads/{html.escape(name)}.sha256">Checksum</a>
                    </p>
                    <details>
                      <summary>SHA-256</summary>
                      <pre><code>{html.escape(sha)}  {html.escape(name)}</code></pre>
                    </details>
                  </article>"""

            def render_release(release_version: str, builds: list[dict[str, str]], *, latest_release: bool) -> str:
                builds_html = "".join(render_build(build) for build in builds)
                label = "Latest release" if latest_release else "Release"
                latest_badge = "<span class=\"badge\">Latest</span>" if latest_release else ""
                title_html = f"{html.escape(release_version)} {latest_badge}" if latest_release else html.escape(release_version)
                class_names = "release latest" if latest_release else "release"
                return f"""
                <section class="{class_names}">
                  <div class="release-heading">
                    <div>
                      <p class="eyebrow">{label}</p>
                      <h3>{title_html}</h3>
                    </div>
                    <span class="build-count">{html.escape(build_count_label(len(builds)))}</span>
                  </div>
                  <div class="build-grid">
                    {builds_html}
                  </div>
                </section>"""

            latest_downloads = render_release(latest_version, artifact_groups[latest_version], latest_release=True)
            previous_downloads = "".join(
                render_release(release_version, builds, latest_release=False)
                for release_version, builds in artifact_groups.items()
                if release_version != latest_version
            )
            previous_downloads_section = f"""
              <h3 class="previous-heading">Previous releases</h3>
              {previous_downloads}""" if previous_downloads else ""

            STYLE = """\
              /* Rosé Pine Dawn */
              :root, :root[data-theme="dawn"] {
                --base: #faf4ed;
                --surface: #fffaf3;
                --overlay: #f2e9e1;
                --hl-med: #dfdad9;
                --muted: #9893a5;
                --subtle: #797593;
                --text: #575279;
                --love: #b4637a;
                --rose: #d7827e;
                --pine: #286983;
                --foam: #56949f;
              }
              /* Rosé Pine Moon */
              :root[data-theme="moon"] {
                --base: #232136;
                --surface: #2a273f;
                --overlay: #393552;
                --hl-med: #44415a;
                --muted: #6e6a86;
                --subtle: #908caa;
                --text: #e0def4;
                --love: #eb6f92;
                --rose: #ea9a97;
                --pine: #3e8fb0;
                --foam: #9ccfd8;
              }
              * { box-sizing: border-box; }
              body {
                background: var(--base);
                color: var(--text);
                font-family: Charter, Georgia, "Iowan Old Style", serif;
                line-height: 1.65;
                max-width: 920px;
                margin: 0 auto;
                padding: 3rem 1.25rem 4rem;
                transition: background 0.25s ease, color 0.25s ease;
              }
              a { color: var(--pine); text-decoration-color: color-mix(in srgb, var(--pine) 40%, transparent); }
              a:hover { color: var(--rose); }
              .masthead { display: flex; justify-content: space-between; align-items: flex-start; gap: 1rem; }
              h1 { font-size: 2rem; margin: 0; font-weight: 700; letter-spacing: -0.01em; }
              .home-link { color: var(--subtle); font-style: italic; margin: 0.25rem 0 0; font-size: 0.95rem; }
              .theme-toggle {
                background: var(--surface); border: 1px solid var(--hl-med); color: var(--subtle);
                border-radius: 999px; padding: 0.3rem 0.8rem; cursor: pointer;
                font-family: inherit; font-size: 0.85rem; font-style: italic;
                transition: border-color 0.15s ease;
                flex-shrink: 0; margin-top: 0.5rem;
              }
              .theme-toggle:hover { border-color: var(--rose); color: var(--text); }
              h2 { font-size: 1.5rem; margin: 2.25rem 0 1rem; font-weight: 700; }
              h3 { font-size: 1.15rem; }
              code, pre {
                font-family: ui-monospace, Menlo, monospace;
                background: var(--overlay); border-radius: 4px; padding: 0.15rem 0.3rem;
              }
              pre { padding: 1rem; overflow-x: auto; }
              pre code { background: none; padding: 0; }
              .release {
                background: var(--surface); border: 1px solid var(--hl-med); border-radius: 4px;
                padding: 1.25rem 1.4rem; margin: 1rem 0 1.5rem;
                box-shadow: 2px 2px 0 var(--hl-med);
              }
              .release.latest { border-color: var(--foam); }
              .release-heading { display: flex; justify-content: space-between; gap: 1rem; align-items: flex-start; margin-bottom: 1rem; }
              .release-heading h3 { margin: 0.1rem 0 0; font-family: ui-monospace, Menlo, monospace; }
              .eyebrow { color: var(--muted); font-size: 0.8rem; font-weight: 700; letter-spacing: 0.06em; margin: 0; text-transform: uppercase; }
              .badge, .build-count {
                border-radius: 999px; display: inline-block; font-size: 0.78rem; font-weight: 700;
                padding: 0.15rem 0.55rem; white-space: nowrap;
                font-family: ui-monospace, Menlo, monospace;
              }
              .badge { background: var(--foam); color: var(--base); margin-left: 0.35rem; vertical-align: middle; }
              .build-count { background: var(--overlay); color: var(--subtle); }
              .build-grid { display: grid; gap: 1rem; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); }
              .build { background: var(--base); border: 1px solid var(--hl-med); border-radius: 4px; padding: 1rem; }
              .build h4 { margin: 0 0 0.5rem; font-size: 0.95rem; }
              .filename { margin: 0 0 0.75rem; overflow-wrap: anywhere; font-size: 0.9rem; }
              .download-links { display: flex; flex-wrap: wrap; gap: 0.75rem; margin: 0.75rem 0; font-family: ui-monospace, Menlo, monospace; font-size: 0.9rem; }
              .primary-link { font-weight: 700; }
              details summary { cursor: pointer; color: var(--subtle); }
              details pre { margin-bottom: 0; }
              .previous-heading { margin-top: 2rem; }
              footer, .footer { color: var(--muted); font-size: 0.88rem; margin-top: 3.5rem; font-style: italic; text-align: center; }
            """

            # Runs before first paint to avoid a theme flash.
            HEAD_SCRIPT = """\
              (function () {
                var stored = null;
                try { stored = localStorage.getItem("theme"); } catch (e) {}
                var system = matchMedia("(prefers-color-scheme: dark)").matches ? "moon" : "dawn";
                document.documentElement.dataset.theme = stored || system;
              })();
            """

            BODY_SCRIPT = """\
              document.getElementById("theme-toggle").addEventListener("click", function () {
                var next = document.documentElement.dataset.theme === "moon" ? "dawn" : "moon";
                document.documentElement.dataset.theme = next;
                try { localStorage.setItem("theme", next); } catch (e) {}
              });
              matchMedia("(prefers-color-scheme: dark)").addEventListener("change", function (event) {
                var stored = null;
                try { stored = localStorage.getItem("theme"); } catch (e) {}
                if (!stored) document.documentElement.dataset.theme = event.matches ? "moon" : "dawn";
              });
            """

            (site_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
            (site_dir / "index.html").write_text(f"""<!doctype html>
            <html lang="en">
            <head>
              <meta charset="utf-8">
              <meta name="viewport" content="width=device-width, initial-scale=1">
              <title>ctx downloads</title>
              <script>
            {HEAD_SCRIPT}  </script>
              <style>
            {STYLE}  </style>
            </head>
            <body>
              <div class="masthead">
                <div>
                  <h1>ctx downloads</h1>
                  <p class="home-link"><a href="https://averagechris.srht.site/">~averagechris</a> / ctx</p>
                </div>
                <button class="theme-toggle" id="theme-toggle" aria-label="toggle color theme">dawn &frasl; moon</button>
              </div>
              <p>Local-first CLI for indexing and searching your coding-agent history. Hard fork of ctxrs/ctx &mdash; no telemetry, no network calls.</p>
              <p><a href="https://git.sr.ht/~averagechris/ctx">Source repository</a></p>

              <h2>What's new in {html.escape(tag)}</h2>
              {markdownish_to_html(current_changelog())}

              <h2>Binary downloads</h2>
              <p>Choose the build for your platform. The latest release is highlighted first; older releases are grouped below by version.</p>
              {latest_downloads}
              {previous_downloads_section}

              <h2>Manual install</h2>
              <pre><code>curl -LO {html.escape(base_url)}/downloads/{html.escape(latest.name)}
            curl -LO {html.escape(base_url)}/downloads/{html.escape(latest_checksum)}
            sha256sum -c {html.escape(latest_checksum)}
            tar -xzf {html.escape(latest.name)}
            install -m 0755 {html.escape(latest.name.removesuffix('.tar.gz'))}/ctx ~/.local/bin/ctx</code></pre>

              <h2>Nix install</h2>
              <pre><code>nix run sourcehut:~averagechris/ctx
            nix run 'git+https://git.sr.ht/~averagechris/ctx?ref=refs/tags/{html.escape(tag)}'</code></pre>
              <script>
            {BODY_SCRIPT}  </script>
            </body>
            </html>
            """)

            subprocess.run([
                "tar", "--sort=name", "--format=ustar", "--mtime=@1", "--owner=0", "--group=0", "--numeric-owner",
                "-C", str(site_dir), "-czf", str(pages_tarball), "."
            ], check=True)
            print(f"created {pages_tarball}")
            PY
          '';
        };

        publish-pages = pkgs.writeShellApplication {
          name = "publish-pages";
          runtimeInputs = with pkgs; [
            git
            hut
          ];
          text = ''
            repo_root="$(git rev-parse --show-toplevel)"
            cd "$repo_root"

            domain="averagechris.srht.site"
            subdirectory="/ctx"

            while [[ $# -gt 0 ]]; do
              case "$1" in
                --domain) domain="$2"; shift 2 ;;
                --subdirectory) subdirectory="$2"; shift 2 ;;
                -h|--help)
                  printf 'usage: publish-pages [--domain DOMAIN] [--subdirectory PATH]\n'
                  exit 0
                  ;;
                *) printf 'unknown argument: %s\n' "$1" >&2; exit 1 ;;
              esac
            done

            pages_tarball="$repo_root/dist/pages/ctx-pages.tar.gz"
            if [[ ! -f "$pages_tarball" ]]; then
              printf 'pages tarball not found: %s\nrun nix run .#build-pages first\n' "$pages_tarball" >&2
              exit 1
            fi

            exec hut pages publish "$pages_tarball" --domain "$domain" --subdirectory "$subdirectory"
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
          inherit build-pages publish-pages;
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
            build-pages = flake-utils.lib.mkApp {drv = build-pages;};
            publish-pages = flake-utils.lib.mkApp {drv = publish-pages;};
            inherit (fleetApps.apps) static-checks;
          }
          // fleetApps.apps;

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
