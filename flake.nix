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
          # build scripts and libsqlite3-sys (bundled) need a C compiler
          stdenv.cc
        ];

        # On darwin the test binaries link against -liconv; make it findable
        # outside a full stdenv build environment.
        darwinLinkEnv = lib.optionalString pkgs.stdenv.isDarwin ''
          export LIBRARY_PATH="${pkgs.libiconv}/lib''${LIBRARY_PATH:+:$LIBRARY_PATH}"
        '';

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
            ${darwinLinkEnv}
            cargo clippy --locked --all-targets -- -D warnings
          '';
        };

        ci-test = pkgs.writeShellApplication {
          name = "ci-test";
          runtimeInputs = rustToolchain ++ [pkgs.python3];
          text = ''
            ${darwinLinkEnv}
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

        releaseArtifactPlatform =
          if pkgs.stdenv.hostPlatform.isDarwin && pkgs.stdenv.hostPlatform.isAarch64
          then "aarch64-darwin"
          else if pkgs.stdenv.hostPlatform.isDarwin && pkgs.stdenv.hostPlatform.isx86_64
          then "x86_64-darwin"
          else if pkgs.stdenv.hostPlatform.isLinux && pkgs.stdenv.hostPlatform.isAarch64
          then "aarch64-linux"
          else if pkgs.stdenv.hostPlatform.isLinux && pkgs.stdenv.hostPlatform.isx86_64
          then "x86_64-linux"
          else null;
        releaseArtifactName =
          if releaseArtifactPlatform == null
          then null
          else "ctx-v${cliPackage.version}-${releaseArtifactPlatform}.tar.gz";
        release-artifact =
          if releaseArtifactName == null
          then null
          else
            pkgs.runCommand "ctx-release-artifact-${cliPackage.version}-${releaseArtifactPlatform}" {
              nativeBuildInputs = with pkgs; [
                coreutils
                gnutar
                gzip
              ];
            } ''
              mkdir -p "$out" "$TMPDIR/stage/${lib.removeSuffix ".tar.gz" releaseArtifactName}"
              stage="$TMPDIR/stage/${lib.removeSuffix ".tar.gz" releaseArtifactName}"

              cp -p ${ctx}/bin/ctx "$stage/ctx"
              chmod 0555 "$stage/ctx"
              cp -p ${./README.md} "$stage/README.md"
              cp -p ${./LICENSE} "$stage/LICENSE"
              cp -p ${./CHANGELOG.md} "$stage/CHANGELOG.md"

              tar \
                --sort=name \
                --format=ustar \
                --mtime='@1' \
                --owner=0 \
                --group=0 \
                --numeric-owner \
                -C "$TMPDIR/stage" \
                -cf - \
                "${lib.removeSuffix ".tar.gz" releaseArtifactName}" | gzip -n > "$out/${releaseArtifactName}"

              sha="$(sha256sum "$out/${releaseArtifactName}" | cut -d ' ' -f1)"
              printf '%s  %s\n' "$sha" "${releaseArtifactName}" > "$out/${releaseArtifactName}.sha256"
            '';

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

            (site_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
            (site_dir / "index.html").write_text(f"""<!doctype html>
            <html lang="en">
            <head>
              <meta charset="utf-8">
              <meta name="viewport" content="width=device-width, initial-scale=1">
              <title>ctx downloads</title>
              <style>
                body {{ font-family: system-ui, sans-serif; max-width: 920px; margin: 3rem auto; padding: 0 1rem; line-height: 1.5; }}
                code, pre {{ background: #f4f4f4; padding: 0.15rem 0.3rem; border-radius: 4px; }}
                pre {{ padding: 1rem; overflow-x: auto; }}
                .release {{ margin: 1rem 0 1.5rem; padding: 1rem; border: 1px solid #ddd; border-radius: 12px; }}
                .release.latest {{ background: #f7fbff; border-color: #9bc7f5; box-shadow: 0 1px 8px rgba(32, 105, 180, 0.12); }}
                .release-heading {{ display: flex; justify-content: space-between; gap: 1rem; align-items: flex-start; margin-bottom: 1rem; }}
                .release-heading h3 {{ margin: 0.1rem 0 0; }}
                .eyebrow {{ color: #555; font-size: 0.85rem; font-weight: 700; letter-spacing: 0.04em; margin: 0; text-transform: uppercase; }}
                .badge, .build-count {{ border-radius: 999px; display: inline-block; font-size: 0.8rem; font-weight: 700; padding: 0.15rem 0.5rem; white-space: nowrap; }}
                .badge {{ background: #0b66c3; color: white; margin-left: 0.35rem; vertical-align: middle; }}
                .build-count {{ background: #eee; color: #333; }}
                .build-grid {{ display: grid; gap: 1rem; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); }}
                .build {{ background: white; border: 1px solid #e5e5e5; border-radius: 10px; padding: 1rem; }}
                .build h4 {{ margin: 0 0 0.5rem; }}
                .filename {{ margin: 0 0 0.75rem; overflow-wrap: anywhere; }}
                .download-links {{ display: flex; flex-wrap: wrap; gap: 0.75rem; margin: 0.75rem 0; }}
                .primary-link {{ font-weight: 700; }}
                details pre {{ margin-bottom: 0; }}
                .previous-heading {{ margin-top: 2rem; }}
              </style>
            </head>
            <body>
              <h1>ctx downloads</h1>
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

        prepare-release = pkgs.writeShellApplication {
          name = "prepare-release";
          runtimeInputs = with pkgs; [
            jujutsu
            python3
          ];
          text = ''
            exec python3 - "$@" <<'PY'
            from __future__ import annotations

            import argparse
            import datetime as dt
            import pathlib
            import re
            import subprocess
            import sys
            import tomllib

            SEMVER_RE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)$")

            def run(args: list[str], *, check: bool = True) -> str:
                completed = subprocess.run(args, check=check, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                return completed.stdout

            def read_version(cargo_toml: pathlib.Path) -> str:
                with cargo_toml.open("rb") as handle:
                    version = tomllib.load(handle).get("package", {}).get("version")
                if not isinstance(version, str) or not SEMVER_RE.match(version):
                    raise SystemExit("crates/ctx-cli/Cargo.toml package.version must be X.Y.Z")
                return version

            def write_version(cargo_toml: pathlib.Path, version: str) -> None:
                if not SEMVER_RE.match(version):
                    raise SystemExit(f"invalid semver version: {version}")
                bare = version.removeprefix("v")
                text = cargo_toml.read_text()
                updated, count = re.subn(r'(?m)^version = "[^"]+"$', f'version = "{bare}"', text, count=1)
                if count != 1:
                    raise SystemExit("could not update package.version in crates/ctx-cli/Cargo.toml")
                cargo_toml.write_text(updated)

            def update_linux_build_manifest(repo_root: pathlib.Path, version: str) -> None:
                manifest = repo_root / "builds" / "release-linux-x86_64.yml"
                if not manifest.exists():
                    return
                tag = f"v{version.removeprefix('v')}"
                text = manifest.read_text()
                text = re.sub(
                    r"ctx-v\d+\.\d+\.\d+-x86_64-linux\.tar\.gz",
                    f"ctx-{tag}-x86_64-linux.tar.gz",
                    text,
                )
                manifest.write_text(text)

            def update_lockfile_version(repo_root: pathlib.Path, version: str) -> None:
                lockfile = repo_root / "Cargo.lock"
                if not lockfile.exists():
                    return
                text = lockfile.read_text()
                updated, count = re.subn(
                    r'(\[\[package\]\]\nname = "ctx"\nversion = ")([^"]+)(")',
                    rf'\g<1>{version.removeprefix("v")}\3',
                    text,
                    count=1,
                )
                if count != 1:
                    raise SystemExit("could not update ctx version in Cargo.lock")
                lockfile.write_text(updated)

            def version_key(version: str) -> tuple[int, int, int]:
                match = SEMVER_RE.match(version)
                if not match:
                    raise ValueError(version)
                return tuple(int(part) for part in match.groups())

            def latest_tag_before(version: str) -> str | None:
                tags = []
                for line in run(["jj", "tag", "list", "--no-pager", "--color=never"], check=False).splitlines():
                    name = line.split(":", 1)[0].strip()
                    if SEMVER_RE.match(name) and version_key(name) < version_key(version):
                        tags.append(name)
                if not tags:
                    return None
                return sorted(tags, key=version_key)[-1]

            def commit_summaries(baseline: str | None, revision: str) -> list[str]:
                revset = f"{baseline}..{revision}" if baseline else revision
                output = run(
                    [
                        "jj", "log", "-r", revset, "--no-graph", "--color=never",
                        "-T", 'description.first_line() ++ "\\n"',
                    ],
                    check=False,
                )
                ignored = re.compile(r"^(chore|build)(\([^)]*\))?: (bump version|release)\b", re.IGNORECASE)
                return [line.strip() for line in output.splitlines() if line.strip() and not ignored.search(line)]

            def bullet_from_commit(summary: str) -> tuple[str, str]:
                match = re.match(r"^(?P<type>[a-z]+)(?:\([^)]*\))?(?P<breaking>!)?:\s*(?P<body>.+)$", summary)
                if not match:
                    return "Changed", summary[0].upper() + summary[1:]
                kind = match.group("type")
                body = match.group("body")
                sentence = body[0].upper() + body[1:]
                if match.group("breaking"):
                    return "Breaking", sentence
                if kind == "feat":
                    return "Added", sentence
                if kind == "fix":
                    return "Fixed", sentence
                if kind == "docs":
                    return "Documentation", sentence
                if kind == "perf":
                    return "Performance", sentence
                return "Changed", sentence

            def generated_changelog(commits: list[str]) -> str:
                if not commits:
                    return "### Changed\n\n- Maintenance release.\n"
                sections: dict[str, list[str]] = {}
                for summary in commits:
                    section, bullet = bullet_from_commit(summary)
                    sections.setdefault(section, []).append(bullet.rstrip("."))
                order = ["Breaking", "Added", "Changed", "Fixed", "Performance", "Documentation"]
                parts: list[str] = []
                for section in order:
                    bullets = sections.get(section)
                    if not bullets:
                        continue
                    parts.append(f"### {section}\n")
                    parts.extend(f"- {bullet}." for bullet in bullets)
                    parts.append("")
                return "\n".join(parts).rstrip() + "\n"

            def unreleased_body(text: str) -> tuple[tuple[int, int] | None, str]:
                match = re.search(r"(?m)^## Unreleased\s*$", text)
                if not match:
                    return None, ""
                next_heading = re.search(r"(?m)^## ", text[match.end():])
                body_start = match.end()
                body_end = match.end() + next_heading.start() if next_heading else len(text)
                return (body_start, body_end), text[body_start:body_end].strip()

            def update_changelog(changelog: pathlib.Path, version: str, date: str, commits: list[str]) -> None:
                tag = f"v{version.removeprefix('v')}"
                entry_heading = f"## {tag} - {date}"
                if not changelog.exists():
                    changelog.write_text(f"# Changelog\n\n## Unreleased\n\n{entry_heading}\n\n{generated_changelog(commits)}")
                    return
                text = changelog.read_text()
                if re.search(rf"(?m)^## {re.escape(tag)}(?:\s+-\s+.*)?$", text):
                    return
                body_range, body = unreleased_body(text)
                entry_body = body if body else generated_changelog(commits).strip()
                entry = f"{entry_heading}\n\n{entry_body}\n"
                if body_range:
                    start, end = body_range
                    text = text[:start] + f"\n\n{entry}\n" + text[end:].lstrip("\n")
                else:
                    if not text.startswith("# Changelog"):
                        text = "# Changelog\n\n" + text
                    text = text.rstrip() + f"\n\n## Unreleased\n\n{entry}"
                changelog.write_text(text.rstrip() + "\n")

            def main() -> int:
                parser = argparse.ArgumentParser(description="Prepare crates/ctx-cli/Cargo.toml and CHANGELOG.md for a deterministic release.")
                parser.add_argument("--version", help="release version to write; defaults to crates/ctx-cli/Cargo.toml package.version")
                parser.add_argument("--revision", default="@", help="jj revision to summarize for the changelog")
                parser.add_argument("--date", default=dt.date.today().isoformat(), help="release date for CHANGELOG.md")
                parser.add_argument("--repo-root", default=".", help="repository root")
                args = parser.parse_args()
                repo_root = pathlib.Path(args.repo_root).resolve()
                # Workspace layout: the root Cargo.toml has no [package]; the
                # CLI crate owns the release version.
                cargo_toml = repo_root / "crates" / "ctx-cli" / "Cargo.toml"
                version = args.version.removeprefix("v") if args.version else read_version(cargo_toml)
                if args.version:
                    write_version(cargo_toml, version)
                update_lockfile_version(repo_root, version)
                update_linux_build_manifest(repo_root, version)
                baseline = latest_tag_before(version)
                commits = commit_summaries(baseline, args.revision)
                update_changelog(repo_root / "CHANGELOG.md", version, args.date, commits)
                print(f"prepared {version}")
                if baseline:
                    print(f"baseline: {baseline}")
                print(f"changelog commits: {len(commits)}")
                return 0

            sys.exit(main())
            PY
          '';
        };

        release-tag = pkgs.writeShellApplication {
          name = "release-tag";
          runtimeInputs = with pkgs; [
            git
            jujutsu
            python3
          ];
          text = ''
            if [[ $# -eq 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
              printf 'usage: %s [--revision REV]\n' "$0"
              printf 'Create and push a release tag from crates/ctx-cli/Cargo.toml version.\n\n'
              printf 'In jj repos, uses jj tag set + git push.\n'
              printf 'In plain git repos, uses git tag + git push.\n'
              exit 0
            fi

            revision="@"
            while [[ $# -gt 0 ]]; do
              case "$1" in
                --revision) revision="$2"; shift 2 ;;
                *) printf 'unknown argument: %s\n' "$1" >&2; exit 1 ;;
              esac
            done

            repo_root="$(git rev-parse --show-toplevel)"

            version="$(python3 -c '
            import pathlib, sys, tomllib
            path = pathlib.Path(sys.argv[1])
            with path.open("rb") as f:
                data = tomllib.load(f)
            version = data.get("package", {}).get("version")
            if not isinstance(version, str) or not version:
                raise SystemExit("Cargo.toml is missing package.version")
            print(version)
            ' "$repo_root/crates/ctx-cli/Cargo.toml")"

            if [[ "$version" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
              tag="v''${version#v}"
            else
              printf 'crates/ctx-cli/Cargo.toml version must be semver in the form X.Y.Z\n' >&2
              exit 1
            fi

            if git rev-parse --verify --quiet "refs/tags/$tag" >/dev/null; then
              printf 'local tag already exists: %s\n' "$tag" >&2
              exit 1
            fi

            if [[ -n "$(git ls-remote --tags origin "refs/tags/$tag" 2>/dev/null)" ]]; then
              printf 'remote tag already exists on origin: %s\n' "$tag" >&2
              exit 1
            fi

            if [[ -d .jj ]]; then
              if [[ "$(jj log -r "$revision" --no-graph --color=never -T 'empty()' 2>/dev/null)" == "true" ]]; then
                printf 'refusing to tag empty jj revision: %s\n' "$revision" >&2
                printf 'pass the non-empty release revision explicitly (for example --revision @-)\n' >&2
                exit 1
              fi
              jj tag set "$tag" --revision "$revision" --no-pager --color=never
              printf 'created tag %s via jj\n' "$tag"
            else
              if ! git diff --quiet || ! git diff --cached --quiet; then
                printf 'working tree must be clean before tagging\n' >&2
                exit 1
              fi
              git tag -a "$tag" HEAD -m "Release $tag"
              printf 'created annotated tag %s\n' "$tag"
            fi

            if ! git push origin "refs/tags/$tag"; then
              printf 'failed to push %s. run manually:\n  git push origin "refs/tags/%s"\n' "$tag" "$tag" >&2
              exit 1
            fi

            printf 'pushed %s to origin\n' "$tag"
          '';
        };

        release = pkgs.writeShellApplication {
          name = "release";
          runtimeInputs =
            rustToolchain
            ++ [
              prepare-release
            ]
            ++ (with pkgs; [
              coreutils
              git
              hut
              jujutsu
              nix
              python3
            ]);
          text = ''
            repo_root="$(git rev-parse --show-toplevel)"
            cd "$repo_root"

            version=""
            revision="@"
            validate=1
            tag_release=1
            build_artifact=1
            build_pages=1
            publish_pages=0
            submit_linux_build=0
            domain="averagechris.srht.site"
            subdirectory="/ctx"
            linux_manifest="builds/release-linux-x86_64.yml"

            usage() {
              cat <<'EOF'
            usage: release [options]

            Prepare changelog/version metadata, validate, tag, build local release artifacts,
            build SourceHut Pages content, and optionally publish/submit Linux builds.

            Options:
              --version X.Y.Z            update crates/ctx-cli/Cargo.toml before preparing the release
              --revision REV             jj revision to tag/summarize (default: @)
              --skip-validate            skip ci-fmt, ci-clippy, ci-test, and ci-docs
              --skip-tag                 do not create/push the release tag
              --skip-artifact            do not build/copy the local release artifact
              --skip-pages               do not build the static downloads page
              --publish-pages            publish dist/pages/ctx-pages.tar.gz with hut
              --submit-linux-build       submit builds/release-linux-x86_64.yml with hut
              --domain DOMAIN            SourceHut Pages domain (default: averagechris.srht.site)
              --subdirectory PATH        SourceHut Pages subdirectory (default: /ctx)
              -h, --help                 show this help
            EOF
            }

            while [[ $# -gt 0 ]]; do
              case "$1" in
                --version) version="$2"; shift 2 ;;
                --revision) revision="$2"; shift 2 ;;
                --skip-validate) validate=0; shift ;;
                --skip-tag) tag_release=0; shift ;;
                --skip-artifact) build_artifact=0; shift ;;
                --skip-pages) build_pages=0; shift ;;
                --publish-pages) publish_pages=1; shift ;;
                --submit-linux-build) submit_linux_build=1; shift ;;
                --domain) domain="$2"; shift 2 ;;
                --subdirectory) subdirectory="$2"; shift 2 ;;
                -h|--help) usage; exit 0 ;;
                *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 1 ;;
              esac
            done

            prepare_args=("--revision" "$revision" "--repo-root" "$repo_root")
            if [[ -n "$version" ]]; then
              prepare_args+=("--version" "$version")
            fi
            prepare-release "''${prepare_args[@]}"

            cargo check --locked --quiet --workspace

            version="$(python3 - <<'PY'
            import pathlib, tomllib
            with pathlib.Path('crates/ctx-cli/Cargo.toml').open('rb') as handle:
                print(tomllib.load(handle)['package']['version'])
            PY
            )"
            tag="v$version"

            # Ensure the release commit is described before tagging: prepare-release
            # stages version/changelog changes into the working copy, which may be a
            # fresh commit with no description — release-tag must never tag an
            # undescribed commit.
            if [[ -d .jj ]]; then
              if [[ -z "$(jj log -r "$revision" --no-graph --color=never -T 'description' 2>/dev/null)" ]]; then
                jj describe "$revision" -m "chore: release $tag" --no-pager --color=never
              fi
            elif ! git diff --quiet || ! git diff --cached --quiet; then
              git add -A
              git commit -m "chore: release $tag"
            fi

            if [[ $validate -eq 1 ]]; then
              nix run .#ci-fmt
              nix run .#ci-clippy
              nix run .#ci-test
              nix run .#ci-docs
            fi

            if [[ $tag_release -eq 1 ]]; then
              release_revision="$(jj log -r "$revision" --no-graph --color=never -T 'commit_id.short()')"
              nix run .#release-tag -- --revision "$release_revision"
              if [[ -d .jj ]]; then
                jj bookmark set main --revision "$release_revision" --no-pager --color=never
                jj git push --remote origin --bookmark main --no-pager --color=never
              fi
            fi

            if [[ $build_artifact -eq 1 ]]; then
              nix build .#release-artifact --out-link result-release-artifact
              mkdir -p dist/downloads
              cp -p result-release-artifact/* dist/downloads/
              printf 'copied release artifact(s) to dist/downloads\n'
            fi

            if [[ $build_pages -eq 1 ]]; then
              nix run .#build-pages -- --domain "$domain" --subdirectory "$subdirectory" --include-existing-downloads
            fi

            if [[ $publish_pages -eq 1 ]]; then
              nix run .#publish-pages -- --domain "$domain" --subdirectory "$subdirectory"
            else
              printf 'pages not published; run: nix run .#publish-pages -- --domain %q --subdirectory %q\n' "$domain" "$subdirectory"
            fi

            if [[ $submit_linux_build -eq 1 ]]; then
              hut builds submit "$linux_manifest" --note "ctx $tag linux release" --tags "ctx/$tag/release" --visibility unlisted
            else
              printf 'linux build not submitted; run: hut builds submit %s --note %q --tags %q --visibility unlisted\n' \
                "$linux_manifest" "ctx $tag linux release" "ctx/$tag/release"
            fi
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
        packages =
          {
            default = ctx;
            inherit ctx ci-fmt ci-clippy ci-test ci-docs fetch-upstream;
            inherit build-pages publish-pages;
            inherit prepare-release release release-tag;
          }
          // lib.optionalAttrs (release-artifact != null) {
            "release-artifact" = release-artifact;
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
          build-pages = flake-utils.lib.mkApp {drv = build-pages;};
          publish-pages = flake-utils.lib.mkApp {drv = publish-pages;};
          prepare-release = flake-utils.lib.mkApp {drv = prepare-release;};
          release = flake-utils.lib.mkApp {drv = release;};
          release-tag = flake-utils.lib.mkApp {drv = release-tag;};
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
