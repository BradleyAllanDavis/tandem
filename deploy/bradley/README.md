# Spoke setup — Bradley's neo (declarative)

Unlike `deploy/jill/` and `deploy/aaron/` (imperative `install.sh` for a Mac
this repo doesn't otherwise manage), Bradley's neo is nix-darwin-managed —
so this spoke is wired declaratively in his dotfiles instead of an install
script living here. This directory is a pointer, not a runbook; there is no
nix module in *this* repo for it (considered, dropped — see below).

- **Runtime**: `spoke/main.py` (this repo, `"writer": "queue"` mode — see
  its module docstring) run by a nix-darwin `launchd.user.agents` job
  defined entirely in the dotfiles repo: `modules/tandem-spoke/`. It
  references `${inputs.things-team}/spoke/main.py` from this repo's flake
  input directly, rather than importing a nix module exported from here.
- **Why not a `darwinModules.tandem-spoke` output here** (like the hub's
  `nixosModules.tandem-hub`): the hub's secrets are systemd `LoadCredential`
  files materialized out of band, source-agnostic. Bradley's Mac-side
  secret fetch is 1Password-**service-account**-token-based
  (`tools/op-read-serialized`, non-interactive, no Touch ID prompt) and
  MUST run as the logged-in user inside the launchd job itself — nix-darwin
  `system.activationScripts` run as root, where that token lookup
  (`$HOME/.config/op/service-account-token`) resolves to the wrong home.
  That constraint is dotfiles-specific plumbing this public repo shouldn't
  need to know about, so the launchd wiring + secret fetch lives entirely
  in `modules/tandem-spoke/` + `tools/tandem-spoke/run` there, matching the
  existing `modules/things-applier/` pattern exactly.
- **Secrets**: device token (`op://Automation/Things Team Bradley Device
  Token`) + the shared queue token (`op://Automation/Things Queue Token`,
  same one `things-applier` already uses) — never hand-placed, never in the
  nix store.
- **Why this exists**: `docs/plans/things-team-push-transport.md` §6 (in the
  dotfiles repo) — Bradley's outbound path previously rode the hub's
  in-process gateway worker reading a Syncthing-synced mirror, which put an
  irreducible ~5-15s Syncthing hop on his side that Jill's local-mirror spoke
  never had. This spoke reads neo's OWN local mirror instead, eliminating
  that hop. Full rationale, measurements, and the gateway-worker cutover
  procedure: same doc, §6.
