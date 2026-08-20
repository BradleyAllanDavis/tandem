# Spoke setup — Bradley's neo (declarative)

Unlike `deploy/jill/` and `deploy/aaron/` (imperative `install.sh` for a Mac
this repo doesn't otherwise manage), Bradley's neo is nix-darwin-managed —
so this spoke is wired declaratively in his dotfiles instead of an install
script living here. This directory is a pointer, not a runbook.

- **Module**: `deploy/nix/spoke-module.nix`, exposed as flake output
  `darwinModules.tandem-spoke` (`mine.services.tandem-spoke` once imported).
- **Host wiring**: `~/.dotfiles/systems/darwin/neo/default.nix` imports
  `inputs.things-team.darwinModules.tandem-spoke` and sets
  `mine.services.tandem-spoke.enable = true` with `writer = "queue"` (writes
  ride the things-gateway queue on tank, never `things:///` directly in
  Bradley's interactive session — see `modules/things-applier/` there for
  the Mac hand that actually opens Things).
- **Secrets**: device token + queue token are 1Password `Automation`-vault
  items, materialized to root-only files at `just neo` rebuild time by the
  same `tools/op-read-serialized` pattern `tools/things-applier/run` already
  uses — never hand-placed, never in the nix store.
- **Why this exists**: `docs/plans/things-team-push-transport.md` §6 (in the
  dotfiles repo) — Bradley's outbound path previously rode the hub's
  in-process gateway worker reading a Syncthing-synced mirror, which put an
  irreducible ~5-15s Syncthing hop on his side that Jill's local-mirror spoke
  never had. This spoke reads neo's OWN local mirror instead, eliminating
  that hop. Full rationale, measurements, and the gateway-worker cutover
  procedure: same doc, §6.
