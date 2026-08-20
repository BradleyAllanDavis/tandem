# tandem spoke — nix-darwin launchd.user.agents module (mine.services.tandem-spoke).
#
# For a member whose Mac is nix-managed (nix-darwin), so the spoke config is
# declarative instead of deploy/jill's imperative install.sh — same
# spoke/main.py, same SpokeCore, just generated + launched by nix-darwin's
# launchd.user.agents (matching modules/things-mirror + modules/things-applier
# in bradley's dotfiles: a stable-binary launchd job, config regenerated on
# every rebuild).
#
# Secrets never live in the nix store: the device token and (queue-writer
# mode) the queue bearer token are read from files materialized out of band
# at deploy time (1Password, via the same tools/op-read-serialized pattern
# things-applier's run script uses) — this module only points at their
# paths, it does not embed the values.

{ config, lib, pkgs, username, src, ... }:

with lib;

let
  cfg = config.mine.services.tandem-spoke;

  configJson = builtins.toJSON ({
    hub_url = cfg.hubUrl;
    device_token_file = cfg.deviceTokenFile;
    trigger_tags = cfg.triggerTags;
    mirror_path = cfg.mirrorPath;
    mirror_agent = cfg.mirrorAgent;
    tick_seconds = cfg.tickSeconds;
    poll_wait = cfg.pollWait;
    writer = cfg.writer;
  } // optionalAttrs (cfg.writer == "queue") {
    queue_url = cfg.queueUrl;
    queue_token_file = cfg.queueTokenFile;
    queue_agent = cfg.queueAgent;
  } // optionalAttrs (cfg.writer == "local") {
    things_auth_token_file = cfg.thingsAuthTokenFile;
  });

  configFile = pkgs.writeText "tandem-spoke-config.json" configJson;

  home = "/Users/${username}";
  appSupport = "${home}/Library/Application Support/things-team";
in
{
  options.mine.services.tandem-spoke = {
    enable = mkEnableOption "tandem spoke (declarative, nix-darwin)";

    hubUrl = mkOption {
      type = types.str;
      default = "http://192.168.0.30:8712";
      description = ''
        Hub base URL, BY IP — launchd-context DNS for LAN hostnames is
        unreliable on macOS (the same reason deploy/jill/install.sh hardcodes
        an IP).
      '';
    };

    deviceTokenFile = mkOption {
      type = types.path;
      description = "Path to this device's hub bearer token (materialized out of band, e.g. from 1Password).";
    };

    triggerTags = mkOption {
      type = types.attrsOf (types.listOf types.str);
      default = { };
      example = { jill = [ "j" ]; };
      description = "Recipient handle -> EXACT tag titles (case-insensitive, never substring) that trigger delegation from this spoke's outbound scan.";
    };

    mirrorPath = mkOption {
      type = types.str;
      default = "${home}/.cache/things-mirror/main.sqlite";
      description = "Things DB mirror snapshot this spoke reads (modules/things-mirror in bradley's dotfiles).";
    };

    mirrorAgent = mkOption {
      type = types.str;
      default = "com.bradley.things-mirror";
      description = "launchd label of the local things-mirror agent to kickstart before a time-sensitive read.";
    };

    tickSeconds = mkOption {
      type = types.int;
      default = 5;
      description = "Local-phase tick interval (outbound scan + observe/retag) when long-poll is disabled or between long-poll returns.";
    };

    pollWait = mkOption {
      type = types.int;
      default = 3;
      description = "Long-poll wait (seconds) on /v1/deliveries; 0 disables long-poll (falls back to a fixed tickSeconds sleep).";
    };

    writer = mkOption {
      type = types.enum [ "local" "queue" ];
      default = "queue";
      description = ''
        "local" opens `things:///json` directly in this session (only sane
        for a Mac whose interactive GUI session it's fine to write into
        unattended). "queue" routes writes through the things-gateway
        durable HTTP queue instead, so nothing ever opens a URL scheme in
        an interactive login session — the queue's own applier is the
        Mac hand that actually does that, decoupled from this spoke.
      '';
    };

    # -- writer=local ------------------------------------------------------
    thingsAuthTokenFile = mkOption {
      type = types.nullOr types.path;
      default = null;
      description = "Things URL-scheme auth token file (writer=local only).";
    };

    # -- writer=queue --------------------------------------------------------
    queueUrl = mkOption {
      type = types.str;
      default = "http://192.168.0.30:8090";
      description = "things-queue base URL (writer=queue only).";
    };

    queueTokenFile = mkOption {
      type = types.nullOr types.path;
      default = null;
      description = "things-queue bearer token file (writer=queue only).";
    };

    queueAgent = mkOption {
      type = types.str;
      default = "tandem-spoke";
      description = "Agent name this spoke's queue ops are submitted under (distinguishes them from the gateway worker's own queue traffic in things-queue's op log during cutover).";
    };
  };

  config = mkIf cfg.enable {
    assertions = [
      {
        assertion = cfg.writer == "local" -> cfg.thingsAuthTokenFile != null;
        message = "mine.services.tandem-spoke: writer=local requires thingsAuthTokenFile.";
      }
      {
        assertion = cfg.writer == "queue" -> cfg.queueTokenFile != null;
        message = "mine.services.tandem-spoke: writer=queue requires queueTokenFile.";
      }
    ];

    system.activationScripts.postActivation.text = ''
      mkdir -p "${appSupport}"
      cp -f "${configFile}" "${appSupport}/config.json"
    '';

    launchd.user.agents.tandem-spoke = {
      serviceConfig = {
        Label = "com.bradley.tandem-spoke";
        ProgramArguments = [ "${pkgs.python3}/bin/python3" "${src}/spoke/main.py" ];
        EnvironmentVariables = {
          HOME = home;
          TANDEM_SPOKE_CONFIG = "${appSupport}/config.json";
        };
        KeepAlive = true;
        RunAtLoad = true;
        ThrottleInterval = 10;
        StandardOutPath = "/tmp/tandem-spoke.out";
        StandardErrorPath = "/tmp/tandem-spoke.err";
      };
    };
  };
}
