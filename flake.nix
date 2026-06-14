{
  description = "oteldb - OpenTelemetry-first aggregation system";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        commonArgs = {
          pname = "oteldb-packages";
          version = "0.1.0"; # Replace with actual version if needed
          src = ./.;

          # Update this hash when go.mod/go.sum changes.
          # You can get the new hash by running `nix build` and copying the `got: sha256-...` hash from the error.
          vendorHash = "sha256-LL29G1jemWTC1rLV44o4t7/I6IH+2of53xTyWEmmNn0=";

          # Optional: Run tests during build, disabled by default to speed up packaging
          doCheck = false;
        };

      in
      {
        packages = {
          oteldb = pkgs.buildGoModule (commonArgs // {
            pname = "oteldb";
            subPackages = [
              "cmd/oteldb"
              "cmd/odbrestore"
              "cmd/odbbackup"
              "cmd/odbmigrate"
              "cmd/otelbench"
              "cmd/oteldemo"
              "cmd/otelproxy"
            ];
          });

          odbagent = pkgs.buildGoModule (commonArgs // {
            pname = "odbagent";
            subPackages = [ "cmd/odbagent" ];
          });

          chotel = pkgs.buildGoModule (commonArgs // {
            pname = "chotel";
            subPackages = [ "cmd/chotel" ];
          });

          default = self.packages.${system}.oteldb;
        };

        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            go
            gopls
            golangci-lint
            delve

            # Added based on repo usage
            go-task
            goreleaser
            svu
            docker-client
            kubernetes-helm
            buf
            cosign
            syft
          ];
        };
      }
    );
}
