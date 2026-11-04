# Maintainer release runbook

This runbook covers release candidates (RCs) and general-availability (GA)
releases. The release workflow always builds and verifies first. Publishing is a
separate, explicit workflow-dispatch choice.

## Prepare the release commit

1. Start from an up-to-date, protected release branch and confirm required CI is
   green.
2. Choose a version:
   - RC: `MAJOR.MINOR.PATCH-rcN`, for example `0.12.0-rc1`.
   - GA: `MAJOR.MINOR.PATCH`, for example `0.12.0`.
3. Run `./release.sh <version>`. This updates `VERSION`, the root development
   revision, examples, and performance projects to `<version>-SNAPSHOT`.
4. Run `python3 scripts/release/validate_version.py`.
5. Review release notes and commit the version changes. The workflow releases
   the exact commit selected in GitHub Actions; do not publish from an
   unreviewed commit.

`VERSION` contains the release version without `-SNAPSHOT`. In source control,
the root Maven `revision` must be exactly `VERSION-SNAPSHOT`. The workflow
passes `-Drevision=<VERSION>` for release builds instead of editing the POM.

## Dry run (required)

1. Open the **Release** workflow in GitHub Actions.
2. Select the release commit or branch.
3. Leave **Publish to Maven Central and create the GitHub release** unchecked.
4. Run the workflow and download the `release-<version>` artifact.
5. Confirm:
   - unit tests and the Maven `verify` lifecycle passed;
   - `SHA256SUMS` lists every attached asset;
   - main JARs exist for all modules, including
     `packages/clickhouse-jdbc-all`;
   - `bom.json` and `bom.xml` are present and describe the complete reactor;
   - the version and commit are the intended release.

The dry run does not use publishing credentials and does not create a tag,
GitHub release, or Maven Central deployment.

## Publish

1. Re-run **Release** for the same reviewed commit.
2. Check **Publish to Maven Central and create the GitHub release**.
3. Approve the protected GitHub `release` environment if one is configured.
4. Wait for the verify step to pass. Only then does the workflow sign and
   publish the reactor to Maven Central.
5. Confirm the Maven Central deployment is published before announcing the
   release.
6. Confirm the GitHub release:
   - an RC is marked as a prerelease and is not `latest`;
   - a GA is not marked as a prerelease and is marked `latest`;
   - the tag is `v<VERSION>` and points to the selected workflow commit;
   - release assets include `clickhouse-jdbc-all`, both CycloneDX SBOM formats,
     and `SHA256SUMS`.

If Maven Central succeeds but GitHub release creation fails, do not rerun the
publish step blindly. Verify Central state first, then create the GitHub release
from the downloaded dry-run inventory or rerun only after confirming that the
Central plugin will not create a duplicate deployment.

## API compatibility gate

The **API compatibility** workflow finds the newest tag matching
`vMAJOR.MINOR.PATCH`; prerelease tags are deliberately excluded. Japicmp then
checks public binary compatibility for reactor JARs against that GA version.
The shaded `clickhouse-jdbc-all` distribution is excluded because its source
modules are checked directly.

Missing classes from optional dependencies are ignored to avoid environment
noise. Any intentional binary break still requires maintainer review and an
explicit compatibility decision; do not weaken the gate to accommodate an
unreviewed break.

## Repository and external controls

Repository automation cannot enforce organization settings. Maintainers should
configure:

- a protected `release` environment with required reviewers;
- least-privilege Maven Central and GPG secrets;
- branch protection requiring build and API compatibility checks;
- GitHub artifact and log retention appropriate for release audit needs;
- Maven Central account protections and recovery procedures.

Trusted publishing/provenance attestations should be added when the Maven
Central and GitHub organization configuration supports them end to end.
