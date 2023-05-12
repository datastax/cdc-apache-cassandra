# Release

## Cut a new release

1. Move to the release version the following files:
- `gradle.properties`: _version_ entry
- `connector/src/main/resources/cassandra-source-version.properties`
- `docs/docs-src/core/antora-cdc-cassandra.yml`: _version_ and _display_version_

2. ```
   VERSION=x.y.z
   git add -A 
   git commit -m "Release $VERSION"
   git tag "v${VERSION}"
   git push
   git push --tags
   ```
3. A "pre-release" with artifacts will be automatically generated. Once you are happy with it,  it is time to publish the release:
   * Navigate to the pre-release and click edit.
   * Click "Generate release notes", change the "Previous tag" baseline of the generated notes if needed.
   * Remove the author from the PR description. 
   * Uncheck the "Set as a pre-release" and check the "Set as the latest release" optins. 
   * Click "Update release"
