# Release

## Cut a new release

1. Move to the release version the following files:
- `gradle.properties`: _version_ entry
- `connector/src/main/resources/cassandra-source-version.properties`
- `docs/antora.yml`: _version_ and _display_version_

2. Add [release notes](https://github.com/datastax/cdc-apache-cassandra/blob/master/CDC_Release_Notes.md).

3. ```
   VERSION=x.y.z
   git add -A 
   git commit -m "Release $VERSION"
   git tag "v${VERSION}"
   git push
   git push --tags
   ```
