1. Publish on master
2. (Re-)build binary package (```./build-centos6/build.sh```)
3. Create tag & release at https://github.com/elixir-europe/plant-brapi-etl-data-lookup-gnpis/releases/new
  * Use semantic versioning to name the tag and the release (X.Y.Z)
  * Add changelogs in release description
  * Upload the binary package file (```./build-centos6/dist-etl.tar.gz```) to the release
