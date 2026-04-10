app_name := "jbolt"
version := "0.1.0"
git_version := `printf "%s%s" "$(git rev-parse --short HEAD)" "$([ -z "$(git status --porcelain)" ] || echo "-dirty")"`

[private]
default:
    @just --list --unsorted

# Build native module
build:
    jpm build

# Run tests
test:
    jpm test

# Install locally via jpm
install:
    jpm install

# Remove build artifacts
clean:
    jpm clean

# Bump version in project.janet
[group('release')]
bump new_version:
    sed -i 's/:version "[^"]*"/:version "{{new_version}}"/' project.janet
    @echo "Version bumped to {{new_version}}"

# Release: bump version, commit, tag, push
[group('release')]
release new_version:
    just bump {{new_version}}
    git add project.janet
    git diff --cached --quiet || git commit -m "chore: bump version to {{new_version}}"
    git tag -fa "v{{new_version}}" -m "v{{new_version}}"
    git push origin main --tags
    @echo "Released v{{new_version}}"
