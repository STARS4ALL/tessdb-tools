
local_env := join(justfile_dir(), ".env")


# list all recipes
default:
    just --list

# --------------------
# Workspace management
# --------------------

# install additional tools
tools:
    uv tool install uv-publish

# Creates virtual env
init:
    uv sync

# sets the packahe version and modifies pyproject.toml
version pkg version:
    uv version --package {{pkg}} {{version}}

lock:
    uv lock --upgrade

sync:
    uv lock --upgrade

# Build the package
build pkg:
    #!/usr/bin/env bash
    set -exuo pipefail
    pkg={{pkg}}
    rm -fr dist/${pkg}/*
    uv build --package "${pkg}" --out-dir dist/${pkg}

test-publish pkg:
    #!/usr/bin/env bash
    set -exuo pipefail
    pkg={{pkg}}
    module="${pkg//-/_}"
    uvx uv-publish --repository testpypi --verbose "dist/${pkg}/*"
    uv run --index "https://test.pypi.org/simple/" --with ${pkg} --no-project python -c "from ${module} import __version__; print(__version__)"

publish pkg:
    #!/usr/bin/env bash
    set -exuo pipefail
    pkg={{pkg}}
    module="${pkg//-/_}"
    uvx uv-publish ${pkg} --verbose "dist/${pkg}/*"
    uv run --with ${pkg} --no-project python -c "from ${module} import __version__; print(__version__)"

# ----------
# Operations
# ----------

easy:
    uv run tess-db --console --log-file easy.log photometer --easy

repaired:
    uv run tess-db --console --log-file repaired.log photometer --repaired

renamed:
    uv run tess-db --console --log-file renamed.log photometer --renamed

complicated:
    uv run tess-db --console --log-file complicated.log photometer --complicated
