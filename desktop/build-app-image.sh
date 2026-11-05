#!/usr/bin/env bash
set -e

# Builds a self-contained jpackage app-image for the current platform/arch:
# a bundled JVM runtime (trimmed via jlink, using the pinned module list in
# desktop/jpackage/modules.list) + the pine-lang uberjar, requiring no
# system-wide Docker or JVM install to run.
#
# Must be run natively on the target OS/arch -- jpackage does not
# cross-compile the app-image. The jar itself (built once, below) is
# platform-independent and can be reused across OS/arch builds.

cd "$(dirname "$0")/.."

VERSION_FILE="src/pine/version.clj"
# -E (POSIX ERE), not -P (PCRE) -- this runs on macOS's BSD grep too, which
# has no -P support, unlike build-image.sh's Linux/CI-only Docker build.
PINE_VERSION=$(grep -oE '"[0-9]+\.[0-9]+\.[0-9]+[^"]*"' "$VERSION_FILE" | tr -d '"')

DESKTOP_DIR="desktop"
MODULES_LIST="$DESKTOP_DIR/jpackage/modules.list"
BUILD_DIR="$DESKTOP_DIR/build"
RUNTIME_DIR="$BUILD_DIR/runtime"
APP_IMAGE_DIR="$BUILD_DIR/app-image"
INPUT_DIR="$BUILD_DIR/input"

echo "Pine version: $PINE_VERSION"

if [ -f target/pine-standalone.jar ]; then
  echo "Reusing existing target/pine-standalone.jar (it's platform-independent -- CI builds it once and shares it across the OS/arch matrix)."
else
  echo "Building uberjar..."
  # clojure, not clj -- clj wants rlwrap (interactive REPL editing), which
  # isn't installed on CI runners; test.yml already uses `clojure` for the
  # same reason.
  clojure -T:build uber
fi

if [ -z "$JAVA_HOME" ]; then
  echo "JAVA_HOME must be set, pointing at the JDK version pinned in desktop/JDK_VERSION." >&2
  exit 1
fi

echo "Building trimmed JVM runtime via jlink..."
rm -rf "$RUNTIME_DIR"
# tr+sed, not `paste -sd,` -- BSD paste (macOS) rejects the combined
# short-option form GNU paste (Linux/Windows git-bash) accepts.
MODULES=$(grep -v '^#' "$MODULES_LIST" | grep -v '^\s*$' | tr '\n' ',' | sed 's/,$//')
# Use $JAVA_HOME/bin explicitly, not bare jlink/jpackage off PATH -- a
# mismatch between the jlink binary's own version and the --module-path
# jmods it's pointed at fails opaquely ("cannot find the build signature").
"$JAVA_HOME/bin/jlink" \
  --module-path "$JAVA_HOME/jmods" \
  --add-modules "$MODULES" \
  --output "$RUNTIME_DIR" \
  --strip-debug --no-header-files --no-man-pages \
  --generate-cds-archive
# --generate-cds-archive writes the JDK's own class-data-sharing archive
# into the runtime (lib/server/classes.jsa), so the JVM maps its classes
# instead of loading them one by one. beamlynx-desktop can then add an
# archive for pine's classes on top; that needs this one to exist.

echo "Building app-image via jpackage..."
rm -rf "$APP_IMAGE_DIR"
# jpackage's macOS bundler rejects any --app-version whose first component
# is 0 ("The first number in an app-version cannot be zero or negative") --
# pine-lang is pre-1.0. This only affects the app bundle's cosmetic
# CFBundleVersion; the real version check is the VERSION file + runtime API
# check in beamlynx-desktop, so it's safe to just omit it on macOS and let
# jpackage default it.
APP_VERSION_ARGS=(--app-version "$PINE_VERSION")
if [ "$(uname)" = "Darwin" ]; then
  APP_VERSION_ARGS=()
fi
# jpackage copies everything in --input into the image. target/ also holds
# the AOT class files the jar was built from (thousands of files the image
# never reads), so only the jar is staged.
rm -rf "$INPUT_DIR"
mkdir -p "$INPUT_DIR"
cp target/pine-standalone.jar "$INPUT_DIR/"
# A single-user server on loopback: the serial collector and a 1 GB heap
# cap use fewer threads and less memory than the JVM's defaults (G1, and a
# heap of a quarter of the machine's RAM).
"$JAVA_HOME/bin/jpackage" --type app-image \
  --name pine-server \
  --input "$INPUT_DIR" \
  --java-options '-XX:+UseSerialGC' \
  --java-options '-Xmx1g' \
  --main-jar pine-standalone.jar \
  --main-class pine.core \
  --runtime-image "$RUNTIME_DIR" \
  --dest "$APP_IMAGE_DIR" \
  "${APP_VERSION_ARGS[@]}"

echo "App-image built at: $APP_IMAGE_DIR/pine-server"
