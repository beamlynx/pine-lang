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
PINE_VERSION=$(grep -oP '\b\d+\.\d+\.\d+(-[a-zA-Z0-9]+)?\b' "$VERSION_FILE")

DESKTOP_DIR="desktop"
MODULES_LIST="$DESKTOP_DIR/jpackage/modules.list"
BUILD_DIR="$DESKTOP_DIR/build"
RUNTIME_DIR="$BUILD_DIR/runtime"
APP_IMAGE_DIR="$BUILD_DIR/app-image"

echo "Pine version: $PINE_VERSION"

if [ -f target/pine-standalone.jar ]; then
  echo "Reusing existing target/pine-standalone.jar (it's platform-independent -- CI builds it once and shares it across the OS/arch matrix)."
else
  echo "Building uberjar..."
  clj -T:build uber
fi

if [ -z "$JAVA_HOME" ]; then
  echo "JAVA_HOME must be set, pointing at the JDK version pinned in desktop/JDK_VERSION." >&2
  exit 1
fi

echo "Building trimmed JVM runtime via jlink..."
rm -rf "$RUNTIME_DIR"
MODULES=$(grep -v '^#' "$MODULES_LIST" | grep -v '^\s*$' | paste -sd,)
# Use $JAVA_HOME/bin explicitly, not bare jlink/jpackage off PATH -- a
# mismatch between the jlink binary's own version and the --module-path
# jmods it's pointed at fails opaquely ("cannot find the build signature").
"$JAVA_HOME/bin/jlink" \
  --module-path "$JAVA_HOME/jmods" \
  --add-modules "$MODULES" \
  --output "$RUNTIME_DIR" \
  --strip-debug --no-header-files --no-man-pages

echo "Building app-image via jpackage..."
rm -rf "$APP_IMAGE_DIR"
"$JAVA_HOME/bin/jpackage" --type app-image \
  --name pine-server \
  --input target \
  --main-jar pine-standalone.jar \
  --main-class pine.core \
  --runtime-image "$RUNTIME_DIR" \
  --dest "$APP_IMAGE_DIR" \
  --app-version "$PINE_VERSION"

echo "App-image built at: $APP_IMAGE_DIR/pine-server"
