#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$(mktemp -d)"
OUTPUT="$PROJECT_ROOT/lambda.zip"

echo "Packaging Lambda function..."
echo "  Build dir: $BUILD_DIR"

# Copy backend source (excluding tests, local server, scripts)
mkdir -p "$BUILD_DIR/backend"
cp -r "$PROJECT_ROOT/backend/config" "$BUILD_DIR/backend/"
cp -r "$PROJECT_ROOT/backend/data" "$BUILD_DIR/backend/"
cp -r "$PROJECT_ROOT/backend/handlers" "$BUILD_DIR/backend/"
cp -r "$PROJECT_ROOT/backend/parsers" "$BUILD_DIR/backend/"
cp -r "$PROJECT_ROOT/backend/services" "$BUILD_DIR/backend/"

# Remove test files from the copy
find "$BUILD_DIR/backend" -name '*.test.js' -delete

# Copy root package files for dependency installation
cp "$PROJECT_ROOT/package.json" "$BUILD_DIR/"
cp "$PROJECT_ROOT/package-lock.json" "$BUILD_DIR/"

# Install production dependencies only
echo "Installing production dependencies..."
cd "$BUILD_DIR"
npm ci --omit=dev --ignore-scripts 2>&1 | tail -1

# Remove unnecessary files from node_modules to reduce size
find "$BUILD_DIR/node_modules" -name '*.d.ts' -delete 2>/dev/null || true
find "$BUILD_DIR/node_modules" -name '*.map' -delete 2>/dev/null || true
find "$BUILD_DIR/node_modules" -name 'README*' -delete 2>/dev/null || true
find "$BUILD_DIR/node_modules" -name 'CHANGELOG*' -delete 2>/dev/null || true
find "$BUILD_DIR/node_modules" -name 'LICENSE*' -delete 2>/dev/null || true

# Create zip
echo "Creating $OUTPUT..."
rm -f "$OUTPUT"
cd "$BUILD_DIR"
zip -rq "$OUTPUT" backend/ node_modules/ package.json

# Cleanup
rm -rf "$BUILD_DIR"

ZIP_SIZE=$(du -h "$OUTPUT" | cut -f1)
echo "Done! lambda.zip: $ZIP_SIZE"
