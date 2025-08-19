#!/bin/bash

# Script to build and install libpg_query from source

set -e

echo "Installing libpg_query from source..."

# Create temporary directory
TEMP_DIR=$(mktemp -d)
cd "$TEMP_DIR"

echo "Cloning libpg_query repository..."
git clone https://github.com/pganalyze/libpg_query.git
cd libpg_query

echo "Building libpg_query..."
make

echo "Installing libpg_query..."
sudo make install

# Update library cache on Linux (not needed on macOS)
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    sudo ldconfig
fi

echo "Cleaning up..."
rm -rf "$TEMP_DIR"

echo "libpg_query installation completed!"
echo ""
echo "You may need to update your PKG_CONFIG_PATH:"
echo "export PKG_CONFIG_PATH=\"/usr/local/lib/pkgconfig:\$PKG_CONFIG_PATH\""