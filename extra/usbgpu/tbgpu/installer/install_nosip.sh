#!/bin/bash
set -e

# Check SIP status if not building only
if [[ "$1" != "--build" ]]; then
  SIP_STATUS=$(csrutil status 2>&1)
  if [[ "$SIP_STATUS" == *"enabled"* ]]; then
    echo "ERROR: System Integrity Protection (SIP) is enabled."
    echo "This dev build requires SIP to be disabled to load unsigned dexts."
    echo ""
    echo "To disable SIP:"
    echo "  1. Restart and hold Power button (M1+) or Cmd+R (Intel)"
    echo "  2. Open Terminal from Recovery menu"
    echo "  3. Run: csrutil disable"
    echo "  4. Restart"
    exit 1
  fi
fi

echo "SIP is disabled, proceeding with dev build..."

cd "$(dirname "$0")"

# TINYGPU_BRIDGE=1 also builds the optional TinyGPUBridge personality (PCIe link reset of the bridges on the path to the GPU) and signs the dext with the
# restricted com.apple.developer.driverkit.transport.pci.bridge entitlement, which only loads on a machine that accepts it. The default build has neither.
BRIDGE_ARGS=(); ENT_SUFFIX=""
if [[ "${TINYGPU_BRIDGE:-0}" == "1" ]]; then BRIDGE_ARGS=(TINYGPU_BRIDGE=YES); ENT_SUFFIX=".Bridge"; fi

# Build without code signing
xcodebuild clean build CODE_SIGN_IDENTITY="" CODE_SIGNING_REQUIRED=NO ${BRIDGE_ARGS[@]+"${BRIDGE_ARGS[@]}"} -alltargets -configuration Debug build

APP_PATH="./build/Debug/TinyGPU.app"
DEXT_PATH="$APP_PATH/Contents/Library/SystemExtensions/org.tinygrad.tinygpu.driver2.dext"

# Ad-hoc sign with dev entitlements (matches any GPU)
codesign --sign - --entitlements "./TinyGPUDriverExtension/TinyGPUDriver.NoSIP$ENT_SUFFIX.entitlements" --force --timestamp --verbose "$DEXT_PATH"
codesign --sign - --entitlements ./macOS/macOS.entitlements --force --timestamp --verbose "$APP_PATH"

echo "Build complete: $APP_PATH"

if [[ "$1" == "--build" ]]; then
  exit 0
fi

# Install
echo "Installing to /Applications..."

if [ -d "/Applications/TinyGPU.app" ]; then
  echo "Removing existing /Applications/TinyGPU.app..."
  rm -rf "/Applications/TinyGPU.app"
fi

cp -r "$APP_PATH" /Applications/
echo "Installed to /Applications/TinyGPU.app"

echo "Activating driver extension..."
/Applications/TinyGPU.app/Contents/MacOS/TinyGPU install
