#!/usr/bin/env python3
"""Build coven's Android library (api.md E1): `fm.coven.NetworkWatch`, the
network callback Android calls for coven's watch of the device's network,
compiled against an Android SDK platform and packaged as an AAR in a Maven
repository beside coven-foundation's sources, where an app's Gradle build
finds it through `cargo metadata`. Run it after changing the Java source,
and commit what it writes: the build is deterministic, so an unchanged
source writes the same bytes.

    scripts/build-android-library.py

It needs a JDK's `javac` (JAVA_HOME, else the PATH's) and an Android SDK
platform (ANDROID_HOME, else ~/Library/Android/sdk; ANDROID_PLATFORM, else
android-35).
"""

import io
import os
import pathlib
import subprocess
import sys
import tempfile
import tomllib
import zipfile

ROOT = pathlib.Path(__file__).resolve().parent.parent
ANDROID = ROOT / "crates" / "coven-foundation" / "android"
SOURCES = ANDROID / "src"
GROUP, ARTIFACT = "fm.coven", "coven-network"
# Every entry's time: the zip format's earliest, so the bytes never change.
STAMP = (1980, 1, 1, 0, 0, 0)

MANIFEST = """<?xml version="1.0" encoding="utf-8"?>
<manifest xmlns:android="http://schemas.android.com/apk/res/android"
    package="fm.coven.network">
    <uses-sdk android:minSdkVersion="26" />
    <uses-permission android:name="android.permission.ACCESS_NETWORK_STATE" />
</manifest>
"""

# The class's methods are reached through JNI only: an app that shrinks
# code keeps them.
PROGUARD = "-keep class fm.coven.NetworkWatch { *; }\n-keep class fm.coven.NetworkWatch$* { *; }\n"

POM = """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>{group}</groupId>
  <artifactId>{artifact}</artifactId>
  <version>{version}</version>
  <packaging>aar</packaging>
</project>
"""


def zipped(entries):
    """A zip of `entries`, name to bytes, in name order, every time the same."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in sorted(entries):
            info = zipfile.ZipInfo(name, STAMP)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, entries[name])
    return buffer.getvalue()


def main():
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    sdk = pathlib.Path(os.environ.get("ANDROID_HOME", pathlib.Path.home() / "Library/Android/sdk"))
    platform = os.environ.get("ANDROID_PLATFORM", "android-35")
    android_jar = sdk / "platforms" / platform / "android.jar"
    if not android_jar.is_file():
        sys.exit(f"no Android SDK platform at {android_jar}")
    javac = "javac"
    if "JAVA_HOME" in os.environ:
        javac = str(pathlib.Path(os.environ["JAVA_HOME"]) / "bin" / "javac")
    sources = sorted(str(path) for path in SOURCES.rglob("*.java"))
    with tempfile.TemporaryDirectory() as classes:
        subprocess.run(
            [javac, "--release", "11", "-classpath", str(android_jar), "-d", classes, "-Xlint:all", "-Werror", *sources],
            check=True,
        )
        compiled = {
            path.relative_to(classes).as_posix(): path.read_bytes()
            for path in pathlib.Path(classes).rglob("*.class")
        }
    classes_jar = zipped({"META-INF/MANIFEST.MF": b"Manifest-Version: 1.0\r\n\r\n", **compiled})
    aar = zipped(
        {
            "AndroidManifest.xml": MANIFEST.encode(),
            "classes.jar": classes_jar,
            "proguard.txt": PROGUARD.encode(),
            "R.txt": b"",
        }
    )
    directory = ANDROID / "maven" / GROUP.replace(".", "/") / ARTIFACT / version
    directory.mkdir(parents=True, exist_ok=True)
    for stale in directory.parent.iterdir():
        if stale != directory:
            sys.exit(f"remove the older version first: {stale}")
    (directory / f"{ARTIFACT}-{version}.aar").write_bytes(aar)
    (directory / f"{ARTIFACT}-{version}.pom").write_text(
        POM.format(group=GROUP, artifact=ARTIFACT, version=version)
    )
    print(f"wrote {directory.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
