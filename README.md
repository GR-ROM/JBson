# 🚀 High-Performance BSON Serializer/Deserializer

A blazing-fast, minimal-overhead BSON serializer and deserializer for Java. Designed for performance-critical applications that require efficient binary data exchange and tight memory usage, with zero reliance on external libraries like `org.bson`.

[![Java](https://img.shields.io/badge/java-23-blue?logo=openjdk)](https://openjdk.org/projects/jdk/23/)
[![Build](https://img.shields.io/github/actions/workflow/status/GR-ROM/JBson/build.yml?branch=main)](https://github.com/GR-ROM/JBson/actions)
[![License](https://img.shields.io/github/license/GR-ROM/JBson)](LICENSE)
[![Version](https://img.shields.io/github/v/tag/GR-ROM/JBson)](https://github.com/GR-ROM/JBson/releases)
[![](https://jitpack.io/v/GR-ROM/JBson.svg)](https://jitpack.io/#GR-ROM/JBson)

## ⚡ Key Features

- ✅ **High Performance**: Built from the ground up with speed in mind. Pure byte-level operations, zero-reflection runtime mode.
- 🧩 **Fully Compliant**: Conforms to the BSON specification v1.1.
- 🔧 **Custom POJO Binding**: Annotation-based field mapping for flexible document-object translation.
- 📦 **No Dependencies**: Self-contained. No `bson`, `jackson`, or `gson` dependencies.
- 🔍 **Tiny Footprint**: Extremely lightweight - ideal for microservices, mobile, and embedded systems.
- 📡 **NIO-Friendly**: Supports reading/writing BSON from `ByteBuffer`, `InputStream`, `OutputStream` for async/event-driven systems

## 📦 Installation

Coming soon to Maven Central...

## JitPack

### gradle.build
dependencies {
implementation("com.github.GR-ROM:JBson:0.1.1")
}