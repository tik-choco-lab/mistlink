# MistLink

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

**MistLink** is a lightweight streaming gateway specifically designed for use in **VRChat**. It enables low-latency video streaming by bridging WebRTC/WHIP and RTSP/UDP protocols, making it compatible with the **AVPro Video** player commonly used in VRChat worlds.

## Features

-   **WHIP Server**: Native support for WebRTC-HTTP Ingestion Protocol (WHIP), allowing seamless ingestion from OBS.
-   **RTSP Bridge**: Automatically serves ingested streams over RTSP, optimized for the **AVPro Video** player in VRChat.
-   **Multi-Protocol Ingestion**: Supports direct UDP (MPEG-TS) ingestion for low-latency scenarios.
-   **Low Latency**: Designed for real-time interaction within VRChat worlds.
-   **WebRTC Peer-to-Peer**: Integrated signaling client for establishing ultra-low latency P2P connections via [MistNet Signaling](https://github.com/tik-choco-lab/mistnet-signaling).

## Requirements

-   **Go**: 1.22 or later
-   **Signaling Server**: A compatible signaling server is required for WebRTC. We recommend using [MistNet Signaling](https://github.com/tik-choco-lab/mistnet-signaling).

## Installation

### Using Docker (Recommended for Windows)

The easiest way to build MistLink, especially for Windows with all CGO dependencies (Opus, FDK-AAC), is using Docker. This method will produce a `mistlink.exe` file.

```bash
# Build the builder image
docker build -t mistlink-builder -f Dockerfile.build .

# Extract the binary
docker create --name temp-container mistlink-builder
docker cp temp-container:/app/mistlink.exe ./mistlink.exe
docker rm temp-container
```

### Building from Source

To build from source, you need **Go 1.22+** and the following C libraries installed on your system (for CGO):

-   **libopus**
-   **fdk-aac**

```bash
# Clone the repository
git clone https://github.com/tik-choco-lab/mistlink.git
cd mistlink

# Build the binary
go build -o mistlink ./cmd/mistlink
```

## Getting Started

### Quick Start (UDP to WebRTC/RTSP)

Run MistLink with a specific room ID and UDP input:

```bash
./mistlink -room my-room-id
```

### Ingesting via WHIP (from OBS)

1.  Start MistLink (it listens for WHIP by default on the port specified in config).
2.  In OBS, go to **Settings > Stream**.
3.  Set **Service** to `WHIP`.
4.  Set **Server** to `http://<your-ip>:8080/whip`.
5.  Start Streaming!

### Viewing the Stream

Once MistLink is running, you can access the stream via:
-   **RTSP**: `rtsp://<your-ip>:8554/stream`
-   **WebRTC**: Connect via a compatible MistNet signaling client using the same `RoomID`.

## Configuration

MistLink uses a JSON-based configuration file. On the first run, it will generate a default config at the standard user config directory:

-   **Linux/macOS**: `~/.config/mistlink/config.json`
-   **Windows**: `%AppData%\mistlink\config.json`

### Config Parameters

| Key | Description | Default |
| :--- | :--- | :--- |
| `room_id` | Identifier for the streaming session | (Generated) |
| `input_url` | UDP ingestion address | `udp://0.0.0.0:1234` |
| `rtsp_url` | Endpoint for the RTSP server | `rtsp://localhost:8554/stream` |
| `whip_url` | Endpoint for the WHIP server | `http://localhost:8080/whip` |
| `signaling_server` | URL of the [MistNet Signaling](https://github.com/tik-choco-lab/mistnet-signaling) server | `ws://localhost:8080` |

## Development

To run with verbose logging for debugging:

```bash
./mistlink -debug
```

To view the current configuration path and content:

```bash
./mistlink --show-config
```

## License

This project is licensed under the **GNU General Public License v3.0**. See the [LICENSE](LICENSE) file for details.
