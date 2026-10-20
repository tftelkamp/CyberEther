#ifndef JETSTREAM_DOMAINS_IO_VRT_ZMQ_BLOCK_HH
#define JETSTREAM_DOMAINS_IO_VRT_ZMQ_BLOCK_HH

#include "jetstream/block.hh"

namespace Jetstream::Blocks {

struct VrtZmq : public Block::Config {

    U64 numberOfBatches = 8;
    U64 numberOfTimeSamples = 8192;
    U64 bufferMultiplier = 4;

    std::string hostName = "localhost";
    U64 vrt_instance = 0;
    U64 vrt_channel = 0;

    JST_BLOCK_TYPE(vrtzmq);
    JST_BLOCK_DOMAIN("IO");
    JST_BLOCK_PARAMS(numberOfBatches,
                     numberOfTimeSamples, bufferMultiplier,
                     hostName, vrt_instance, vrt_channel);

    JST_BLOCK_DESCRIPTION(
        "VRT-ZMQ",
        "Receives samples from a VRT stream over ZMQ.",
        "# VRT-ZMQ\n"
        "\n"
        "Subscribes to a VITA-49 (VRT) stream published over ZMQ and outputs its complex "
        "samples. Frequency and sample rate come from the stream's context packets, not "
        "from settings here.\n"
        "\n"
        "- **The stream paces the chain.** Each buffer comes out only once the stream has"
        " delivered all of its samples.\n"
        "- **Context changes drop older samples.** Samples received before a new frequency"
        " or sample rate never carry its new labels.\n"
        "- **No samples before the first context packet.** Data packets are ignored until "
        "the stream has announced its frequency and sample rate.\n"
        "- **Only 16-bit samples.** A stream whose context announces another sample "
        "size, such as one from `vrt_quantize`, stops the block with an error.\n"
        "- **A slow chain loses samples.** When the chain falls behind, the oldest "
        "samples are overwritten and Buffer Loss rises.\n"
        "\n"
        "## Ports\n"
        "\n"
        "| Port | Type | Shape |\n"
        "|---|---|---|\n"
        "| **Output** (out) | CF32 | Batches x Samples, on a `batchAxis` then a "
        "`sampleAxis`. Sets `frequency` and `sampleRate` from the context that labels "
        "these samples. |\n"
        "\n"
        "## Settings\n"
        "\n"
        "| Setting | Default | Range | Effect |\n"
        "|---|---|---|---|\n"
        "| **VRT ZMQ host** | localhost | Host name or address | Publisher to subscribe "
        "to. |\n"
        "| **VRT instance** | 0 | 0 or more | Selects TCP port 50100 + 10 x instance. |\n"
        "| **VRT channel** | 0 | 0 to 9 | Stream ID bit to receive. |\n"
        "| **Batches** | 8 | 1 or more | Batches per output buffer. |\n"
        "| **Samples** | 8,192 | 1 or more | Samples per batch, and the FFT size "
        "downstream. |\n"
        "| **Buffer Multiplier** | 4 | 1 or more | Output buffers the internal buffer "
        "holds before old samples are overwritten. |\n"
        "\n"
        "## Readouts\n"
        "\n"
        "| Readout | Shows |\n"
        "|---|---|\n"
        "| **VRT Frequency** | RF frequency from the latest context packet. |\n"
        "| **VRT Sample Rate** | Sample rate from the latest context packet. |\n"
        "| **Buffer Health** | How full the internal buffer is, smoothed, from 0 to "
        "100%. Near 100% means the chain is falling behind. |\n"
        "| **Buffer Loss** | Share of samples overwritten before the chain read them. "
        "Breaks in the VRT packet count show as GAP. |\n"
        "| **Throughput** | Rate the chain reads in MB/s against the rate the stream "
        "sends at 8 bytes per sample. |\n"
        "\n"
        "## Troubleshooting\n"
        "\n"
        "| Symptom | Cause | Fix |\n"
        "|---|---|---|\n"
        "| The log reports that the channel carries 1-bit or 2-bit samples | The stream "
        "was quantized by `vrt_quantize` | Insert `vrt_quantize --unpack` between the "
        "publisher and CyberEther. |\n"
        "\n"
        "## Under the Hood\n"
        "\n"
        "A receive thread converts 16-bit IQ data packets into a ring buffer, and each "
        "cycle copies out one full buffer. Changing any setting reconnects the stream.\n"
        "\n"
        "## See Also\n"
        "\n"
        "- Radios plugged in over USB work through **SDR**."
    );
};

}  // namespace Jetstream::Blocks

#endif  // JETSTREAM_DOMAINS_IO_VRT_ZMQ_BLOCK_HH
