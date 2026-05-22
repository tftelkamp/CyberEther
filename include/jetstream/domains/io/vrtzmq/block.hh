#ifndef JETSTREAM_DOMAINS_IO_VRTZMQ_BLOCK_HH
#define JETSTREAM_DOMAINS_IO_VRTZMQ_BLOCK_HH

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
        "Interface for VRT ZMQ streams.",
        "# VRT ZMQ Source\n"
        "The VRT ZMQ block provides an interface to receive VRT of ZMQ streams, "
        "facilitating data acquisition.\n\n"

        "## Arguments\n"
        "- **Number of Batches**: Number of batches in output buffer.\n"
        "- **Number of Time Samples**: Samples per batch.\n"
        "- **Buffer Multiplier**: Internal buffer size multiplier.\n\n"

        "## Useful For\n"
        "- Receiving RF signals from software defined radios.\n"
        "- Real-time spectrum analysis.\n"
        "- Signal processing of live radio data.\n\n"

        "## Examples\n"
        "- Receive FM broadcast:\n"
        "  Config: Frequency=96.9 MHz, Sample Rate=2 MHz, Batches=8, Samples=8192\n"
        "  Output: CF32[8, 8192]\n\n"

        "## Implementation\n"
        "Soapy Module -> Output Buffer\n"
        "1. Opens VRT ZMQ stream with specified configuration.\n"
        "2. Streams samples from device into circular buffer.\n"
        "3. Outputs batches of samples for downstream processing."
    );
};

}  // namespace Jetstream::Blocks

#endif  // JETSTREAM_DOMAINS_IO_VRTZMQ_BLOCK_HH
