#ifndef JETSTREAM_DOMAINS_IO_VRT_ZMQ_MODULE_HH
#define JETSTREAM_DOMAINS_IO_VRT_ZMQ_MODULE_HH

#include "jetstream/memory/types.hh"
#include "jetstream/module.hh"

namespace Jetstream::Modules {

struct VrtZmq : public Module::Config {

    U64 numberOfBatches = 8;
    U64 numberOfTimeSamples = 8192;
    U64 bufferMultiplier = 4;

    std::string hostName = "localhost";
    U64 vrt_instance = 0;
    U64 vrt_channel = 0;

    JST_MODULE_TYPE(vrtzmq);
    JST_MODULE_PARAMS(numberOfBatches,
                      numberOfTimeSamples,
                      bufferMultiplier,
                      hostName, vrt_instance, vrt_channel);
};

}  // namespace Jetstream::Modules

#endif  // JETSTREAM_DOMAINS_IO_VRT_ZMQ_MODULE_HH

