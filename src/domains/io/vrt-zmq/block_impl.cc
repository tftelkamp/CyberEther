#include <jetstream/domains/io/vrtzmq/block.hh>
#include <jetstream/detail/block_impl.hh>

#include <jetstream/domains/io/vrtzmq/module.hh>
#include "module_impl.hh"

namespace Jetstream::Blocks {

struct VrtZmqImpl : public Block::Impl, public DynamicConfig<Blocks::VrtZmq> {
    Result configure() override;
    Result define() override;
    Result create() override;

 protected:
    std::shared_ptr<Modules::VrtZmq> moduleConfig = std::make_shared<Modules::VrtZmq>();
    Modules::VrtZmqImpl* moduleImpl = nullptr;
    // std::string deviceDropdown;
};

Result VrtZmqImpl::configure() {
   
    moduleConfig->numberOfBatches = numberOfBatches;
    moduleConfig->numberOfTimeSamples = numberOfTimeSamples;
    moduleConfig->bufferMultiplier = bufferMultiplier;

    moduleConfig->hostName = hostName;
    moduleConfig->vrt_instance = vrt_instance;
    moduleConfig->vrt_channel = vrt_channel;


    return Result::SUCCESS;
}

Result VrtZmqImpl::define() {
    JST_CHECK(defineInterfaceOutput("signal",
                                    "Output",
                                    "The output buffer containing samples from the VRT stream."));

    JST_CHECK(defineInterfaceConfig("hostName",
                                    "VRT ZMQ host",
                                    "VRT ZMQ host",
                                    "text"));

    JST_CHECK(defineInterfaceConfig("vrt_instance",
                                    "VRT instance",
                                    "VRT instance",
                                    "int:instance"));

     JST_CHECK(defineInterfaceConfig("vrt_channel",
                                    "VRT channel",
                                    "VRT channel",
                                    "int:channel"));

    JST_CHECK(defineInterfaceConfig("numberOfBatches",
                                    "Batches",
                                    "Number of batches in output buffer.",
                                    "int:batches"));

    JST_CHECK(defineInterfaceConfig("numberOfTimeSamples",
                                    "Samples",
                                    "Number of samples per batch.",
                                    "int:samples"));

    JST_CHECK(defineInterfaceConfig("bufferMultiplier",
                                    "Buffer Multiplier",
                                    "Internal buffer size multiplier.",
                                    "int:x"));

    JST_CHECK(defineInterfaceMetric("VRTFreq",
                                    "VRT Frequency",
                                    "VRT Frequency.",
                                    "label",
        [this]() -> std::any {
            if (!moduleImpl) {
                return std::string("N/A");
            }
            const auto actual = moduleImpl->getVRTFreq();
            return jst::fmt::format("{:.1f} Hz", actual);
        }));

     JST_CHECK(defineInterfaceMetric("VRTRate",
                                    "VRT Sample Rate",
                                    "VRT Sample Rate.",
                                    "label",
        [this]() -> std::any {
            if (!moduleImpl) {
                return std::string("N/A");
            }
            const auto actual = moduleImpl->getVRTRate();
            return jst::fmt::format("{:.3f} Msps", actual/1e6);
        }));

    JST_CHECK(defineInterfaceMetric("bufferHealth",
                                    "Buffer Health",
                                    "Current buffer occupancy level.",
                                    "progressbar",
        [this]() -> std::any {
            if (!moduleImpl) {
                return std::pair<std::string, F32>{"0.0%", 0.0f};
            }
            const F32 bufferHealth = moduleImpl->getBufferHealth();
            return std::pair<std::string, F32>{jst::fmt::format("{:.1f}%", bufferHealth * 100.0f),
                                               bufferHealth};
        }));

    JST_CHECK(defineInterfaceMetric("throughput",
                                    "Throughput",
                                    "Current data throughput.",
                                    "label",
        [this]() -> std::any {
            if (!moduleImpl) {
                return std::string("N/A");
            }
            const auto [actual, expected] = moduleImpl->getThroughput();
            return jst::fmt::format("{:.1f} / {:.1f} MB/s", actual, expected);
        }));

    return Result::SUCCESS;
}

Result VrtZmqImpl::create() {
    JST_CHECK(moduleCreate("vrtzmq", moduleConfig, {}));
    JST_CHECK(moduleExposeOutput("signal", {"vrtzmq", "signal"}));

    moduleImpl = moduleHandle("vrtzmq")->getImpl<Modules::VrtZmqImpl>();

    return Result::SUCCESS;
}

JST_REGISTER_BLOCK(VrtZmqImpl);

}  // namespace Jetstream::Blocks
