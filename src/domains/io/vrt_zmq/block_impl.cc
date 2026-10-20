#include <jetstream/domains/io/vrt_zmq/block.hh>
#include <jetstream/detail/block_impl.hh>

#include <jetstream/domains/io/vrt_zmq/module.hh>
#include "module_impl.hh"

namespace Jetstream::Blocks {

struct VrtZmqImpl : public Block::Impl, public DynamicConfig<Blocks::VrtZmq> {

    Result validate() override;
    Result configure() override;
    Result define() override;
    Result create() override;

 protected:
    std::shared_ptr<Modules::VrtZmq> moduleConfig = std::make_shared<Modules::VrtZmq>();
    Modules::VrtZmqImpl* moduleImpl = nullptr;
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

Result VrtZmqImpl::validate() {
    const auto& config = *candidate();

    if (config.numberOfBatches == 0) {
        JST_ERROR("[BLOCK_VRT_ZMQ] Number of batches cannot be zero.");
        return Result::ERROR;
    }

    if (config.numberOfTimeSamples == 0) {
        JST_ERROR("[BLOCK_VRT_ZMQ] Number of time samples cannot be zero.");
        return Result::ERROR;
    }

    if (config.bufferMultiplier == 0) {
        JST_ERROR("[BLOCK_VRT_ZMQ] Buffer multiplier cannot be zero.");
        return Result::ERROR;
    }

    return Result::SUCCESS;
}

Result VrtZmqImpl::define() {
    JST_CHECK(defineInterfaceOutput("signal",
                                    "Output",
                                    "The output buffer containing samples from the VRT stream."));

    JST_CHECK(defineInterfaceConfig("hostName",
                                    "VRT ZMQ host",
                                    "VRT ZMQ host",
                                    {{"type", "text"}}));

    JST_CHECK(defineInterfaceConfig("vrt_instance",
                                    "VRT instance",
                                    "VRT instance",
                                    {{"type", "uint"}, {"unit", ""}}));

     JST_CHECK(defineInterfaceConfig("vrt_channel",
                                    "VRT channel",
                                    "VRT channel",
                                    {{"type", "uint"}, {"unit", ""}}));

    JST_CHECK(defineInterfaceConfig("numberOfBatches",
                                    "Batches",
                                    "Number of batches in output buffer.",
                                    {{"type", "uint"}, {"unit", "batches"}}));

    JST_CHECK(defineInterfaceConfig("numberOfTimeSamples",
                                    "Samples",
                                    "Number of samples per batch.",
                                    {{"type", "uint"}, {"unit", "samples"}}));

    JST_CHECK(defineInterfaceConfig("bufferMultiplier",
                                    "Buffer Multiplier",
                                    "Internal buffer size multiplier.",
                                    {{"type", "uint"}, {"unit", "x"}}));

    JST_CHECK(defineInterfaceMetric("VRTFreq",
                                    "VRT Frequency",
                                    "RF frequency from the latest VRT context packet.",
                                    {{"type", "label"}},
        [this]() -> std::any {
            if (!moduleImpl) {
                return std::string("N/A");
            }
            return jst::fmt::format("{:.1f} Hz", moduleImpl->getMetadata().frequency);
        }));

    JST_CHECK(defineInterfaceMetric("VRTRate",
                                    "VRT Sample Rate",
                                    "Sample rate from the latest VRT context packet.",
                                    {{"type", "label"}},
        [this]() -> std::any {
            if (!moduleImpl) {
                return std::string("N/A");
            }
            return jst::fmt::format("{:.3f} Msps", moduleImpl->getMetadata().sampleRate / 1e6);
        }));

    JST_CHECK(defineInterfaceMetric("bufferHealth",
                                    "Buffer Health",
                                    "Current buffer occupancy level.",
                                    {{"type", "progressbar"}},
        [this]() -> std::any {
            const F32 health = moduleImpl ? moduleImpl->getBufferHealth() : 0.0f;
            return std::pair<std::string, F32>{jst::fmt::format("{:.1f}%", health * 100.0f), health};
        }));

    JST_CHECK(defineInterfaceMetric("bufferLoss",
                                    "Buffer Loss",
                                    "Samples overwritten before the chain read them, plus VRT "
                                    "packet-count gaps (GAP) since the stream started.",
                                    {{"type", "progressbar"}},
        [this]() -> std::any {
            const auto loss = moduleImpl ? moduleImpl->getLoss()
                                         : Modules::VrtZmqImpl::LossSnapshot{};
            auto label = loss.bufferLoss > 0.0 && loss.bufferLoss < 0.0001
                ? std::string("<0.01%") : jst::fmt::format("{:.2f}%", loss.bufferLoss * 100.0);
            if (loss.deviceGaps > 0) {
                label = jst::fmt::format("{} ({} GAP)", label, loss.deviceGaps);
            }
            return std::pair<std::string, F32>{label, static_cast<F32>(loss.bufferLoss)};
        }));

    JST_CHECK(defineInterfaceMetric("throughput",
                                    "Throughput",
                                    "Rate the chain reads relative to the rate the stream sends.",
                                    {{"type", "progressbar"}},
        [this]() -> std::any {
            const auto [actual, expected] = moduleImpl ? moduleImpl->getThroughput()
                                                       : std::pair<F32, F32>{0.0f, 0.0f};
            const F32 fraction = expected > 0.0f ? actual / expected : 0.0f;
            return std::pair<std::string, F32>{
                jst::fmt::format("{:.1f} / {:.1f} MB/s", actual, expected), fraction};
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
