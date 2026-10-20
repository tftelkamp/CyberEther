#include "module_impl.hh"

#include <zmq.h>

#include "vrt-tools.h"

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <limits>
#include <new>
#include <vector>

#include <jetstream/memory/axis.hh>
#include <jetstream/tools/numeric.hh>

namespace Jetstream::Modules {

namespace {

// Bounds each blocking receive so destroy() can stop the thread.
constexpr int ReceiveTimeoutMs = 100;
constexpr int ReceiveHighWaterMark = 10000;

}  // namespace

VrtZmqImpl::~VrtZmqImpl() {
    stopReceiver();
    closeSocket();
}

Result VrtZmqImpl::validate() {
    const auto& config = *candidate();
    validatedOutputSizeBytes = 0;
    validatedInternalElements = 0;
    validatedInternalSizeBytes = 0;

    if (config.vrt_channel >= MAX_CHANNELS) {
        JST_ERROR("[MODULE_VRT_ZMQ] VRT channel must be below {}.", MAX_CHANNELS);
        return Result::ERROR;
    }
    if (config.vrt_instance >
        (std::numeric_limits<U16>::max() - DEFAULT_MAIN_PORT) / MAX_CHANNELS) {
        JST_ERROR("[MODULE_VRT_ZMQ] VRT instance {} is beyond the TCP port range.",
                  config.vrt_instance);
        return Result::ERROR;
    }
    if (config.numberOfBatches == 0) {
        JST_ERROR("[MODULE_VRT_ZMQ] Number of batches cannot be zero.");
        return Result::ERROR;
    }
    if (config.numberOfTimeSamples == 0) {
        JST_ERROR("[MODULE_VRT_ZMQ] Number of time samples cannot be zero.");
        return Result::ERROR;
    }
    if (config.bufferMultiplier == 0) {
        JST_ERROR("[MODULE_VRT_ZMQ] Buffer multiplier cannot be zero.");
        return Result::ERROR;
    }

    U64 outputElements = 0;
    U64 outputSizeBytes = 0;
    if (!detail::CheckedMultiply(config.numberOfBatches, config.numberOfTimeSamples,
                                 outputElements) ||
        !detail::CheckedMultiply(outputElements, static_cast<U64>(sizeof(CF32)),
                                 outputSizeBytes)) {
        JST_ERROR("[MODULE_VRT_ZMQ] Output buffer dimensions are too large.");
        return Result::ERROR;
    }

    U64 internalElements = 0;
    U64 internalSizeBytes = 0;
    if (!detail::CheckedMultiply(outputElements, config.bufferMultiplier, internalElements) ||
        !detail::CheckedMultiply(internalElements, static_cast<U64>(sizeof(CF32)),
                                 internalSizeBytes)) {
        JST_ERROR("[MODULE_VRT_ZMQ] Internal buffer layout is too large.");
        return Result::ERROR;
    }

    validatedOutputSizeBytes = outputSizeBytes;
    validatedInternalElements = internalElements;
    validatedInternalSizeBytes = internalSizeBytes;
    return Result::SUCCESS;
}

Result VrtZmqImpl::define() {
    JST_CHECK(defineInterfaceOutput("signal"));
    return Result::SUCCESS;
}

Result VrtZmqImpl::create() {
    if (hostName.empty()) {
        JST_ERROR("[MODULE_VRT_ZMQ] No host set.");
        return Result::INCOMPLETE;
    }

    errored = false;
    streaming = false;
    bufferHealth.publish(0.0f);
    throughput.publish({0.0f, 0.0f});
    deviceGaps.publish(0);

    try {
        JST_CHECK(buffer.create(device(), DataType::CF32,
                                {numberOfBatches, numberOfTimeSamples}));
        JST_CHECK(SetSignalAxes(buffer, {
            .sample = Index{1},
            .batch = Index{0},
        }));
        JST_CHECK(stream.resize(validatedInternalElements));
    } catch (const std::bad_alloc&) {
        JST_ERROR("[MODULE_VRT_ZMQ] Failed to allocate the internal buffer.");
        return Result::ERROR;
    }
    // Samples are only accepted once the first context packet sets a generation.
    stream.reset({});

    outputs()["signal"].produced(name(), "signal", buffer);
    JST_CHECK(buffer.setAttribute("frequency", 0.0f));
    JST_CHECK(buffer.setAttribute("sampleRate", 0.0f));
    publishedMetadata = {};

    JST_CHECK(openSocket());
    if (const auto result = startReceiver(); result != Result::SUCCESS) {
        closeSocket();
        return result;
    }
    return Result::SUCCESS;
}

Result VrtZmqImpl::destroy() {
    stopReceiver();
    closeSocket();
    bufferHealth.publish(0.0f);
    throughput.publish({0.0f, 0.0f});
    return Result::SUCCESS;
}

Result VrtZmqImpl::reconfigure() {
    const auto& config = *candidate();

    if (config.hostName != hostName ||
        config.vrt_instance != vrt_instance ||
        config.vrt_channel != vrt_channel ||
        config.numberOfBatches != numberOfBatches ||
        config.numberOfTimeSamples != numberOfTimeSamples ||
        config.bufferMultiplier != bufferMultiplier) {
        return Result::RECREATE;
    }

    return Result::SUCCESS;
}

Result VrtZmqImpl::openSocket() {
    const auto port = DEFAULT_MAIN_PORT + MAX_CHANNELS * vrt_instance;
    const auto endpoint = jst::fmt::format("tcp://{}:{}", hostName, port);

    context = zmq_ctx_new();
    subscriber = context ? zmq_socket(context, ZMQ_SUB) : nullptr;
    if (!subscriber) {
        JST_ERROR("[MODULE_VRT_ZMQ] Failed to create the ZMQ socket: {}",
                  zmq_strerror(zmq_errno()));
        closeSocket();
        return Result::ERROR;
    }

    const int highWaterMark = ReceiveHighWaterMark;
    const int timeout = ReceiveTimeoutMs;
    if (zmq_setsockopt(subscriber, ZMQ_RCVHWM, &highWaterMark, sizeof(highWaterMark)) != 0 ||
        zmq_setsockopt(subscriber, ZMQ_RCVTIMEO, &timeout, sizeof(timeout)) != 0 ||
        zmq_connect(subscriber, endpoint.c_str()) != 0 ||
        zmq_setsockopt(subscriber, ZMQ_SUBSCRIBE, "", 0) != 0) {
        JST_ERROR("[MODULE_VRT_ZMQ] Failed to connect to '{}': {}",
                  endpoint, zmq_strerror(zmq_errno()));
        closeSocket();
        return Result::ERROR;
    }

    JST_DEBUG("[MODULE_VRT_ZMQ] Subscribed to '{}' channel {}.", endpoint, vrt_channel);
    return Result::SUCCESS;
}

void VrtZmqImpl::closeSocket() {
    if (subscriber) {
        zmq_close(subscriber);
        subscriber = nullptr;
    }
    if (context) {
        zmq_ctx_destroy(context);
        context = nullptr;
    }
}

Result VrtZmqImpl::publishMetadata(const Radio::StreamBuffer::Metadata& metadata) {
    if (metadata == publishedMetadata) {
        return Result::SUCCESS;
    }
    JST_CHECK(buffer.setAttribute("frequency", static_cast<F32>(metadata.frequency)));
    JST_CHECK(buffer.setAttribute("sampleRate", static_cast<F32>(metadata.sampleRate)));
    publishedMetadata = metadata;
    return Result::SUCCESS;
}

Result VrtZmqImpl::emitFrame() {
    Radio::StreamBuffer::Metadata metadata;
    const auto result = stream.pop(buffer.data<CF32>(), buffer.size(), metadata);
    if (result == Result::INCOMPLETE) {
        return Result::YIELD;
    }
    JST_CHECK(result);
    return publishMetadata(metadata);
}

Result VrtZmqImpl::startReceiver() {
    // Set before the thread starts so an early destroy() can stop it.
    streaming = true;
    try {
        receiver = std::thread([this] {
            Result result = Result::ERROR;
            try {
                result = receiveLoop();
            } catch (...) {
                result = Result::ERROR;
            }
            if (result != Result::SUCCESS) {
                errored = true;
            }
        });
    } catch (const std::exception& e) {
        streaming = false;
        JST_ERROR("[MODULE_VRT_ZMQ] Failed to start the receive thread: {}", e.what());
        return Result::ERROR;
    }
    return Result::SUCCESS;
}

void VrtZmqImpl::stopReceiver() {
    streaming = false;
    if (receiver.joinable()) {
        receiver.join();
    }
}

Result VrtZmqImpl::receiveLoop() {
    std::vector<uint32_t> packet(ZMQ_BUFFER_SIZE / sizeof(uint32_t));
    std::vector<CF32> scratch(packet.size());

    context_type vrtContext;
    init_context(&vrtContext);
    packet_type vrtPacket{};
    vrtPacket.channel_filt = 1u << vrt_channel;
    vrtPacket.first_frame = true;

    bool started = false;
    U64 generation = 0;
    U64 gaps = 0;

    while (streaming) {
        const int length = zmq_recv(subscriber, packet.data(), ZMQ_BUFFER_SIZE, 0);
        if (!streaming) {
            break;
        }
        if (length < 0) {
            const int error = zmq_errno();
            if (error == EAGAIN || error == EINTR) {
                continue;
            }
            JST_ERROR("[MODULE_VRT_ZMQ] Reception stopped on '{}': {}", name(),
                      zmq_strerror(error));
            return Result::ERROR;
        }

        // zmq_recv returns the full message size even when truncated.
        const auto words = static_cast<uint32_t>(
            std::min<std::size_t>(length, ZMQ_BUFFER_SIZE) / sizeof(uint32_t));
        if (words == 0 || !vrt_process(packet.data(), words, &vrtContext, &vrtPacket)) {
            continue;
        }

        if (vrtPacket.context) {
            // Data packets are decoded as ci16, so quantized streams would show as noise.
            if (vrtContext.has_payload_format && vrtContext.data_item_size != 16) {
                JST_ERROR("[MODULE_VRT_ZMQ] Channel {} carries {}-bit samples, VRT-ZMQ "
                          "requires 16-bit. Insert 'vrt_quantize --unpack' ahead of it.",
                          vrt_channel, static_cast<unsigned>(vrtContext.data_item_size));
                return Result::ERROR;
            }

            const F64 frequency = static_cast<F64>(vrtContext.rf_freq) + vrtContext.rf_frac_freq;
            const F64 sampleRate = static_cast<F64>(vrtContext.sample_rate);
            const auto current = stream.metadata();
            if (!started || frequency != current.frequency || sampleRate != current.sampleRate) {
                // A new generation drops samples labeled with the previous context.
                stream.reset({
                    .frequency = frequency,
                    .sampleRate = sampleRate,
                    .generation = ++generation,
                });
                started = true;
            }
        }

        if (!vrtPacket.data || !started) {
            continue;
        }

        if (vrtPacket.lost_frame) {
            stream.split(generation);
            deviceGaps.publish(++gaps);
        }

        const U64 count = std::min<U64>(vrtPacket.num_rx_samps,
                                        words > vrtPacket.offset ? words - vrtPacket.offset : 0);
        for (U64 i = 0; i < count; ++i) {
            int16_t re;
            int16_t im;
            const auto* item = reinterpret_cast<const char*>(&packet[vrtPacket.offset + i]);
            std::memcpy(&re, item, sizeof(re));
            std::memcpy(&im, item + sizeof(re), sizeof(im));
            scratch[i] = CF32(static_cast<F32>(re) / 32768.0f, static_cast<F32>(im) / 32768.0f);
        }
        stream.push(generation, scratch.data(), count);

        const U64 capacity = stream.capacity();
        if (capacity > 0) {
            const F32 level = static_cast<F32>(stream.size()) / static_cast<F32>(capacity);
            bufferHealth.publish(bufferHealth.get() * 0.99f + level * 0.01f);
        }
        const auto current = stream.metadata();
        throughput.publish({
            static_cast<F32>(stream.throughput() * sizeof(CF32) / 1e6),
            static_cast<F32>(current.sampleRate * sizeof(CF32) / 1e6),
        });
    }
    return Result::SUCCESS;
}

F32 VrtZmqImpl::getBufferHealth() const {
    return bufferHealth.get();
}

VrtZmqImpl::LossSnapshot VrtZmqImpl::getLoss() const {
    const auto stats = stream.statistics();
    return {
        .bufferLoss = stats.pushedSamples > 0
            ? static_cast<F64>(stats.overwrittenSamples) / static_cast<F64>(stats.pushedSamples)
            : 0.0,
        .deviceGaps = deviceGaps.get(),
    };
}

std::pair<F32, F32> VrtZmqImpl::getThroughput() const {
    return throughput.get();
}

Radio::StreamBuffer::Metadata VrtZmqImpl::getMetadata() const {
    return stream.metadata();
}

}  // namespace Jetstream::Modules
