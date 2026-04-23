#include "module_impl.hh"

// VRT
#include <vrt/vrt_init.h>
#include <vrt/vrt_string.h>
#include <vrt/vrt_types.h>
#include <vrt/vrt_util.h>
#include <vrt/vrt_write.h>
#include <vrt/vrt_read.h>

#include <zmq.h>

#include "vrt-tools.h"

namespace Jetstream::Modules {

Result VrtZmqImpl::define() {
    JST_CHECK(defineInterfaceOutput("signal"));

    return Result::SUCCESS;
}

Result VrtZmqImpl::create() {

    errored = false;
    streaming = false;
    bufferHealth.publish(0.0f);
    throughput.publish({0.0f, 0.0f});

    int hwm = 10000;
    uint16_t main_port = DEFAULT_MAIN_PORT + MAX_CHANNELS*vrt_instance;

    // if (context == NULL)
    context = zmq_ctx_new();
    subscriber = zmq_socket(context, ZMQ_SUB);
    int rc = zmq_setsockopt (subscriber, ZMQ_RCVHWM, &hwm, sizeof hwm);
    std::string connect_string = "tcp://" + hostName + ":" + std::to_string(main_port);
    rc = zmq_connect(subscriber, connect_string.c_str());
    assert(rc == 0);
    zmq_setsockopt(subscriber, ZMQ_SUBSCRIBE, "", 0);

    JST_CHECK(buffer.create(device(), DataType::CF32, {numberOfBatches, numberOfTimeSamples}));

    outputs()["signal"].produced(name(), "signal", buffer);

    buffer.setAttribute("frequency", (F32)current_freq);
    buffer.setAttribute("sampleRate", (F32)current_sample_rate);

    circularBuffer.resize(buffer.size() * bufferMultiplier);

    producer = std::thread([this] {
        try {
            JST_CHECK_THROW(VrtThreadLoop());
        } catch (...) {
            errored = true;
            JST_FATAL("[MODULE_VRTZMQ] Device thread crashed.");
        }
    });

    return Result::SUCCESS;
}

Result VrtZmqImpl::destroy() {
    streaming = false;

    if (producer.joinable()) {
        producer.join();
    }

    zmq_close(subscriber);
    zmq_ctx_destroy(context);

    bufferHealth.publish(0.0f);
    throughput.publish({0.0f, 0.0f});

    return Result::SUCCESS;
}

Result VrtZmqImpl::reconfigure() {
    const auto& newConfig = *candidate();

    if (
        newConfig.hostName != hostName ||
        newConfig.vrt_instance != vrt_instance ||
        newConfig.vrt_channel != vrt_channel ||
        newConfig.numberOfTimeSamples != numberOfTimeSamples ||
        newConfig.bufferMultiplier != bufferMultiplier) {
        return Result::RECREATE;
    }

    return Result::SUCCESS;
}

Result VrtZmqImpl::VrtThreadLoop() {

    CF32 tmp[10000];

    // VRT
    uint32_t zmq_buffer[ZMQ_BUFFER_SIZE];

    context_type vrt_context;
    packet_type vrt_packet;

    init_context(&vrt_context);
    uint32_t use_channel = vrt_channel;
    vrt_packet.channel_filt = 1<<use_channel;

    bool start_rx = false;

    sample_counter = 0;
    streaming = true;

    while (streaming) {
        try {
            int len = zmq_recv(subscriber, zmq_buffer, ZMQ_BUFFER_SIZE, ZMQ_DONTWAIT);

            if (streaming && len > 0) {

                if (not vrt_process(zmq_buffer, sizeof(zmq_buffer), &vrt_context, &vrt_packet)) {
                    printf("Not a Vita49 packet?\n");
                    continue;
                }

                if (vrt_packet.context) {
                    if (current_sample_rate != vrt_context.sample_rate) {
                        current_sample_rate = vrt_context.sample_rate;
                        buffer.setAttribute("sampleRate", (F32)current_sample_rate);
                    }
                    if (current_freq != vrt_context.rf_freq) {
                        current_freq = vrt_context.rf_freq;
                        buffer.setAttribute("frequency", (F32)current_freq);
                    }
                    start_rx = true;
                }

                if (vrt_packet.data && start_rx && streaming && !errored) {

                    for (uint32_t i = 0; i < vrt_packet.num_rx_samps; i++) {

                        int16_t re;
                        memcpy(&re, (char*)&zmq_buffer[vrt_packet.offset+i], 2);
                        int16_t img;
                        memcpy(&img, (char*)&zmq_buffer[vrt_packet.offset+i]+2, 2);

                        tmp[sample_counter] = std::complex((float)re / 32768.0, (float)img / 32768.0);
                        sample_counter++;
                        
                        if (sample_counter == numberOfTimeSamples) {
                            circularBuffer.put(tmp, numberOfTimeSamples);
                            sample_counter = 0;
                        }
                        
                    }

                    const U64 capacity = circularBuffer.getCapacity();
                    if (capacity > 0) {
                        const F32 newHealth = static_cast<F32>(circularBuffer.getOccupancy()) /
                                              static_cast<F32>(capacity);
                        const F32 smoothedHealth = bufferHealth.get() * 0.99f + newHealth * 0.01f;
                        bufferHealth.publish(smoothedHealth);
                    }
                    const F32 actualMB = static_cast<F32>(circularBuffer.getThroughput() * sizeof(CF32)) / 1e6f;
                    const F32 expectedMB = (current_sample_rate * sizeof(CF32)) / 1e6f;
                    throughput.publish({actualMB, expectedMB});
                }
            }

            usleep(5);

        } catch (const std::exception& e) {
            JST_ERROR("[MODULE_VRTZMQ] Failed to read stream: {}", e.what());
            errored = true;
            break;
        } catch (...) {
            JST_ERROR("[MODULE_VRTZMQ] Failed to read stream.");
            errored = true;
            break;
        }
    }

    return Result::SUCCESS;
}

F32 VrtZmqImpl::getBufferHealth() const {
    return bufferHealth.get();
}

F64 VrtZmqImpl::getVRTFreq() const {
    return current_freq;
}

U32 VrtZmqImpl::getVRTRate() const {
    return current_sample_rate;
}

std::pair<F32, F32> VrtZmqImpl::getThroughput() const {
    return throughput.get();
}

}  // namespace Jetstream::Modules
