#ifndef JETSTREAM_DOMAINS_IO_SOAPY_MODULE_IMPL_HH
#define JETSTREAM_DOMAINS_IO_SOAPY_MODULE_IMPL_HH

#include <thread>
#include <atomic>
#include <vector>
#include <map>

#include <jetstream/domains/io/vrtzmq/module.hh>
#include <jetstream/detail/module_impl.hh>
#include <jetstream/tools/circular_buffer.hh>
#include <jetstream/tools/snapshot.hh>

namespace Jetstream::Modules {

struct VrtZmqImpl : public Module::Impl, public DynamicConfig<VrtZmq> {
 public:
    
    Result define() override;
    Result create() override;
    Result destroy() override;
    Result reconfigure() override;

    F32 getBufferHealth() const;
    std::pair<F32, F32> getThroughput() const;

    F64 getVRTFreq() const;
    U32 getVRTRate() const;



 protected:
    Tensor buffer;

    // VRT
    int64_t current_freq;
    uint32_t current_sample_rate;

    // VRT ZMQ
    void *context = NULL;
    void *subscriber = NULL;

    uint32_t sample_counter = 0;

    std::thread producer;
    std::atomic<bool> errored{false};
    std::atomic<bool> streaming{false};

    Tools::CircularBuffer<CF32> circularBuffer;
    Tools::Snapshot<F32> bufferHealth{0.0f};
    Tools::Snapshot<std::pair<F32, F32>> throughput{{0.0f, 0.0f}};

    Result VrtThreadLoop();

};

}  // namespace Jetstream::Modules

#endif  // JETSTREAM_DOMAINS_IO_SOAPY_MODULE_IMPL_HH
