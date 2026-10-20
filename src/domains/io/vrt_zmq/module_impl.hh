#ifndef JETSTREAM_DOMAINS_IO_VRT_ZMQ_MODULE_IMPL_HH
#define JETSTREAM_DOMAINS_IO_VRT_ZMQ_MODULE_IMPL_HH

#include <atomic>
#include <thread>

#include <jetstream/domains/io/vrt_zmq/module.hh>
#include <jetstream/detail/module_impl.hh>
#include <jetstream/tools/snapshot.hh>

#include "domains/io/sdr/tools/stream_buffer.hh"

namespace Jetstream::Modules {

struct JETSTREAM_API VrtZmqImpl : public Module::Impl, public DynamicConfig<VrtZmq> {
 public:
    struct LossSnapshot {
        F64 bufferLoss = 0.0;
        U64 deviceGaps = 0;
    };

    ~VrtZmqImpl() override;

    Result validate() override;
    Result define() override;
    Result create() override;
    Result destroy() override;
    Result reconfigure() override;

    F32 getBufferHealth() const;
    LossSnapshot getLoss() const;
    std::pair<F32, F32> getThroughput() const;
    Radio::StreamBuffer::Metadata getMetadata() const;

 protected:
    Tensor buffer;

    U64 validatedOutputSizeBytes = 0;
    U64 validatedInternalElements = 0;
    U64 validatedInternalSizeBytes = 0;

    std::thread receiver;
    std::atomic<bool> errored{false};
    std::atomic<bool> streaming{false};

    Radio::StreamBuffer stream;
    Radio::StreamBuffer::Metadata publishedMetadata;

    Tools::Snapshot<F32> bufferHealth{0.0f};
    Tools::Snapshot<std::pair<F32, F32>> throughput{{0.0f, 0.0f}};
    Tools::Snapshot<U64> deviceGaps{0};

    Result receiveLoop();
    Result emitFrame();
    Result publishMetadata(const Radio::StreamBuffer::Metadata& metadata);

 private:
    void* context = nullptr;
    void* subscriber = nullptr;

    Result openSocket();
    void closeSocket();
    Result startReceiver();
    void stopReceiver();
};

}  // namespace Jetstream::Modules

#endif  // JETSTREAM_DOMAINS_IO_VRT_ZMQ_MODULE_IMPL_HH
