#include <chrono>

#include <jetstream/runtime_context_native_cpu.hh>
#include <jetstream/scheduler_context.hh>
#include <jetstream/module_context.hh>
#include <jetstream/registry.hh>

#include "module_impl.hh"

namespace Jetstream::Modules {

struct VrtZmqImplNativeCpu : public VrtZmqImpl,
                            public NativeCpuRuntimeContext,
                            public Scheduler::Context {
 public:
    Result create() final;

    Result computeSubmit() override;
    Result hasPendingCompute() override;
};

Result VrtZmqImplNativeCpu::create() {
    JST_CHECK(VrtZmqImpl::create());

    return Result::SUCCESS;
}

Result VrtZmqImplNativeCpu::hasPendingCompute() {
    if (errored) {
        return Result::ERROR;
    }

    if (circularBuffer.size() < buffer.size()) {
        // Keep waits bounded so receive-thread failures reach the scheduler.
        const auto result = circularBuffer.waitForSize(buffer.size(),
                                                       std::chrono::milliseconds(100));
        return errored ? Result::ERROR : result;
    }

    return Result::SUCCESS;
}

Result VrtZmqImplNativeCpu::computeSubmit() {
    if (errored) {
        return Result::ERROR;
    }

    if (circularBuffer.size() < buffer.size()) {
        return Result::YIELD;
    }

    JST_CHECK(circularBuffer.pop(
        reinterpret_cast<CF32*>(buffer.data()), buffer.size()));

    return Result::SUCCESS;
}

JST_REGISTER_MODULE(VrtZmqImplNativeCpu, DeviceType::CPU, RuntimeType::NATIVE, "generic");

}  // namespace Jetstream::Modules
