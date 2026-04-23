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
    if (circularBuffer.getOccupancy() < buffer.size()) {
        return circularBuffer.waitBufferOccupancy(buffer.size());
    }

    return Result::SUCCESS;
}

Result VrtZmqImplNativeCpu::computeSubmit() {
    if (errored) {
        return Result::ERROR;
    }

    if (circularBuffer.getOccupancy() < buffer.size()) {
        return Result::YIELD;
    }

    circularBuffer.get(reinterpret_cast<CF32*>(buffer.data()), buffer.size());

    return Result::SUCCESS;
}

JST_REGISTER_MODULE(VrtZmqImplNativeCpu, DeviceType::CPU, RuntimeType::NATIVE, "generic");

}  // namespace Jetstream::Modules
