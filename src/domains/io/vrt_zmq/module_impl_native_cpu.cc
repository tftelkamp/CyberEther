#include <chrono>
#include <limits>

#include <jetstream/memory/macros.hh>
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
    Result validate() final;

    Result computeSubmit() override;
    Result hasPendingCompute() override;
};

Result VrtZmqImplNativeCpu::validate() {
    JST_CHECK(VrtZmqImpl::validate());

    U64 alignedOutputSize = 0;
    if (!detail::CheckedPageAlignedSize(validatedOutputSizeBytes, alignedOutputSize) ||
        alignedOutputSize > std::numeric_limits<std::size_t>::max() ||
        validatedInternalSizeBytes > std::numeric_limits<std::size_t>::max()) {
        JST_ERROR("[MODULE_VRT_ZMQ_NATIVE_CPU] Output or internal buffer allocation size is too large.");
        return Result::ERROR;
    }

    return Result::SUCCESS;
}

Result VrtZmqImplNativeCpu::hasPendingCompute() {
    if (errored) {
        return Result::ERROR;
    }

    if (stream.size() < buffer.size()) {
        // Keep waits bounded so receive-thread failures reach the scheduler.
        const auto result = stream.waitForSize(buffer.size(), std::chrono::milliseconds(100));
        return errored ? Result::ERROR : result;
    }

    return Result::SUCCESS;
}

Result VrtZmqImplNativeCpu::computeSubmit() {
    if (errored) {
        return Result::ERROR;
    }

    return emitFrame();
}

JST_REGISTER_MODULE(VrtZmqImplNativeCpu, DeviceType::CPU, RuntimeType::NATIVE, "generic");

}  // namespace Jetstream::Modules
