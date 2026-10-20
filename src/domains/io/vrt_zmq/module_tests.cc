#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <vrt/vrt_init.h>
#include <vrt/vrt_types.h>
#include <vrt/vrt_write.h>
#include <zmq.h>

#include "jetstream/domains/io/vrt_zmq/module.hh"
#include "jetstream/module_context.hh"
#include "jetstream/registry.hh"
#include "jetstream/runtime_context_native_cpu.hh"
#include "jetstream/scheduler_context.hh"
#include "module_impl.hh"

using namespace Jetstream;

namespace {

// Matches DEFAULT_MAIN_PORT and MAX_CHANNELS in vrt-tools.h.
constexpr U64 Instance = 77;
constexpr U16 Port = 50100 + 10 * Instance;
constexpr U64 Channel = 2;
constexpr U64 Samples = 64;
// Symmetric bytes so the value does not depend on payload byte order.
constexpr int16_t Item = 0x0101;
constexpr int16_t OtherChannelItem = 0x0303;

template<typename Predicate>
bool WaitFor(Predicate predicate, const std::chrono::milliseconds limit = std::chrono::seconds(5)) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

// Publishes one context and one data packet per channel until stopped.
class Publisher {
 public:
    std::atomic<F64> frequency{433.92e6};
    std::atomic<F64> sampleRate{2.0e6};
    std::atomic<bool> skipCounts{false};
    // Bits per component announced in the context payload format, 0 for none.
    std::atomic<U8> bits{0};

    Publisher() {
        context = zmq_ctx_new();
        socket = zmq_socket(context, ZMQ_PUB);
        const auto endpoint = "tcp://127.0.0.1:" + std::to_string(Port);
        REQUIRE(zmq_bind(socket, endpoint.c_str()) == 0);
        worker = std::thread([this] { run(); });
    }

    ~Publisher() {
        stopping = true;
        worker.join();
        zmq_close(socket);
        zmq_ctx_destroy(context);
    }

 private:
    void run() {
        std::vector<uint32_t> wire(4096);
        U8 count = 0;
        while (!stopping) {
            send(wire, contextPacket(1u << Channel));
            send(wire, dataPacket(1u << Channel, Item, count));
            send(wire, dataPacket(1u << (Channel + 1), OtherChannelItem, count));
            count = static_cast<U8>((count + (skipCounts ? 3 : 1)) % 16);
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    }

    vrt_packet contextPacket(const uint32_t streamId) {
        vrt_packet packet;
        vrt_init_packet(&packet);
        packet.header.packet_type = VRT_PT_IF_CONTEXT;
        packet.fields.stream_id = streamId;
        packet.if_context.has.sample_rate = true;
        packet.if_context.sample_rate = sampleRate.load();
        packet.if_context.has.rf_reference_frequency = true;
        packet.if_context.rf_reference_frequency = frequency.load();
        if (const U8 size = bits.load(); size > 0) {
            // The raw fields hold one less than the actual size.
            auto& format = packet.if_context.data_packet_payload_format;
            packet.if_context.has.data_packet_payload_format = true;
            format.packing_method = VRT_PM_LINK_EFFICIENT;
            format.real_or_complex = VRT_ROC_COMPLEX_CARTESIAN;
            format.data_item_format = VRT_DIF_SIGNED_FIXED_POINT;
            format.item_packing_field_size = static_cast<uint8_t>(2 * size - 1);
            format.data_item_size = static_cast<uint8_t>(size - 1);
        }
        return packet;
    }

    vrt_packet dataPacket(const uint32_t streamId, const int16_t value, const U8 count) {
        payload.resize(Samples);
        for (auto& word : payload) {
            std::memcpy(&word, &value, sizeof(value));
            std::memcpy(reinterpret_cast<char*>(&word) + sizeof(value), &value, sizeof(value));
        }
        vrt_packet packet;
        vrt_init_packet(&packet);
        packet.header.packet_type = VRT_PT_IF_DATA_WITH_STREAM_ID;
        packet.header.packet_count = count;
        packet.fields.stream_id = streamId;
        packet.body = payload.data();
        packet.words_body = static_cast<int32_t>(payload.size());
        return packet;
    }

    void send(std::vector<uint32_t>& wire, const vrt_packet& packet) {
        const auto words = vrt_write_packet(&packet, wire.data(),
                                            static_cast<int32_t>(wire.size()), true);
        REQUIRE(words > 0);
        zmq_send(socket, wire.data(), static_cast<std::size_t>(words) * sizeof(uint32_t), 0);
    }

    void* context = nullptr;
    void* socket = nullptr;
    std::vector<uint32_t> payload;
    std::atomic<bool> stopping{false};
    std::thread worker;
};

std::shared_ptr<Module> BuildVrtZmqModule() {
    const auto implementations = Registry::ListAvailableModules("vrtzmq");
    REQUIRE_FALSE(implementations.empty());
    const auto& implementation = implementations.front();
    std::shared_ptr<Module> module;
    REQUIRE(Registry::BuildModule("vrtzmq", implementation.device, implementation.runtime,
                                  implementation.provider, module) == Result::SUCCESS);
    return module;
}

struct ModuleCleanup {
    std::shared_ptr<Module> module;

    ~ModuleCleanup() {
        if (module->state() == Module::State::CREATED) {
            static_cast<void>(module->destroy());
        }
    }
};

Modules::VrtZmq TestConfig() {
    Modules::VrtZmq config;
    config.hostName = "127.0.0.1";
    config.vrt_instance = Instance;
    config.vrt_channel = Channel;
    config.numberOfBatches = 1;
    config.numberOfTimeSamples = Samples * 2;
    config.bufferMultiplier = 4;
    return config;
}

F32 AttributeOf(const Tensor& tensor, const std::string& key) {
    return std::any_cast<F32>(tensor.attribute(key));
}

std::shared_ptr<NativeCpuRuntimeContext> RuntimeOf(const std::shared_ptr<Module>& module) {
    auto runtime = std::dynamic_pointer_cast<NativeCpuRuntimeContext>(module->context()->runtime());
    REQUIRE(runtime != nullptr);
    return runtime;
}

// Pulls frames until one carries the expected frequency label.
bool EmitLabeled(const std::shared_ptr<Module>& module, const F32 frequency) {
    const auto runtime = RuntimeOf(module);
    const auto& output = module->outputs().at("signal").tensor;
    return WaitFor([&] {
        if (module->context()->scheduler()->hasPendingCompute() != Result::SUCCESS) {
            return false;
        }
        return runtime->computeSubmit() == Result::SUCCESS &&
               AttributeOf(output, "frequency") == frequency;
    });
}

}  // namespace

TEST_CASE("VRT-ZMQ outputs only the selected channel, labeled by its context",
          "[modules][io][vrt_zmq]") {
    Publisher publisher;
    const auto module = BuildVrtZmqModule();
    const ModuleCleanup cleanup{module};
    REQUIRE(module->create("test", TestConfig(), {}) == Result::SUCCESS);
    const auto& output = module->outputs().at("signal").tensor;

    REQUIRE(EmitLabeled(module, 433.92e6f));
    REQUIRE(AttributeOf(output, "sampleRate") == 2.0e6f);
    const F32 expected = static_cast<F32>(Item) / 32768.0f;
    for (U64 i = 0; i < output.size(); ++i) {
        REQUIRE(output.data<CF32>()[i] == CF32(expected, expected));
    }
    REQUIRE(module->getImpl<Modules::VrtZmqImpl>()->getLoss().deviceGaps == 0);
}

TEST_CASE("VRT-ZMQ context changes relabel only samples received after them",
          "[modules][io][vrt_zmq][metadata]") {
    Publisher publisher;
    const auto module = BuildVrtZmqModule();
    const ModuleCleanup cleanup{module};
    REQUIRE(module->create("test", TestConfig(), {}) == Result::SUCCESS);
    const auto& output = module->outputs().at("signal").tensor;

    REQUIRE(EmitLabeled(module, 433.92e6f));
    publisher.sampleRate = 4.0e6;
    publisher.frequency = 100.0e6;
    REQUIRE(EmitLabeled(module, 100.0e6f));
    REQUIRE(AttributeOf(output, "sampleRate") == 4.0e6f);
    REQUIRE(module->getImpl<Modules::VrtZmqImpl>()->getMetadata().frequency == 100.0e6);
}

TEST_CASE("VRT-ZMQ packet-count breaks show as device gaps",
          "[modules][io][vrt_zmq][metrics]") {
    Publisher publisher;
    const auto module = BuildVrtZmqModule();
    const ModuleCleanup cleanup{module};
    REQUIRE(module->create("test", TestConfig(), {}) == Result::SUCCESS);
    auto* impl = module->getImpl<Modules::VrtZmqImpl>();

    REQUIRE(EmitLabeled(module, 433.92e6f));
    REQUIRE(impl->getLoss().deviceGaps == 0);
    publisher.skipCounts = true;
    REQUIRE(WaitFor([&] { return impl->getLoss().deviceGaps > 0; }));
}

TEST_CASE("VRT-ZMQ accepts streams that announce 16-bit samples",
          "[modules][io][vrt_zmq][format]") {
    Publisher publisher;
    publisher.bits = 16;
    const auto module = BuildVrtZmqModule();
    const ModuleCleanup cleanup{module};
    REQUIRE(module->create("test", TestConfig(), {}) == Result::SUCCESS);
    const auto& output = module->outputs().at("signal").tensor;

    REQUIRE(EmitLabeled(module, 433.92e6f));
    const F32 expected = static_cast<F32>(Item) / 32768.0f;
    REQUIRE(output.data<CF32>()[0] == CF32(expected, expected));
}

TEST_CASE("VRT-ZMQ refuses quantized streams instead of decoding them as 16-bit",
          "[modules][io][vrt_zmq][format]") {
    Publisher publisher;
    publisher.bits = 2;
    const auto module = BuildVrtZmqModule();
    const ModuleCleanup cleanup{module};
    REQUIRE(module->create("test", TestConfig(), {}) == Result::SUCCESS);
    const auto runtime = RuntimeOf(module);

    REQUIRE(WaitFor([&] {
        return module->context()->scheduler()->hasPendingCompute() == Result::ERROR;
    }));
    REQUIRE(runtime->computeSubmit() == Result::ERROR);
    REQUIRE(module->getImpl<Modules::VrtZmqImpl>()->getLoss().bufferLoss == 0.0);
}

TEST_CASE("VRT-ZMQ rejects channels and instances outside the port layout",
          "[modules][io][vrt_zmq][validation]") {
    const auto module = BuildVrtZmqModule();
    const ModuleCleanup cleanup{module};

    auto config = TestConfig();
    config.vrt_channel = 10;
    REQUIRE(module->create("test", config, {}) == Result::ERROR);

    config = TestConfig();
    config.vrt_instance = 10000;
    REQUIRE(module->create("test", config, {}) == Result::ERROR);
}
