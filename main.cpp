#include <websocketpp/config/asio_no_tls_client.hpp>
#include <websocketpp/client.hpp>
#include <simdjson.h>
#include <iostream>
#include <ostream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <csignal>
#include <atomic>
#include <optional>
#include <string>
#include <array>
#include <cstring>

typedef websocketpp::client<websocketpp::config::asio_client> client;
client c;

// Структура для хранения данных одной сделки (выравнивание для оптимизации)
struct alignas(64) Trade {
    char s[12];  // Symbol name, 12 байт
    char S[4];   // Side (Buy/Sell), 4 байта
    char v[16];  // Trade size, 16 байт
    char p[16];  // Trade price, 16 байт
    bool BT;     // Block trade, 1 байт
    bool RPI;    // RPI trade, 1 байт
    char padding[10]; // Выравнивание до 64 байт (12+4+16+16+1+1+10=60+4=64)
};

// Структура буфера: ts + массив сделок
struct Buffer {
    int64_t ts;                         // Временная метка сообщения
    std::array<Trade, 512> trades;      // Массив на 512 сделок
    size_t size = 0;                    // Количество заполненных сделок
};

// Два статических буфера
Buffer buffer1;
Buffer buffer2;
std::atomic<bool> use_buffer1{true}; // Переключатель между буферами

std::mutex bufferMutex;
std::condition_variable bufferCV;
std::atomic<bool> running{true};

// Кольцевой буфер для входящих сообщений
constexpr size_t RING_BUFFER_SIZE = 10;
std::array<simdjson::padded_string, RING_BUFFER_SIZE> ringBuffer;
size_t ringBuffer_head = 0;
size_t ringBuffer_tail = 0;
std::atomic<size_t> ringBuffer_count{0};

void print_simdjson_info() {
    std::cout << "✅ simdjson is using: " 
              << simdjson::get_active_implementation()->name() 
              << " (" << simdjson::get_active_implementation()->description() 
              << ")\n" << std::flush;
}

void signal_handler(int signal) {
    std::cout << "\n⚠ Received signal " << signal << ", stopping...\n" << std::flush;
    running = false;
    bufferCV.notify_all();
    c.stop();
}

void write_to_buffer(const std::string& data) {
    std::unique_lock<std::mutex> lock(bufferMutex);
    if (ringBuffer_count.load() >= RING_BUFFER_SIZE) {
        std::cerr << "⚠ Warning: Ring buffer is full, dropping data!\n" << std::flush;
        return;
    }
    ringBuffer[ringBuffer_tail] = simdjson::padded_string(data);
    ringBuffer_tail = (ringBuffer_tail + 1) % RING_BUFFER_SIZE;
    ringBuffer_count.fetch_add(1);
    lock.unlock();
    std::cout << "📥 Data added to buffer\n" << std::flush;
    bufferCV.notify_one();
}

void parse_buffer() {
    std::cout << "🚀 Parser thread started\n" << std::flush;
    simdjson::ondemand::parser parser;

    long long total_parsing_time = 0;
    int message_count = 0;

    while (running.load()) {
        std::unique_lock<std::mutex> lock(bufferMutex);
        bufferCV.wait(lock, [] { return ringBuffer_count.load() > 0 || !running.load(); });

        if (!running.load() && ringBuffer_count.load() == 0) {
            std::cout << "🏁 Parser thread exiting\n" << std::flush;
            if (message_count > 0) {
                std::cout << "⏱ Total parsing time (active): " << total_parsing_time << " microseconds (" 
                          << total_parsing_time / 1000.0 << " ms)\n" << std::flush;
                std::cout << "⏱ Average time per message: " << total_parsing_time / message_count << " microseconds\n" << std::flush;
            } else {
                std::cout << "⏱ No messages parsed\n" << std::flush;
            }
            break;
        }

        if (ringBuffer_count.load() > 0) {
            simdjson::padded_string buffer = std::move(ringBuffer[ringBuffer_head]);
            ringBuffer_head = (ringBuffer_head + 1) % RING_BUFFER_SIZE;
            ringBuffer_count.fetch_sub(1);
            lock.unlock();

            auto start = std::chrono::high_resolution_clock::now();

            try {
                std::cout << "📦 JSON size: " << buffer.size() << " bytes\n" << std::flush;
                auto doc = parser.iterate(buffer);
                int64_t ts = doc["ts"].get_int64();
                auto data = doc["data"].get_array();

                size_t trade_count = 0;
                for (auto trade : data) trade_count++;
                std::cout << "📊 Trade count: " << trade_count << "\n" << std::flush;

                if (use_buffer1.load()) {
                    if (buffer1.size + trade_count > buffer1.trades.size()) {
                        use_buffer1 = false;
                        buffer2.size = 0;
                    }
                } else {
                    if (buffer2.size + trade_count > buffer2.trades.size()) {
                        use_buffer1 = true;
                        buffer1.size = 0;
                    }
                }

                Buffer& target_buffer = use_buffer1.load() ? buffer1 : buffer2;
                target_buffer.ts = ts;

                for (auto trade : data) {
                    Trade& t = target_buffer.trades[target_buffer.size];

                    std::string_view s_sv = trade["s"].get_string().value();
                    std::string_view S_sv = trade["S"].get_string().value();
                    std::string_view v_sv = trade["v"].get_string().value();
                    std::string_view p_sv = trade["p"].get_string().value();

                    size_t s_len = std::min(s_sv.size(), sizeof(t.s) - 1);
                    memcpy(t.s, s_sv.data(), s_len);
                    t.s[s_len] = '\0';

                    size_t S_len = std::min(S_sv.size(), sizeof(t.S) - 1);
                    memcpy(t.S, S_sv.data(), S_len);
                    t.S[S_len] = '\0';

                    size_t v_len = std::min(v_sv.size(), sizeof(t.v) - 1);
                    memcpy(t.v, v_sv.data(), v_len);
                    t.v[v_len] = '\0';

                    size_t p_len = std::min(p_sv.size(), sizeof(t.p) - 1);
                    memcpy(t.p, p_sv.data(), p_len);
                    t.p[p_len] = '\0';

                    t.BT = (trade["BT"].type() == simdjson::ondemand::json_type::boolean) && trade["BT"].get_bool();
                    t.RPI = (trade["RPI"].type() == simdjson::ondemand::json_type::boolean) && trade["RPI"].get_bool();

                    target_buffer.size++;
                }

                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
                std::cout << "⏱ Parsing time for this message: " << duration << " microseconds\n" << std::flush;
                total_parsing_time += duration;
                message_count++;

            } catch (const std::exception& e) {
                std::cerr << "❌ Exception during parsing: " << e.what() << "\n" << std::flush;
            }
        }
    }
}

void on_message(websocketpp::connection_hdl, client::message_ptr msg) {
    std::cout << "📩 WebSocket message received\n" << std::flush;
    write_to_buffer(msg->get_payload());
}

void websocket_thread() {
    std::cout << "🚀 WebSocket thread started\n" << std::flush;
    try {
        c.run();
    } catch (const std::exception& e) {
        std::cerr << "❌ WebSocket error: " << e.what() << "\n" << std::flush;
        running = false;
        bufferCV.notify_all();
    }
    std::cout << "🏁 WebSocket thread exiting\n" << std::flush;
}

int main() {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    std::cout << "Starting program...\n" << std::flush;
    print_simdjson_info();
    std::cout << "Initialization complete\n" << std::flush;

    c.init_asio();
    c.set_open_handler([](websocketpp::connection_hdl) { 
        std::cout << "🔗 Connected\n" << std::flush; 
    });
    c.set_close_handler([](websocketpp::connection_hdl) { 
        std::cout << "❌ Disconnected\n" << std::flush; 
    });
    c.set_message_handler(on_message);

    websocketpp::lib::error_code ec;
    auto con = c.get_connection("ws://localhost:8765", ec);
    if (ec) {
        std::cerr << "❌ Connection error: " << ec.message() << "\n" << std::flush;
        return 1;
    }
    c.connect(con);

    std::thread parserThread(parse_buffer);
    std::thread wsThread(websocket_thread);

    wsThread.join();
    parserThread.join();

    std::cout << "✅ Program exited cleanly\n" << std::flush;
    return 0;
}
