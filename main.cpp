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
#include "concurrentqueue.h"

typedef websocketpp::client<websocketpp::config::asio_client> client;
client c;

// Структура для хранения данных одной сделки (выравнивание)
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
    int64_t ts = 0;                     // Временная метка сообщения
    std::array<Trade, 512> trades;      // Массив на 512 сделок
    size_t size = 0;                    // Количество заполненных сделок
    bool ready = false;                 // Флаг готовности буфера

    void clean() {
        ts = 0;
        size = 0;
        ready = false;
        std::fill(trades.begin(), trades.end(), Trade{});
    }
};

// Три буфера на CPU
Buffer buffer1;
Buffer buffer2;
Buffer buffer3;
std::atomic<int> state{0}; // Счётчик состояния (0, 1, 2)
std::mutex stateMutex;
std::condition_variable cv; // Условная переменная для синхронизации
std::atomic<bool> running{true};
moodycamel::ConcurrentQueue<std::string> messageQueue;

void print_simdjson_info() {
    std::cout << "✅ simdjson is using: " 
              << simdjson::get_active_implementation()->name() 
              << " (" << simdjson::get_active_implementation()->description() 
              << ")\n" << std::flush;
}

void signal_handler(int signal) {
    std::cout << "\n⚠ Received signal " << signal << ", stopping...\n" << std::flush;
    running = false;
    c.stop();
    cv.notify_all(); // Разбудить все ждущие потоки при остановке
}

void write_to_buffer(const std::string& data) {
    messageQueue.enqueue(data);
    std::cout << "📥 Data added to queue\n" << std::flush;
}

// Функция чтения буфера
void read_buffer(Buffer& buffer) {
    if (buffer.size > 0 && buffer.ready) {
        std::cout << "📖 Read " << buffer.size << " trades from buffer (ts: " << buffer.ts << ")\n" << std::flush;
        buffer.ready = false; // Сбрасываем флаг после чтения
    } else {
        std::cout << "⚠ Buffer not ready or empty (size: " << buffer.size << ", ready: " << buffer.ready << ")\n" << std::flush;
    }
}

void parse_buffer() {
    std::cout << "🚀 Parser thread started\n" << std::flush;
    simdjson::ondemand::parser parser;

    while (running.load() || !messageQueue.size_approx() == 0) {
        std::string message;
        if (!messageQueue.try_dequeue(message)) {
            if (!running.load()) break;
            std::this_thread::sleep_for(std::chrono::microseconds(100)); // Ждём сообщения
            continue;
        }

        auto start = std::chrono::high_resolution_clock::now();
        auto doc = parser.iterate(message);
        int64_t ts = doc["ts"].get_int64();
        auto data = doc["data"].get_array();

        std::unique_lock<std::mutex> lock(stateMutex);
        int current_state = state.load();
        Buffer* write_buffer = nullptr;

        switch (current_state) {
            case 0: write_buffer = &buffer1; break;
            case 1: write_buffer = &buffer2; break;
            case 2: write_buffer = &buffer3; break;
        }

        write_buffer->ts = ts;
        write_buffer->size = 0;
        for (auto trade : data) {
            Trade& t = write_buffer->trades[write_buffer->size];
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

            write_buffer->size++;
        }
        write_buffer->ready = true;

        std::cout << "📝 Wrote " << write_buffer->size << " trades to buffer " << current_state + 1 << " (ts: " << ts << ")\n" << std::flush;

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        std::cout << "⏱ Parsing time for this message: " << duration << " microseconds\n" << std::flush;

        // Переключение состояния только после парсинга и уведомление manage_buffers
        state.store((current_state + 1) % 3);
        lock.unlock();
        cv.notify_one(); // Уведомляем manage_buffers о завершении парсинга
    }
    std::cout << "🏁 Parser thread exiting\n" << std::flush;
}

void manage_buffers() {
    std::cout << "🚀 Buffer management thread started\n" << std::flush;
    while (running.load()) {
        std::unique_lock<std::mutex> lock(stateMutex);

        // Ждём, пока парсер завершит работу и уведомит нас
        cv.wait(lock, [] { return buffer1.ready || buffer2.ready || buffer3.ready || !running.load(); });

        if (!running.load()) break;

        int current_state = state.load();
        Buffer* clean_buffer = nullptr;
        Buffer* buffer_to_read = nullptr;

        switch (current_state) {
            case 0:
                clean_buffer = &buffer2;
                buffer_to_read = &buffer3;
                break;
            case 1:
                clean_buffer = &buffer3;
                buffer_to_read = &buffer1;
                break;
            case 2:
                clean_buffer = &buffer1;
                buffer_to_read = &buffer2;
                break;
        }

        read_buffer(*buffer_to_read); // Читаем буфер и сбрасываем ready
        clean_buffer->clean();        // Очищаем следующий буфер

        lock.unlock();
    }

    std::cout << "Buffer 1 (size: " << buffer1.size << "):\n";
    if (buffer1.size > 0) {
        std::cout << "  ts: " << buffer1.ts << "\n";
        std::cout << "  s: " << buffer1.trades[0].s << "\n";
        std::cout << "  S: " << buffer1.trades[0].S << "\n";
        std::cout << "  v: " << buffer1.trades[0].v << "\n";
        std::cout << "  p: " << buffer1.trades[0].p << "\n";
        std::cout << "  BT: " << (buffer1.trades[0].BT ? "true" : "false") << "\n";
        std::cout << "  RPI: " << (buffer1.trades[0].RPI ? "true" : "false") << "\n";
    } else {
        std::cout << "  Empty\n";
    }

    std::cout << "Buffer 2 (size: " << buffer2.size << "):\n";
    if (buffer2.size > 0) {
        std::cout << "  ts: " << buffer2.ts << "\n";
        std::cout << "  s: " << buffer2.trades[0].s << "\n";
        std::cout << "  S: " << buffer2.trades[0].S << "\n";
        std::cout << "  v: " << buffer2.trades[0].v << "\n";
        std::cout << "  p: " << buffer2.trades[0].p << "\n";
        std::cout << "  BT: " << (buffer2.trades[0].BT ? "true" : "false") << "\n";
        std::cout << "  RPI: " << (buffer2.trades[0].RPI ? "true" : "false") << "\n";
    } else {
        std::cout << "  Empty\n";
    }

    std::cout << "Buffer 3 (size: " << buffer3.size << "):\n";
    if (buffer3.size > 0) {
        std::cout << "  ts: " << buffer3.ts << "\n";
        std::cout << "  s: " << buffer3.trades[0].s << "\n";
        std::cout << "  S: " << buffer3.trades[0].S << "\n";
        std::cout << "  v: " << buffer3.trades[0].v << "\n";
        std::cout << "  p: " << buffer3.trades[0].p << "\n";
        std::cout << "  BT: " << (buffer3.trades[0].BT ? "true" : "false") << "\n";
        std::cout << "  RPI: " << (buffer3.trades[0].RPI ? "true" : "false") << "\n";
    } else {
        std::cout << "  Empty\n";
    }

    std::cout << "🏁 Buffer management thread exiting\n" << std::flush;
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

    std::thread parseThread(parse_buffer);
    std::thread manageThread(manage_buffers);
    std::thread wsThread(websocket_thread);

    wsThread.join();
    parseThread.join();
    manageThread.join();

    std::cout << "✅ Program exited cleanly\n" << std::flush;
    return 0;
}
