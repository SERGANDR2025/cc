#include <websocketpp/config/asio_no_tls_client.hpp>
#include <websocketpp/client.hpp>
#include <simdjson.h>
#include <boost/circular_buffer.hpp>
#include <iostream>
#include <fstream>
#include <functional>
#include <chrono>
#include <thread>
#include <string>
#include <vector>
#include <tbb/concurrent_vector.h>
#include <mutex>
#include <csignal>
#include <atomic>
#include <optional>
#include <future>

typedef websocketpp::client<websocketpp::config::asio_client> client;
client c;

// 🔹 Структура трейда
struct Trade {
    int64_t ts;
    std::string s, S, L;
    double v, p;
    std::optional<bool> BT, RPI;
};

// 🔹 Константы буфера
constexpr size_t BUFFER_SIZE = 10;  // Кольцевой буфер на 10 пакетов

// 🔹 Потокобезопасный кольцевой буфер
std::mutex bufferMutex;
boost::circular_buffer<simdjson::padded_string> ringBuffer(BUFFER_SIZE);
std::condition_variable bufferCV;
std::atomic<bool> running{true};

// 🔹 Очередь для хранения трейдов
tbb::concurrent_vector<Trade> parsedTrades;

// 🔹 Вывод информации о SIMDJSON
void print_simdjson_info() {
    std::cout << "✅ simdjson is using: " 
              << simdjson::get_active_implementation()->name() 
              << " (" << simdjson::get_active_implementation()->description() 
              << ")\n" << std::flush;
}

// 🔹 Запись трейдов в лог
void save_trades_to_log() {
    std::ofstream logFile("trades.log", std::ios::app);
    if (!logFile.is_open()) {
        std::cerr << "❌ Error: Cannot open log file!\n";
        return;
    }

    for (const auto& trade : parsedTrades) {
        logFile << trade.ts << " " << trade.s << " " << trade.p << " " << trade.v << "\n";
    }

    logFile.close();
    std::cout << "📄 Trades saved to trades.log\n" << std::flush;
}

// 🔹 Обработчик `Ctrl+C`
void signal_handler(int signal) {
    std::cout << "\n⚠ Received signal " << signal << ", stopping...\n" << std::flush;
    running.store(false);
    bufferCV.notify_one();
    c.stop();
}

// 🔹 Запись данных в буфер
void write_to_buffer(const std::string& data) {
    std::unique_lock<std::mutex> lock(bufferMutex);

    if (ringBuffer.full()) {
        std::cerr << "⚠ Warning: Ring buffer is full, dropping data!\n" << std::flush;
        return;
    }

    // ✅ Используем padded_string для гарантированного `SIMDJSON_PADDING`
    ringBuffer.push_back(simdjson::padded_string::copy(data));
    bufferCV.notify_one();
}

// 🔹 Парсинг сообщений из буфера
void parse_buffer() {
    simdjson::ondemand::parser parser;
    while (running.load()) {
        std::unique_lock<std::mutex> lock(bufferMutex);
        bufferCV.wait(lock, [] { return !running.load() || !ringBuffer.empty(); });

        if (!running.load()) break;

        simdjson::padded_string buffer = std::move(ringBuffer.front());
        ringBuffer.pop_front();
        lock.unlock();

        try {
            auto doc = parser.iterate(buffer);
            int64_t ts = doc["ts"].get_int64();

            auto trades = doc["data"].get_array();
            for (auto trade : trades) {
                Trade t;
                t.ts = ts;
                t.s = std::string(trade["s"].get_string().value());
                t.S = std::string(trade["S"].get_string().value());
                t.v = std::stod(std::string(trade["v"].get_string().value()));
                t.p = std::stod(std::string(trade["p"].get_string().value()));
                t.L = std::string(trade["L"].get_string().value());

                t.BT = trade["BT"].type() == simdjson::ondemand::json_type::boolean
                    ? std::optional<bool>(trade["BT"].get_bool())
                    : std::nullopt;

                t.RPI = trade["RPI"].type() == simdjson::ondemand::json_type::boolean
                    ? std::optional<bool>(trade["RPI"].get_bool())
                    : std::nullopt;

                parsedTrades.push_back(t);

                std::cout << "📊 Parsed Trade: " << t.s << " Price: " << t.p << " Volume: " << t.v << "\n" << std::flush;
            }
        }
        catch (const std::exception& e) {
            std::cerr << "❌ Exception during parsing: " << e.what() << "\n" << std::flush;
        }
    }
}

// 🔹 Обработчик сообщений WebSocket
void on_message(websocketpp::connection_hdl, client::message_ptr msg) {
    write_to_buffer(msg->get_payload());
}

// 🔹 Поток WebSocket
void websocket_thread() {
    while (running.load()) {
        try {
            c.run_one();
        } catch (const std::exception& e) {
            std::cerr << "❌ WebSocket error: " << e.what() << "\n";
        }
    }
}

// 🔹 Основная функция
int main() {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    print_simdjson_info();  // ✅ Вывод информации о SIMDJSON

    c.init_asio();
    c.set_open_handler([](websocketpp::connection_hdl) { std::cout << "🔗 Connected\n" << std::flush; });
    c.set_close_handler([](websocketpp::connection_hdl) { std::cout << "❌ Disconnected\n" << std::flush; });
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
    save_trades_to_log();

    std::cout << "✅ Program exited cleanly\n" << std::flush;
    return 0;
}
