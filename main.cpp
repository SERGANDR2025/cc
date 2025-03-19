#include <websocketpp/config/asio_no_tls_client.hpp>
#include <websocketpp/client.hpp>
#include <simdjson.h>
#include <boost/circular_buffer.hpp>
#include <iostream>
#include <ostream>
#include <fstream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <csignal>
#include <atomic>
#include <optional>
#include <string>
#include <vector>
#include <tbb/concurrent_vector.h>

typedef websocketpp::client<websocketpp::config::asio_client> client;
client c;

// Структура Trade с нужными полями
struct Trade {
    int64_t ts;         // Timestamp сообщения (верхний уровень)
    std::string s;      // Символ
    std::string S;      // Сторона сделки (Buy/Sell)
    double v;           // Объем (преобразуем из строки)
    double p;           // Цена (преобразуем из строки)
    std::optional<bool> BT;  // Блочная сделка
    std::optional<bool> RPI; // RPI сделка
};

constexpr size_t BUFFER_SIZE = 10;
std::mutex bufferMutex;
boost::circular_buffer<simdjson::padded_string> ringBuffer(BUFFER_SIZE);
std::condition_variable bufferCV;
std::atomic<bool> running{true};
tbb::concurrent_vector<Trade> parsedTrades;

void print_simdjson_info() {
    std::cout << "✅ simdjson is using: " 
              << simdjson::get_active_implementation()->name() 
              << " (" << simdjson::get_active_implementation()->description() 
              << ")\n" << std::flush;
}

void save_trades_to_log() {
    std::ofstream logFile("trades.log", std::ios::app);
    if (!logFile.is_open()) {
        std::cerr << "❌ Error: Cannot open log file!\n";
        return;
    }
    for (const auto& trade : parsedTrades) {
        logFile << trade.ts << " " << trade.s << " " << trade.S << " " << trade.p << " " << trade.v 
                << " BT:" << (trade.BT.has_value() ? (trade.BT.value() ? "true" : "false") : "null")
                << " RPI:" << (trade.RPI.has_value() ? (trade.RPI.value() ? "true" : "false") : "null") << "\n";
    }
    logFile.close();
    std::cout << "📄 Trades saved to trades.log\n" << std::flush;
}

void signal_handler(int signal) {
    std::cout << "\n⚠ Received signal " << signal << ", stopping...\n" << std::flush;
    running = false;
    bufferCV.notify_all();
    c.stop();
}

void write_to_buffer(const std::string& data) {
    std::unique_lock<std::mutex> lock(bufferMutex);
    if (ringBuffer.full()) {
        std::cerr << "⚠ Warning: Ring buffer is full, dropping data!\n" << std::flush;
        return;
    }
    ringBuffer.push_back(simdjson::padded_string(data));
    lock.unlock();
    std::cout << "📥 Data added to buffer\n" << std::flush;
    bufferCV.notify_one();
}

void parse_buffer() {
    std::cout << "🚀 Parser thread started\n" << std::flush;
    simdjson::ondemand::parser parser;
    while (running.load()) {
        std::unique_lock<std::mutex> lock(bufferMutex);
        bufferCV.wait(lock, [] { return !ringBuffer.empty() || !running.load(); });

        if (!running.load() && ringBuffer.empty()) {
            std::cout << "🏁 Parser thread exiting\n" << std::flush;
            break;
        }

        if (!ringBuffer.empty()) {
            simdjson::padded_string buffer = std::move(ringBuffer.front());
            ringBuffer.pop_front();
            lock.unlock();

            try {
                auto doc = parser.iterate(buffer);
                int64_t ts = doc["ts"].get_int64(); // Timestamp верхнего уровня
                auto data = doc["data"].get_array();

                for (auto trade : data) {
                    Trade t;

                    // Парсим только нужные поля
                    t.ts = ts;
                    t.s = std::string(trade["s"].get_string().value());
                    t.S = std::string(trade["S"].get_string().value());
                    t.v = std::stod(std::string(trade["v"].get_string().value()));
                    t.p = std::stod(std::string(trade["p"].get_string().value()));
                    t.BT = trade["BT"].type() == simdjson::ondemand::json_type::boolean
                        ? std::optional<bool>(trade["BT"].get_bool())
                        : std::nullopt;
                    t.RPI = trade["RPI"].type() == simdjson::ondemand::json_type::boolean
                        ? std::optional<bool>(trade["RPI"].get_bool())
                        : std::nullopt;

                    parsedTrades.push_back(t);
                    std::cout << "📊 Parsed Trade: " << t.s << " Side: " << t.S 
                              << " Price: " << t.p << " Volume: " << t.v 
                              << " BT: " << (t.BT.has_value() ? (t.BT.value() ? "true" : "false") : "null")
                              << " RPI: " << (t.RPI.has_value() ? (t.RPI.value() ? "true" : "false") : "null")
                              << " Time: " << t.ts << "\n" << std::flush;
                }
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

    print_simdjson_info();

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

    save_trades_to_log();

    std::cout << "✅ Program exited cleanly\n" << std::flush;
    return 0;
}