
#include <websocketpp/config/asio_no_tls_client.hpp>
#include <websocketpp/client.hpp>
#include <simdjson.h>
#include <iostream>
#include <functional>
#include <chrono>
#include <thread>
#include <string>
#include <vector>
#include <tbb/concurrent_vector.h>
#include <mutex>
#include <csignal>
#include <atomic>
#include <array>
#include <optional>
#include <future>

typedef websocketpp::client<websocketpp::config::asio_client> client;
client c;

// Структура для хранения данных трейда
struct Trade {
    int64_t ts;
    std::string s;
    std::string S;
    double v;
    double p;
    std::string L;
    std::optional<bool> BT;
    std::optional<bool> RPI;
};

// Флаги работы потоков
std::atomic<bool> running{true};
std::atomic<bool> wsRunning{true};
std::promise<void> exitSignal;
std::future<void> futureExit = exitSignal.get_future();

std::thread parserThread, wsThread;
std::mutex coutMutex;
std::mutex bufferMutex;
std::condition_variable bufferCV;

// Очередь для хранения распарсенных данных
tbb::concurrent_vector<Trade> parsedTrades;

// Буферы для WebSocket-сообщений
constexpr size_t BUFFER_SIZE = 5000;
std::array<std::string, 2> buffers;
std::atomic<size_t> activeBufferIndex{0};

// 🔹 Обработчик `Ctrl+C`
void signal_handler(int signal) {
    {
        std::lock_guard<std::mutex> lock(coutMutex);
        std::cout << "\nReceived signal " << signal << ", stopping...\n" << std::flush;
    }

    running.store(false);
    wsRunning.store(false);
    exitSignal.set_value();  // Отправляем сигнал завершения

    c.stop();  // Останавливаем WebSocket

    if (wsThread.joinable()) wsThread.join();
    if (parserThread.joinable()) parserThread.join();

    std::cout << "Program exited cleanly.\n" << std::flush;
}

// 🔹 Запись данных в буфер
void write_to_buffer(const std::string& data) {
    std::unique_lock<std::mutex> lock(bufferMutex);
    size_t index = activeBufferIndex.load();
    buffers[index] += data;

    if (buffers[index].size() >= BUFFER_SIZE) {
        activeBufferIndex.store(1 - index);
        buffers[1 - index].clear();
        bufferCV.notify_one();
    }
}

// 🔹 Парсинг сообщений из буфера
void parse_buffer() {
    simdjson::ondemand::parser parser;
    while (running.load()) {
        std::unique_lock<std::mutex> lock(bufferMutex);
        bufferCV.wait(lock, [] { return !running.load() || !buffers[1 - activeBufferIndex.load()].empty(); });

        if (!running.load()) break;

        size_t index = 1 - activeBufferIndex.load();
        std::string& buffer = buffers[index];
        if (buffer.empty()) continue;

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

                std::lock_guard<std::mutex> lock(coutMutex);
                std::cout << "Parsed Trade: " << t.s << " Price: " << t.p << " Volume: " << t.v << "\n" << std::flush;
            }
        }
        catch (const std::exception& e) {
            std::cerr << "Exception during parsing: " << e.what() << "\n" << std::flush;
        }

        buffer.clear();
    }
}

// 🔹 Обработчик сообщений WebSocket
void on_message(websocketpp::connection_hdl, client::message_ptr msg) {
    write_to_buffer(msg->get_payload());
}

// 🔹 Обработчики событий WebSocket
void on_open(websocketpp::connection_hdl) {
    std::cout << "Connected\n" << std::flush;
}

void on_close(websocketpp::connection_hdl) {
    std::cout << "Disconnected\n" << std::flush;
}

// 🔹 Поток WebSocket
void websocket_thread() {
    while (wsRunning.load()) {
        try {
            c.run_one();  // Запускаем WebSocket-клиент в цикле, чтобы `Ctrl+C` мог его остановить
        } catch (const std::exception& e) {
            std::cerr << "WebSocket error: " << e.what() << "\n";
        }
    }
}

// 🔹 Основная функция
int main() {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
    std::signal(SIGPIPE, SIG_IGN);

    c.init_asio();
    c.set_open_handler(on_open);
    c.set_close_handler(on_close);
    c.set_message_handler(on_message);

    websocketpp::lib::error_code ec;
    auto con = c.get_connection("ws://localhost:8765", ec);
    if (ec) {
        std::cerr << "Connection error: " << ec.message() << "\n" << std::flush;
        return 1;
    }
    c.connect(con);

    parserThread = std::thread(parse_buffer);
    wsThread = std::thread(websocket_thread);

    // 🔹 Ждём сигнал завершения
    futureExit.wait();

    if (wsThread.joinable()) wsThread.join();
    if (parserThread.joinable()) parserThread.join();

    std::cout << "Program exited cleanly\n" << std::flush;
    return 0;
}
