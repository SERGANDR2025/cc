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
#include <optional> // Добавлено для std::optional

typedef websocketpp::client<websocketpp::config::asio_client> client;
client c;

// Структура для хранения данных трейда
struct Trade {
    int64_t ts;         // Временная метка
    std::string s;      // Символ
    std::string S;      // Сторона (Buy/Sell)
    double v;           // Объем
    double p;           // Цена
    std::string L;      // Направление изменения цены
    std::optional<bool> BT;  // Поле BT (boolean, может отсутствовать)
    std::optional<bool> RPI; // Поле RPI (boolean, может отсутствовать)
};

// Два буфера для данных
constexpr size_t BUFFER_SIZE = 5000; // Размер буфера с запасом
std::array<std::string, 2> buffers;
std::atomic<size_t> activeBufferIndex{ 0 }; // Индекс активного буфера
std::mutex bufferMutex;
std::condition_variable bufferCV;
std::atomic<bool> running{ true };

// Очередь для хранения распарсенных данных
tbb::concurrent_vector<Trade> parsedTrades;
std::mutex coutMutex;

// Обработчик сигналов для graceful shutdown
void signal_handler(int signal) {
    {
        std::lock_guard<std::mutex> lock(coutMutex);
        std::cout << "Received signal " << signal << ", stopping...\n";
    }
    running.store(false);
    c.stop();
}

// Функция для записи данных в буфер
void write_to_buffer(const std::string& data) {
    std::unique_lock<std::mutex> lock(bufferMutex);
    size_t index = activeBufferIndex.load();
    buffers[index] += data; // Добавляем данные в активный буфер
    if (buffers[index].size() >= BUFFER_SIZE) {
        // Переключаемся на другой буфер
        activeBufferIndex.store(1 - index);
        buffers[1 - index].clear(); // Очищаем второй буфер
        bufferCV.notify_one();     // Оповещаем поток парсера
    }
}

// Функция для парсинга данных из буфера
void parse_buffer() {
    simdjson::ondemand::parser parser;
    while (running.load()) {
        std::unique_lock<std::mutex> lock(bufferMutex);
        bufferCV.wait(lock, [&] {
            return !running.load() || buffers[1 - activeBufferIndex.load()].size() > 0;
            });

        if (!running.load()) break;

        // Получаем данные из неактивного буфера
        size_t index = 1 - activeBufferIndex.load();
        std::string& buffer = buffers[index];
        if (buffer.empty()) continue;

        try {
            auto doc = parser.iterate(buffer);
            int64_t ts = doc["ts"].get_int64(); // Временная метка

            // Обрабатываем массив data
            auto trades = doc["data"].get_array();
            for (auto trade : trades) {
                Trade t;
                t.ts = ts;

                // Извлекаем обязательные поля
                t.s = std::string(trade["s"].get_string().value());
                t.S = std::string(trade["S"].get_string().value());
                t.v = std::stod(std::string(trade["v"].get_string().value()));
                t.p = std::stod(std::string(trade["p"].get_string().value()));
                t.L = std::string(trade["L"].get_string().value());

                // Обрабатываем опциональные boolean поля
                t.BT = trade["BT"].type() == simdjson::ondemand::json_type::boolean
                    ? std::optional<bool>(trade["BT"].get_bool())
                    : std::nullopt;

                t.RPI = trade["RPI"].type() == simdjson::ondemand::json_type::boolean
                    ? std::optional<bool>(trade["RPI"].get_bool())
                    : std::nullopt;

                // Добавляем трейд в очередь
                parsedTrades.push_back(t);

                // Выводим распарсенные данные на консоль
                std::lock_guard<std::mutex> lock(coutMutex);
                std::cout << "Parsed Trade:\n"
                    << "  ts: " << t.ts << "\n"
                    << "  s: " << t.s << "\n"
                    << "  S: " << t.S << "\n"
                    << "  v: " << t.v << "\n"
                    << "  p: " << t.p << "\n"
                    << "  L: " << t.L << "\n"
                    << "  BT: " << (t.BT.has_value() ? (t.BT.value() ? "true" : "false") : "null") << "\n"
                    << "  RPI: " << (t.RPI.has_value() ? (t.RPI.value() ? "true" : "false") : "null") << "\n"
                    << "-------------------------\n";
            }
        }
        catch (const std::exception& e) {
            std::cerr << "Exception during parsing: " << e.what() << "\n";
        }

        buffer.clear(); // Очищаем буфер после обработки
    }
}

// Обработчик сообщений WebSocket
void on_message(client*, websocketpp::connection_hdl, client::message_ptr msg) {
    std::string payload = msg->get_payload();
    write_to_buffer(payload); // Записываем данные в буфер
}

void on_open(client*, websocketpp::connection_hdl) {
    std::cout << "Connected\n";
}

void on_close(client*, websocketpp::connection_hdl) {
    std::cout << "Disconnected\n";
}

int main() {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    c.init_asio();
    c.set_open_handler(std::bind(&on_open, &c, std::placeholders::_1));
    c.set_close_handler(std::bind(&on_close, &c, std::placeholders::_1));
    c.set_message_handler(std::bind(&on_message, &c, std::placeholders::_1, std::placeholders::_2));

    websocketpp::lib::error_code ec;
    auto con = c.get_connection("ws://localhost:8765", ec);
    if (ec) {
        std::cout << "Connection error: " << ec.message() << "\n";
        return 1;
    }
    c.connect(con);

    // Запускаем поток для парсинга данных
    std::thread parserThread(parse_buffer);

    // Запускаем WebSocket
    std::thread wsThread([&]() {
        c.run();
        std::cout << "WebSocket thread finished\n";
        });

    wsThread.join();
    parserThread.join();
    std::cout << "Program exited cleanly\n";
    return 0;
}