#include <iostream>
#include <string>
#include <vector>
#include <optional>
#include <fstream>
#include <sstream>
#include <map>
#include <unordered_set>
#include <windflow.hpp>


struct SensorInput {
    uint64_t timestamp; 
    std::string sensor_id;
    double temperature;

    bool operator==(const SensorInput& other) const {
        return timestamp == other.timestamp &&
               sensor_id == other.sensor_id &&
               temperature == other.temperature;
    }
};

namespace std {
    template <>
    struct hash<SensorInput> {
        std::size_t operator()(const SensorInput& s) const {
            std::size_t h1 = std::hash<uint64_t>{}(s.timestamp);
            std::size_t h2 = std::hash<std::string>{}(s.sensor_id);
            std::size_t h3 = std::hash<double>{}(s.temperature);
            return h1 ^ (h2 << 1) ^ (h3 << 2);
        }
    };
}

class Source_Functor {
private:
    std::string file_path;
public:
    Source_Functor(const std::string& path) : file_path(path) {}

    void operator()(wf::Source_Shipper<SensorInput> &shipper) {
        std::ifstream file(file_path);
        if (!file.is_open()) return;

        std::string line;
        std::getline(file, line); // Salta header

        while (std::getline(file, line)) {
            if (line.empty() || line.front() == '\r') continue;

            // Parsing timestamp (min, sec, ms)
            std::string min_str = line.substr(14, 2); 
            std::string sec_str = line.substr(17, 2); 
            std::string ms_str  = line.substr(20, 3); 

            uint64_t min = std::stoull(min_str);
            uint64_t sec = std::stoull(sec_str);
            uint64_t ms  = std::stoull(ms_str);

            uint64_t ts_us = ((min * 60 + sec) * 1000 + ms) * 1000;

            std::stringstream ss(line);
            std::string item;
            SensorInput record;
            record.timestamp = ts_us;

            std::getline(ss, item, ','); 
            std::getline(ss, record.sensor_id, ',');
            std::getline(ss, item, ',');
            record.temperature = std::stod(item);

            // Invio tupla con timestamp e aggiornamento watermark
            shipper.pushWithTimestamp(record, ts_us);
            shipper.setNextWatermark(ts_us);
        }
        file.close();
    }
};

class Distinct_Functor {
private:
    std::unordered_set<SensorInput> seen;

public:
    bool operator()(SensorInput &input) {
        auto result = seen.insert(input);
        return result.second; 
    }
};

class Order_Functor {
public:
    void operator()(const SensorInput& in, wf::Shipper<SensorInput> &shipper) {
        shipper.push(in); // Inoltro immediato
    }
};

class Sink_Functor {
public:
    void operator()(std::optional<SensorInput> &input) {
        if (!input) {
            std::cout << "[SINK] EOS Ricevuto. Ordinamento Completato." << std::endl;
            return;
        }
        std::cout << "[OrderBy] TS: " << input->timestamp / 1000 << "ms"
                  << " | Sensore: " << input->sensor_id 
                  << " | Temp: " << input->temperature << "°C"
                  << std::endl;
    }
};

int main(int argc, char* argv[]) {
    Source_Functor src_func("sensor_stream_input.csv");
    Distinct_Functor distinct_func;
    Order_Functor order_func;
    Sink_Functor snk_func;

    wf::PipeGraph topology("TableAPI_OrderBy", 
                           wf::Execution_Mode_t::DEFAULT, 
                           wf::Time_Policy_t::EVENT_TIME); 

    auto source_op = wf::Source_Builder(src_func).withName("Table_Src").build();

    auto distinct_op = wf::Filter_Builder(distinct_func)
        .withName("Distinct_Parallel")
        .withParallelism(3)
        .withKeyBy([](const SensorInput &in) -> SensorInput { return in; })
        .build();

    // OrderBy ad un solo thread per garantire l'ordinamento globale
    auto order_op = wf::FlatMap_Builder(order_func)
        .withName("OrderBy_Op")
        .withParallelism(1)
        .withOrderingMode(wf::ordering_mode_t::ORDERED)
        .build();

    auto sink_op = wf::Sink_Builder(snk_func)
        .withName("Sink")
        .withParallelism(1)
        .build();

    topology.add_source(source_op)
            .add(distinct_op)
            .add(order_op)
            .add_sink(sink_op);

    topology.run(); 

    return 0;
}