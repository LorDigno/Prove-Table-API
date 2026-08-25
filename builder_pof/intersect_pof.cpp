#include <iostream>
#include <string>
#include <optional>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <algorithm>
#include <windflow.hpp>
#include <select_builder.hpp>
#include <where_builder.hpp>
#include <intersect_builder.hpp>
#include <intersect_all_builder.hpp>
#include <tagged_tuple.hpp>

//schema del file
struct SensorInput{
    std::string sensor_id;
    double temperature;
    double humidity;

    //necessario per l'unordered set
    bool operator==(const SensorInput& other) const {
        return sensor_id == other.sensor_id && temperature == other.temperature && humidity == other.humidity;
    }
};

//aggiungo l'hash per sensorInput in std
namespace std {
    template <>
    struct hash<SensorInput> {
        std::size_t operator()(const SensorInput& s) const {
            std::size_t h1 = std::hash<std::string>{}(s.sensor_id);
            std::size_t h2 = std::hash<double>{}(s.temperature);
            std::size_t h3 = std::hash<double>{}(s.humidity);
            return h1 ^ (h2 << 1) ^ (h3 << 2); // XOR + Shift per combinare gli hash
        }
    };
}

//funtore sorgente di SensorInput
class Source_Functor {
private:
    std::string file_path;

public:
    Source_Functor(const std::string& path) : file_path(path) {}

    void operator()(wf::Source_Shipper<SensorInput> &shipper) {
        std::ifstream file(file_path);
        if (!file.is_open()) {
            std::cerr << "[ERROR] Impossibile aprire il file: " << file_path << std::endl;
            return;
        }

        std::string line;
        // 1. Salta l'intestazione (header)
        std::getline(file, line); 

        // 2. Lettura riga per riga
        while (std::getline(file, line)) {
            if (line.empty() || line.front() == '\r') continue;

            // --- Parsing del Timestamp ISO 8601 (YYYY-MM-DDTHH:MM:SS.mmmZ) ---
            // Estrazione di minuti (pos 14), secondi (pos 17) e millisecondi (pos 20)
            std::string min_str = line.substr(14, 2); 
            std::string sec_str = line.substr(17, 2); 
            std::string ms_str  = line.substr(20, 3); 

            uint64_t min = std::stoull(min_str);
            uint64_t sec = std::stoull(sec_str);
            uint64_t ms  = std::stoull(ms_str);

            // Conversione del tempo in microsecondi
            uint64_t ts_us = ((min * 60 + sec) * 1000 + ms) * 1000;

            // --- Parsing dei campi separati da virgola ---
            std::stringstream ss(line);
            std::string item;
            SensorInput record;

            std::getline(ss, item, ',');               // Salta colonna ISO timestamp stringa
            std::getline(ss, record.sensor_id, ',');  // sensor_id
            std::getline(ss, item, ',');
            record.temperature = std::stod(item);      // temperature
            std::getline(ss, item, ',');
            record.humidity = std::stod(item);         // humidity

            // --- Inoltro tupla e sincronizzazione Watermark ---
            shipper.pushWithTimestamp(record, ts_us);
            shipper.setNextWatermark(ts_us);
        }

        file.close();
    }
};

class Sink_Functor {
public:
    void operator()(std::optional<SensorInput> &input) {
        if (!input) {
            std::cout << "[SINK] EOS Ricevuto. Intersezione Temporale Completata." << std::endl;
            return;
        }
        std::cout << "[INTERSECT]"
                  << " | Sensore: " << input->sensor_id
                  << " | T: " << input->temperature << "°C"
                  << " | H: " << input->humidity << "%"
                  << std::endl;
    }
};

int main() {
    Source_Functor src_func1("sensor_stream_input.csv");
    Source_Functor src_func2("sensor_stream_input2.csv");
    Sink_Functor sink_func;

    wf::PipeGraph topology("TableAPI_PoC", wf::Execution_Mode_t::DEFAULT, wf::Time_Policy_t::EVENT_TIME);

    //sorgente
    auto source_op1 = wf::Source_Builder(src_func1)
        .withName("Source_CSV")
        .build();

    //sorgente
    auto source_op2 = wf::Source_Builder(src_func2)
        .withName("Source_CSV")
        .build();    

    //logica di filtraggio    
    auto where1_logic = [](const SensorInput& in) -> bool {
        return in.humidity > 47;
    };

    //where
    auto where_op1 = Where_Builder<SensorInput>(where1_logic)
        .withName("Where_Sensor_Temperature")
        .withParallelism(2)
        .build();


    //logica di filtraggio    
    auto where2_logic = [](const SensorInput& in) -> bool {
        return in.temperature < 25;
    };

    //where
    auto where_op2 = Where_Builder<SensorInput>(where2_logic)
        .withName("Where_Sensor_Temperature")
        .withParallelism(2)
        .build();

    
    auto left_tagger = [](const SensorInput& in) -> Tagged_Tuple<SensorInput> {
        return Tagged_Tuple<SensorInput>(in, 0);
    };

    auto tagger_op1 = Select_Builder<SensorInput, Tagged_Tuple<SensorInput>>(left_tagger)
        .withName("tagger1")
        .build();
      
    auto right_tagger = [](const SensorInput& in) -> Tagged_Tuple<SensorInput> {
        return Tagged_Tuple<SensorInput>(in, 1);
    };

    auto tagger_op2 = Select_Builder<SensorInput, Tagged_Tuple<SensorInput>>(right_tagger)
        .withName("tagger1")
        .build();

       
    auto intersect_op = Intersect_Builder<SensorInput>()
        .withName("intersect_op")
        .withParallelism(3)
        .build_keyed();

    /*
    auto intersect_all_op = Intersect_All_Builder<SensorInput>()
        .withName("intersect_op")
        .withParallelism(3)
        .build_keyed();
    */

    //sink
    auto sink_op = wf::Sink_Builder(sink_func)
        .withName("Sink")
        .withParallelism(1)
        .build();

    //topologia
    auto& pipe1 = topology.add_source(source_op1).add(where_op1).add(tagger_op1);
    auto& pipe2 = topology.add_source(source_op2).add(where_op2).add(tagger_op2);

    std::vector<wf::MultiPipe*> branches = {&pipe1, &pipe2};
    auto* merged_pipe = wf::merge_multipipes_func(&topology, branches);

    merged_pipe->add(intersect_op).add_sink(sink_op);
    //merged_pipe->add(intersect_all_op).add_sink(sink_op);

    topology.run();
    return 0;
}