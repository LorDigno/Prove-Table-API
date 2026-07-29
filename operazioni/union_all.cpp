#include <iostream>
#include <string>
#include <optional>
#include <fstream>
#include <sstream>
#include <vector>
#include <chrono>
#include <iomanip>
#include <windflow.hpp> 

//Svolge:
// q = tab1.unionAll(tab2).select("sensor_id", "temperature"))

//schema del file
struct SensorInput{
    std::string sensor_id;
    double temperature;

    //necessario per l'unordered set
    bool operator==(const SensorInput& other) const {
        return sensor_id == other.sensor_id && temperature == other.temperature;
    }
};

//aggiungo l'hash per sensorInput in std
namespace std {
    template <>
    struct hash<SensorInput> {
        std::size_t operator()(const SensorInput& s) const {
            std::size_t h1 = std::hash<std::string>{}(s.sensor_id);
            std::size_t h2 = std::hash<double>{}(s.temperature);
            return h1 ^ (h2 << 1); // XOR + Shift per combinare gli hash
        }
    };
}

//crea i sensor_input
class Source_Functor {
    std::string file_path;
public:
    Source_Functor(const std::string& path) : file_path(path) {}
    
    void operator()(wf::Source_Shipper<SensorInput> &shipper) {
        std::ifstream file(file_path);
        if (!file.is_open()) return;

        std::string line;
        std::getline(file, line); 

        //parsing del file e push della tupla
        while (std::getline(file, line)) {
            if (line.empty() || line.front() == '\r') continue;
            
            //minuti, secondi e millisecondi
            std::string min_str = line.substr(14, 2); 
            std::string sec_str = line.substr(17, 2); 
            std::string ms_str  = line.substr(20, 3); 

            uint64_t min = std::stoull(min_str);
            uint64_t sec = std::stoull(sec_str);
            uint64_t ms  = std::stoull(ms_str);

            //tempo in milli e micro secondi
            uint64_t ts_ms = (min * 60 + sec) * 1000 + ms;
            uint64_t ts_us = ts_ms * 1000;
            
            std::stringstream ss(line);
            std::string item;
            SensorInput record;

            std::getline(ss, item, ','); 
            std::getline(ss, record.sensor_id, ',');
            std::getline(ss, item, ',');
            record.temperature = std::stod(item);

            shipper.pushWithTimestamp(record, ts_us);
            shipper.setNextWatermark(ts_us);
        }
        file.close();
    }
};

//stampa a schermo
class Sink_Functor {
public:
    void operator()(std::optional<SensorInput> &input) {
        if (!input) {
            std::cout << "[SINK] EOS Ricevuto." << std::endl;
            return;
        }
        std::cout << "[UnionAll] Sensore: " << input->sensor_id 
                  << " | Temperature: " << input->temperature
                  << std::endl;
    }
};

int main() {
    Source_Functor src_func("sensor_stream_input.csv");
    Source_Functor src_func2("sensor_stream_input2.csv");
    Sink_Functor snk_func;

    auto source_op = wf::Source_Builder(src_func).withName("Table_Src").build();
    auto source_op2 = wf::Source_Builder(src_func2).withName("Table2_Src").build();

    auto sink_op = wf::Sink_Builder(snk_func).withParallelism(1).withName("Sink").build();

    wf::PipeGraph topology("TableAPI_UnionAllQuery", 
                           wf::Execution_Mode_t::DEFAULT, 
                           wf::Time_Policy_t::EVENT_TIME); 

    
    //singole pipe create dalle sorgenti
    auto& pipe1 = topology.add_source(source_op);
    auto& pipe2 = topology.add_source(source_op2);
    std::vector<wf::MultiPipe*> branches = {&pipe1, &pipe2};

    //merge del grafo
    auto* merged_pipe = wf::merge_multipipes_func(&topology, branches);
    merged_pipe->add_sink(sink_op);

    topology.run(); 

    return 0;
}