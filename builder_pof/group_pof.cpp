#include <iostream>
#include <string>
#include <vector>
#include <optional>
#include <fstream>
#include <sstream>
#include <map>
#include <unordered_set>
#include <windflow.hpp>
#include <select_builder.hpp>
#include <where_builder.hpp>
#include <global_group_builder.hpp>
#include <windowed_group_builder.hpp>

//struct dato dalla sorgente
struct SensorInput {
    std::string sensor_id;
    double temperature;
    double humidity;
};

//per l'estrazione della chiave
struct KeyRecord{
    std::string sensor_id;

    bool operator==(const KeyRecord& other) const {
        return sensor_id == other.sensor_id;
    }
};

//aggiungo l'hash per KeyRecord in std
namespace std {
    template <>
    struct hash<KeyRecord> {
        std::size_t operator()(const KeyRecord& k) const {
            return std::hash<std::string>{}(k.sensor_id);
        }
    };
}

//output delle aggregazioni
struct GroupOutput{
    std::string sensor_id;
    double temp_avg = 0.0;
    int count = 0;
    double sum = 0.0;

    uint64_t win_id = 0;

    GroupOutput() = default;

    //costruttore necessario per fare le finestre non keyed
    GroupOutput(uint64_t _id) 
        : win_id(_id), count(0), sum(0.0), temp_avg(0.0) {}

    //costruttore necessario per fare le finestre keyed    
    GroupOutput(KeyRecord _key, uint64_t _id) 
        : sensor_id(_key.sensor_id), win_id(_id), count(0), sum(0.0), temp_avg(0.0) {}    
};

//output a schermo
class Sink_Functor {
public:
    void operator()(std::optional<GroupOutput> &input) {
        if (input && input->count > 0) {
            std::cout << "[MEDIA GROUP-BY] Sensore: " << input->sensor_id 
                      //<< " | Finestra ID: " << input->win_id
                      << " | Media Temp: " << input->temp_avg
                      << " °C (su " << input->count << " letture)" 
                      << std::endl;
        }
    }
};

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

int main() {
    Source_Functor src_func("sensor_stream_input.csv");
    Sink_Functor sink_func;

    wf::PipeGraph topology("TableAPI_PoC", wf::Execution_Mode_t::DEFAULT, wf::Time_Policy_t::EVENT_TIME);

    //sorgente
    auto source_op = wf::Source_Builder(src_func)
        .withName("Source_CSV")
        .build();

    //logica di raggruppamento e aggregazione
    auto group_logic = [](const SensorInput& in, GroupOutput& out) -> void {
        //chiave
        out.sensor_id = in.sensor_id;

        //count e sum per calcolare la media
        out.count += 1;
        out.sum += in.temperature;
        out.temp_avg = out.sum / out.count; 
    };

    auto group_key = [](const SensorInput& in) -> KeyRecord {
        return KeyRecord({in.sensor_id});
    };

    // Global - Keyed
    auto group_op = Global_Group_Builder<SensorInput, GroupOutput, KeyRecord>(group_logic)
        .withName("group_by")
        .withParallelism(3)
        .withKeyBy(group_key)
        .build();

    /* Global - NotKeyed
    auto group_op = Global_Group_Builder<SensorInput, GroupOutput, KeyRecord>(group_logic)
        .withName("group_by")
        .withParallelism(3)
        .build();
    */

    /* Windowed - NotKeyed
    auto group_op = Windowed_Group_Builder<SensorInput, GroupOutput, KeyRecord>(group_logic)
        .withName("group_by")
        .withParallelism(3)
        .withCBWindow(6, 3) 
        .build();
    */

    /* Windowed - Keyed
    auto group_op = Windowed_Group_Builder<SensorInput, GroupOutput, KeyRecord>(group_logic)
        .withName("group_by")
        .withParallelism(3)
        .withTBWindow(5000000)  //5 secondi
        .withKeyBy(group_key)
        .build();
    */    
    
    //sink
    auto sink_op = wf::Sink_Builder(sink_func)
        .withName("Sink")
        .withParallelism(1)
        .build();

    //topologia
    topology.add_source(source_op)
            .add(group_op)
            .add_sink(sink_op);

    topology.run();
    return 0;
}
