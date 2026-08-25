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

//Svolge:
//    q = tab.where("temperature" < 30).select("sensor_id", "humidity")

//I funtori di source e sink li ho fatti a mano perché ancora non c'è il builder. 
//Questo file dovrebbe essere quello generato dal generatore di codice a seguito del parsing.

//I builder usano template per poter operare su ogni struct generato e richiedono una lambda con
//  una certa firma (in base all'operazione).
//La parte più complessa sarà probabilmente creare le lambda che danno la logica ai funtori
//  nei builder. 

//Quando ci saranno da fare gli operatori con TTL per evitare la crescità infinita dello stato
//  sarà il builder a gestire la cosa mentre il generatore dovrà solo fare un file come questo.

//struct dato dalla sorgente
struct SensorInput {
    std::string sensor_id;
    double temperature;
    double humidity;
};

//struct di output della select
struct SelectedSensor {
    std::string sensor_id;
    double humidity;
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

//stampa a schermo dei SelectedSensor
class Sink_Functor {
public:
    void operator()(std::optional<SelectedSensor> &input) {
        if (!input) {
            std::cout << "[SINK] EOS Ricevuto." << std::endl;
            return;
        }
        std::cout << "[SINK] Sensore: " << input->sensor_id 
                  << " | Humidity: " << input->humidity
                  //<< " | Temperature: " << input->temperature
                  << std::endl;
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

    //logica di filtraggio    
    auto where_logic = [](const SensorInput& in) -> bool {
        return in.temperature < 30 && in.humidity < 50;
    };

    //where
    auto where_op = Where_Builder<SensorInput>(where_logic)
        .withName("Where_Sensor")
        .withParallelism(2)
        .build();

        
    //logica di selezione
    auto select_logic = [](const SensorInput& in) -> SelectedSensor {
        //questa va inferita in base agli schemi di input e output
        //la regola di base sarà di mappare attributo x in attributo x (con lo stesso nome)
        //  che è quella applicata qua.
    
        SelectedSensor out;
		out.sensor_id = in.sensor_id;
		out.humidity = in.humidity;
		return out;
    };

    //select
    auto select_op = Select_Builder<SensorInput, SelectedSensor>(select_logic)
        .withName("Select_Sensor_Humidity")
        .withParallelism(2)
        .build();

    //sink
    auto sink_op = wf::Sink_Builder(sink_func)
        .withName("Sink")
        .withParallelism(1)
        .build();

    //topologia
    topology.add_source(source_op)
            .add(where_op)
            .add(select_op)
            .add_sink(sink_op);

    topology.run();
    return 0;
}