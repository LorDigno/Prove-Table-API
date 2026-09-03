#include <string>
#include <functional>
#include <windflow.hpp>
#include <chrono>
#include <thread>
#include <state_map.hpp>
#include <windflow.hpp>

//Prova della State_Map con un Distinct Modificato
template <typename InputT>
class Distinct_Clean_Functor{
    private:
        State_Map<InputT> state;

    public:
        Distinct_Clean_Functor(State_Map<InputT> map) 
            : state(map) {} 

        //metodo chiamato dalla filter   
        bool operator()(InputT& input){
            uint64_t time = now_micros();
            state.cleanup(time);

            return state.insert(input, time);
        }    
};

//la chiave di default è tutta la tupla
template <typename InputT, typename KeyT = InputT>
class Distinct_Clean_Builder {
private:
    std::string op_name = "Distinct_Operator";
    size_t parallelism = 1;
    std::function<KeyT(const InputT&)> key_func = nullptr;
    bool keyed = false;

    //parametri per il tll
    bool has_ttl = false;
    uint64_t ttl = 0;

public:
    Distinct_Clean_Builder() = default;

    Distinct_Clean_Builder& withName(const std::string& name) {
        this->op_name = name;
        return *this;
    }

    Distinct_Clean_Builder& withParallelism(size_t par) {
        this->parallelism = par;
        return *this;
    }

    Distinct_Clean_Builder& withKeyBy(std::function<KeyT(const InputT&)> k_func) {
        this->key_func = k_func;
        this->keyed = true;
        return *this;
    }

    //si richiede un ttl in microsecondi
    Distinct_Clean_Builder& withTTL(uint64_t ttl) {
        this->ttl = ttl;
        this->has_ttl = true;
        return *this;
    }

    auto build() {
        assert(has_ttl);

        State_Map<InputT> state = State_Map<InputT>(ttl);
        Distinct_Clean_Functor<InputT> functor(state);
        return wf::Filter_Builder(functor)
            .withName(op_name)
            .withParallelism(1)
            .build();
    }

    auto build_keyed() {
        assert(has_ttl);
        assert(keyed);

        State_Map<InputT> state = State_Map<InputT>(ttl);
        Distinct_Clean_Functor<InputT> functor(state);
        return wf::Filter_Builder(functor)
            .withName(op_name)
            .withParallelism(parallelism)
            .withKeyBy(key_func)
            .build();
    }
};

//struct dato dalla sorgente
struct SensorInput {
    std::string sensor_id;
    double temperature;
    double humidity;
    
    bool operator==(const SensorInput& other) const {
        return sensor_id == other.sensor_id && temperature == other.temperature && humidity == other.humidity;
    }
};

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

            //sleep di prova per il ttl
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }

        file.close();
    }
};

//stampa a schermo dei SelectedSensor
class Sink_Functor {
public:
    void operator()(std::optional<SensorInput> &input) {
        if (!input) {
            std::cout << "[SINK] EOS Ricevuto." << std::endl;
            return;
        }
        std::cout << "[SINK] Sensore: " << input->sensor_id 
                  << " | Humidity: " << input->humidity
                  << " | Temperature: " << input->temperature
                  << std::endl;
    }
};

//codice di prova
int main() {
    Source_Functor src_func("sensor_stream_input.csv");
    Sink_Functor sink_func;

    wf::PipeGraph topology("TableAPI_PoC", wf::Execution_Mode_t::DEFAULT, wf::Time_Policy_t::EVENT_TIME);

    //sorgente
    auto source_op = wf::Source_Builder(src_func)
        .withName("Source_CSV")
        .build();

    //distinct
    auto distinct_op = Distinct_Clean_Builder<SensorInput>()
        .withName("distinct")
        .withParallelism(1)
        //500ms di ttl quando c'è 50ms di sleep fra una tupla e l'altra
        //una tupla viene invalidata dopo che 10 altre tuple sono passate
        .withTTL(500000)      
        .build();

    //sink
    auto sink_op = wf::Sink_Builder(sink_func)
        .withName("Sink")
        .withParallelism(1)
        .build();

    //topologia
    topology.add_source(source_op)
            .add(distinct_op)
            .add_sink(sink_op);

    topology.run();
    return 0;
}
