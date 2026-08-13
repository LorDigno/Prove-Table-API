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
    
//Svolge:
//  q = tab1.intersect(tab2).select("sensor_id", "temperature")

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

//sensorInput con provenienza di flusso
struct TabRecord {
    std::string sensor_id;
    double temperature;
    int tab;
};

//funtore da dare ad un operatore di Map
class TabTagger {
    int id;

public:
    TabTagger(int num){
        id = num;
    }

    void operator()(const SensorInput& in, wf::Shipper<TabRecord> &shipper){
        shipper.push(TabRecord{in.sensor_id, in.temperature, id});
    }
}; 

//stato per ogni tupla
struct TupleState {
    bool seen1 = false;
    bool seen2 = false;
    bool emitted = false;
};

class Intersect_FlatMap {
    std::unordered_map<SensorInput, TupleState> tuple_state;

    
public:
    void operator()(const TabRecord& in, wf::Shipper<SensorInput> &shipper){
        SensorInput sens = {in.sensor_id, in.temperature};
        auto& state = tuple_state[sens];

        //return anticipato se l'ho già inviata
        if (state.emitted) {
            return;
        }

        if(in.tab == 1) {
            state.seen1 = true;
        }else if (in.tab == 2) {
            state.seen2 = true;
        }

        //c'è in entrambe ed invio
        if(state.seen1 && state.seen2) {
            shipper.push(sens);
            state.emitted = true;
        }
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
                  << std::endl;
    }
};

int main() {
    Source_Functor src_func1("sensor_stream_input.csv");
    Source_Functor src_func2("sensor_stream_input2.csv");
    TabTagger tab1_logic(1), tab2_logic(2);
    Intersect_FlatMap intersect_logic;
    Sink_Functor sink_logic;

    wf::PipeGraph topology("TableAPI_Intersect", wf::Execution_Mode_t::DEFAULT, wf::Time_Policy_t::EVENT_TIME);

    auto src_q1 = wf::Source_Builder(src_func1).withName("Src_Q1").build();
    auto src_q2 = wf::Source_Builder(src_func2).withName("Src_Q2").build();

    auto tab1_op = wf::FlatMap_Builder(tab1_logic).withName("T1").withParallelism(1).build();
    auto tab2_op = wf::FlatMap_Builder(tab2_logic).withName("T2").withParallelism(1).build();

    auto intersect_op = wf::FlatMap_Builder(intersect_logic)
        .withName("Intersect_Time")
        .withParallelism(2)
        .withKeyBy([](const TabRecord &in) -> SensorInput {
            return SensorInput({in.sensor_id, in.temperature}); 
        })
        .build();
    
    auto sink_op = wf::Sink_Builder(sink_logic).withName("Sink").withParallelism(1).build();    

    //aggiungo i tab per tracciare i flussi
    auto& pipe_q1 = topology.add_source(src_q1).add(tab1_op);
    auto& pipe_q2 = topology.add_source(src_q2).add(tab2_op);

    //unisco e metto la intersect
    auto* merged_u = wf::merge_multipipes_func(&topology, {&pipe_q1, &pipe_q2});
    merged_u->add(intersect_op).add_sink(sink_op);

    topology.run();
    return 0;
}