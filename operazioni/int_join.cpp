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
//  i = Interval(-1 sec, 1 sec) 
//  q = tab1.join(tab2, "sensor_id", i).select("sensor_id", "temperature", "temp")

//schema del file
struct SensorInput{
    std::string sensor_id;
    double temperature;
};

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

struct JoinOutput{
    std::string sensor_id;
    double temperature;
    double temp;
};

//funtore di join
class EquiJoin_Functor {
public:
    //richiesti a compilazione
    using tuple_t = SensorInput;    
    using result_t = JoinOutput;   

    std::optional<JoinOutput> operator()(const SensorInput &left, const SensorInput &right) {
        if (left.sensor_id == right.sensor_id) {
            return JoinOutput({
                left.sensor_id,
                left.temperature,
                right.temperature       //l'operatore richiede lo stesso tipo da dx e sx quindi
                                        //  c'è stata una mappatura.
            });
        }
        return std::nullopt; //nessun match
    }
};

// --- SINK ---
class Sink_Functor {
public:
    void operator()(std::optional<JoinOutput> &input) {
        if (!input) {
            std::cout << "[SINK] EOS Ricevuto. Self-Join Completata." << std::endl;
            return;
        }
        std::cout << "[JOIN MATCH] Sensore: " << input->sensor_id 
                  << " | T1_Temperature: " << input->temperature
                  << " | T2_Temp: " << input->temp 
                  << std::endl;
    }
};

int main() {
    Source_Functor src1_func("sensor_stream_input.csv");
    Source_Functor src2_func("sensor_stream_input.csv");
    
    EquiJoin_Functor join_func;
    Sink_Functor snk_func;

    auto source1 = wf::Source_Builder(src1_func).withName("Table1_Src").build();
    auto source2 = wf::Source_Builder(src2_func).withName("Table2_Src").build();

    auto interval_join = wf::Interval_Join_Builder(join_func)
        .withName("IntervalJoin_Sensors")
        .withParallelism(3) 
        .withKeyBy([](const SensorInput &in) -> std::string { return in.sensor_id; })
        .withKPMode()
        .withBoundaries(std::chrono::microseconds(-1000000), std::chrono::microseconds(1000000))
        .build();

    auto sink = wf::Sink_Builder(snk_func).withName("Sink").build();

    wf::PipeGraph topology("TableAPI_IntervalJoinQuery", 
                           wf::Execution_Mode_t::DEFAULT, 
                           wf::Time_Policy_t::EVENT_TIME); 

    // Estraiamo i riferimenti alle MultiPipe creati dalle sorgenti
    auto& pipe1 = topology.add_source(source1);
    auto& pipe2 = topology.add_source(source2);
    
    std::vector<wf::MultiPipe*> branches = {&pipe1, &pipe2};

    auto* merged_pipe = wf::merge_multipipes_func(&topology, branches);
    merged_pipe->add(interval_join).add_sink(sink);

    topology.run(); 

    return 0;
}