#include <iostream>
#include <string>
#include <vector>
#include <optional>
#include <fstream>
#include <sstream>
#include <chrono>
#include <windflow.hpp>

//Svolge:
//  w = Windows.createTBWindow(10 minuti, 10 minuti)
//  tab.group_by("sensor_id", ).select("sensor_id", avg("temperature"))

//schema del file, ignoriamo humidity e timestamp non richiesti
struct SensorInput {
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

//struct di output, la media volendo si può anche calcolare dopo
struct SensorAvg {
    std::string sensor_id;
    uint64_t win_id = 0;
    int count = 0;
    double sum = 0.0;
    double avg_temperature = 0.0;


    SensorAvg() = default;

    //costruttore necessario per fare le finestre non keyed
    SensorAvg(uint64_t _id) 
        : win_id(_id), count(0), sum(0.0), avg_temperature(0.0) {}

    //costruttore necessario per fare le finestre keyed    
    SensorAvg(std::string _key, uint64_t _id) 
        : sensor_id(_key), win_id(_id), count(0), sum(0.0), avg_temperature(0.0) {}
};

//funtore del groupBy via KeyedWindows
struct Window_Avg_Functor {
public: 
    void operator()(const wf::Iterable<SensorInput> &win, SensorAvg &out) const {
        double sum = 0.0;
        int count = 0;

        for (const auto &record : win) {
            sum += record.temperature;
            count++;
        }

        if (count > 0) {
            out.count = count;          
            out.sum = sum; 
            out.avg_temperature = sum / count;
        }
    }
};

//output a schermo
class Sink_Functor {
public:
    void operator()(std::optional<SensorAvg> &input) {
        if (input && input->count > 0) {
            std::cout << "[MEDIA GROUP-BY WIN] Sensore: " << input->sensor_id 
                      << " | Finestra ID: " << input->win_id
                      << " | Media Temp: " << input->avg_temperature 
                      << " °C (su " << input->count << " letture)" << std::endl;
        }
    }
};

int main(int argc, char* argv[]) {
    std::string input_file = "sensor_stream_input.csv";

    //sorgente
    Source_Functor src_func(input_file);
    auto source_op = wf::Source_Builder(src_func).withParallelism(1).build();

    Window_Avg_Functor group_func;
    auto group_op = wf::Keyed_Windows_Builder(group_func)
        .withParallelism(3)
        .withKeyBy(
            [](const SensorInput &input) -> std::string { 
                return input.sensor_id;
            }
        )
        .withTBWindows(std::chrono::microseconds(1000000), std::chrono::microseconds(1000000))
        .build();

    //sink
    Sink_Functor sink_func;
    auto sink_op = wf::Sink_Builder(sink_func).withParallelism(1).build();

    //topologia
    wf::PipeGraph topology("TableAPI_Query", wf::Execution_Mode_t::DEFAULT, wf::Time_Policy_t::EVENT_TIME);

    topology.add_source(source_op).add(group_op).add_sink(sink_op);
    topology.run(); 

    return 0;
}