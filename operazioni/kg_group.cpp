#include <iostream>
#include <string>
#include <vector>
#include <optional>
#include <fstream>
#include <sstream>
#include <windflow.hpp>

//Svolge un tab.group_by("sensor_id").select("sensor_id", avg("temperature"))

//schema del file, ignoriamo humidity e timestamp non richiesti
struct SensorInput {
    std::string sensor_id;
    double temperature;
};

//crea i sensor_input
class Source_Functor {
private:
    std::string file_path;
public:
    Source_Functor(const std::string& path) : file_path(path) {}
    void operator()(wf::Source_Shipper<SensorInput> &shipper) {
        std::ifstream file(file_path);
        if (!file.is_open()) return;
        std::string line;
        std::getline(file, line);
        while (std::getline(file, line)) {
            std::stringstream ss(line);
            std::string item;
            SensorInput record;
            std::getline(ss, item, ','); 
            std::getline(ss, record.sensor_id, ',');
            std::getline(ss, item, ',');
            record.temperature = std::stod(item);
            shipper.push(record); 
        }
        file.close();
    }
};

//struct di output, la media volendo si può anche calcolare dopo
struct SensorAvg {
    std::string sensor_id;
    int count = 0;
    double sum = 0.0;
    double avg_temperature = 0.0;
};

//funtore di Reduce, lo stato è anche l'output
class Reduce_Functor {
    public:
        void operator()(const SensorInput &in, SensorAvg &state){
            state.sensor_id = in.sensor_id;
            state.count ++;
            state.sum += in.temperature;
            state.avg_temperature = state.sum / state.count;
        };
};

//output a schermo
class Sink_Functor {
public:
    void operator()(std::optional<SensorAvg> &input) {
        if (input && input->count > 0) {
            
            std::cout << "[MEDIA GROUP-BY] Sensore: " << input->sensor_id 
                      << " | Media Temp: " << input->avg_temperature 
                      << " (basata su " << input->count << " letture)" << std::endl;
        }
    }
};

int main(int argc, char* argv[]) {
    std::string input_file = "sensor_stream_input.csv";

    //sorgente
    Source_Functor src_func(input_file);
    auto source_op = wf::Source_Builder(src_func).withParallelism(1).build();

    //reduce
    Reduce_Functor red_func;
    auto reduce_op = wf::Reduce_Builder(red_func)
        .withParallelism(3)
        .withKeyBy(
                [](const SensorInput &input) -> std::string { 
                    return input.sensor_id;
                }
            )
        .build();

    //sink
    Sink_Functor sink_func;
    auto sink_op = wf::Sink_Builder(sink_func).withParallelism(1).build();

    //topologia
    wf::PipeGraph topology("TableAPI_Query", 
                           wf::Execution_Mode_t::DEFAULT);

    topology.add_source(source_op).add(reduce_op).add_sink(sink_op);
    topology.run(); 

    return 0;
}