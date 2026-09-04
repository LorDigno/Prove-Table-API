#include <iostream>
#include <string>
#include <vector>
#include <optional>
#include <fstream>
#include <sstream>
#include <map>
#include <unordered_set>
#include <windflow.hpp>
#include <table_source_builder.hpp>
#include <table_sink_builder.hpp>
#include <windowed_group_builder.hpp>
#include <state_map.hpp>

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

int main() {
    //disordinato
    //std::string filepath = "sensor_real_stream.csv";
    //ordinato
    std::string filepath = "sensor_stream_input.csv";

    wf::PipeGraph topology(
        "TableAPI_PoC", 
        wf::Execution_Mode_t::DEFAULT
        ,wf::Time_Policy_t::EVENT_TIME
        //,wf::Time_Policy_t::INGRESS_TIME
    );

    auto source_logic = [](const std::string& line, SensorInput& record, uint64_t& timestamp){
        std::stringstream ss(line);
        std::string token;

        // Colonna 0: Timestamp ISO 8601
        std::getline(ss, token, ',');
        timestamp = parse_TIMESTAMP_ISO8601(token);

        // Colonne dati
        std::getline(ss, record.sensor_id, ',');
        std::getline(ss, token, ',');
        record.temperature = parse_DOUBLE(token);
        std::getline(ss, token, ',');
        record.humidity = parse_DOUBLE(token);
    };

    auto source_op = Table_Source_Builder<SensorInput>(filepath, source_logic)
        .withName("Sensor_Source")
        .withHeader()
        //versione ordinata
        .withOrderedEventTime()
        //versione disordinata
        //due ore di delay dato che i rilevamenti sono ogni 15 minuti (8 rilevamenti di buffer)
        //.withWatermarkDelay(7200000000ULL)
        .build();

    //chiave di partizionamento del group_by
    auto group_key = [](const SensorInput& in) -> KeyRecord {
        return KeyRecord({in.sensor_id});
    };

    //logica di raggruppamento e aggregazione
    auto group_logic = [](const SensorInput& in, GroupOutput& out) -> void {
        //chiave
        out.sensor_id = in.sensor_id;

        //count e sum per calcolare la media
        out.count += 1;
        out.sum += in.temperature;
        out.temp_avg = out.sum / out.count; 
    };

    //Windowed - Keyed
    auto group_op = Windowed_Group_Builder<SensorInput, GroupOutput, KeyRecord>(group_logic)
        .withName("group_by")
        .withParallelism(3)
        //finestra di un'ora ovvero 4 rilevamenti
        .withTBWindow(3600000000ULL)  
        .withKeyBy(group_key)
        .build_keyed();  

    //logica di stampa ad output
    auto sink_logic = [](const GroupOutput& record, std::ostream& os) {
        os << record.sensor_id << ","
        << record.temp_avg;
    };   

    //sink
    auto sink_op = Table_Sink_Builder<GroupOutput>("sensor_output.csv", sink_logic)
        .withName("Sensor_Sink")
        .withHeader("sensor_id, temp_avg")
        .withParallelism(1)
        .build();

    //topologia
    topology.add_source(source_op)
            .add(group_op)
            .add_sink(sink_op);
            
    topology.run();
    return 0;     
}
