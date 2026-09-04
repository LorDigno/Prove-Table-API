#include <iostream>
#include <string>
#include <vector>
#include <optional>
#include <fstream>
#include <sstream>
#include <chrono>
#include <windflow.hpp>
#include <windflow_table_api.hpp>
#include "avg_cold_temperature_structs.hpp"

int main(int argc, char* argv[]) {
    //-----     OPERATOR BUILDERS   -----

    auto from_1_op = Table_Source_Builder<source_cold_and_dry_from_6>( "sensor_input_stream.csv",
    [](const std::string& line, source_cold_and_dry_from_6& record, uint64_t& timestamp) {
    std::stringstream ss(line);
    std::string token;

    //timestamp
    std::getline(ss, token, ',');
    timestamp = parse_TIMESTAMP_ISO8601(token);
    //dati
    std::getline(ss, record.sensor_id, ',');
    std::getline(ss, token, ',');
    record.temperature = parse_DOUBLE(token);
    std::getline(ss, token, ',');
    record.humidity = parse_DOUBLE(token);
}
)
    .withName("cold_and_dry_from_6")
    .withHeader()
    .withOrderedEventTime()
    .build();

    auto where_2_op = Where_Builder<source_cold_and_dry_from_6>(
        [](const source_cold_and_dry_from_6& in) -> bool {
    return ((in.temperature < 10) && (in.humidity < 20));
}
    )
    .withName("cold_and_dry_where_5")
    .withParallelism(2)
    .build();

    auto distinct_3_op = Distinct_Builder<source_cold_and_dry_from_6, source_cold_and_dry_from_6>()
    .withName("cold_and_dry_distinct_4")
    .withParallelism(2)     
    .withKeyBy([](const source_cold_and_dry_from_6& in) -> source_cold_and_dry_from_6{ return in; })
    .build_keyed();


    auto select_4_op = Select_Builder<source_cold_and_dry_from_6, source_cold_and_dry_from_6>(
        [](const source_cold_and_dry_from_6& in) -> source_cold_and_dry_from_6 {
    source_cold_and_dry_from_6 out;
    out.sensor_id = in.sensor_id;
    out.temperature = in.temperature;
    out.humidity = in.humidity;
    return out;
}
    )
    .withName("cold_and_dry_select_3")
    .withParallelism(2)
    .build();

    auto group_5_op = Global_Group_Builder<source_cold_and_dry_from_6, avg_cold_temperature_group_by_2_struct_out, avg_cold_temperature_group_by_2_key_struct>(
    [](const source_cold_and_dry_from_6& in, avg_cold_temperature_group_by_2_struct_out& out) -> void {
    out.sensor_id = in.sensor_id;

    out.COUNT_ += 1;
    out.SUM_temperature += in.temperature;
    out.AVG_temperature = out.SUM_temperature / out.COUNT_ ;
}
)
    .withName("avg_cold_temperature_group_by_2")
    .withParallelism(2)
    .withKeyBy([](const source_cold_and_dry_from_6& in) -> avg_cold_temperature_group_by_2_key_struct {
    avg_cold_temperature_group_by_2_key_struct out;
    out.sensor_id = in.sensor_id;
    return out;
})
    .build_keyed();


    auto select_6_op = Select_Builder<avg_cold_temperature_group_by_2_struct_out, avg_cold_temperature_select_1_struct_out>(
        [](const avg_cold_temperature_group_by_2_struct_out& in) -> avg_cold_temperature_select_1_struct_out {
    avg_cold_temperature_select_1_struct_out out;
    out.sensor_id = in.sensor_id;
    out.avg_temp = in.AVG_temperature;
    return out;
}
    )
    .withName("avg_cold_temperature_select_1")
    .withParallelism(2)
    .build();

    auto sink_7_op = Table_Sink_Builder<avg_cold_temperature_select_1_struct_out>("avg_cold_temperature_output.csv",
    [](const avg_cold_temperature_select_1_struct_out& record, std::ostream& os) {
    os << record.sensor_id << ",";
    os << record.avg_temp;
}
)
    .withName("avg_cold_temperature_sink")
    .withHeader("sensor_id, avg_temp")
    .withParallelism(1)
    .build();

    //-----     PIPES AND TOPOLOGY  ------
    wf::PipeGraph topology(
        "avg_cold_temperature", 
        wf::Execution_Mode_t::DEFAULT
        , wf::Time_Policy_t::EVENT_TIME 
    );

    auto& pipe_0 = topology.add_source(from_1_op).add(where_2_op).add(distinct_3_op).add(select_4_op).add(group_5_op).add(select_6_op).add_sink(sink_7_op);

    topology.run();
    return 0;
}