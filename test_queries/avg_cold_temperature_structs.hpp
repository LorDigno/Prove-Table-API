#ifndef AVG_COLD_TEMPERATURE_STRUCTS_HPP
#define AVG_COLD_TEMPERATURE_STRUCTS_HPP

#include <string>
#include <cstdint>
#include <functional>
#include <limits>

// ============================================================================
// Struct: source_cold_and_dry_from_6
// ============================================================================
struct source_cold_and_dry_from_6 {
    std::string sensor_id;
    double temperature;
    double humidity;


    bool operator==(const source_cold_and_dry_from_6& other) const {
        return sensor_id == other.sensor_id && temperature == other.temperature && humidity == other.humidity;
    }
};

namespace std {
    template<>
    struct hash<source_cold_and_dry_from_6> {
        size_t operator()(const source_cold_and_dry_from_6& k) const {
            size_t h = 0;
            h ^= std::hash<std::string>{}(k.sensor_id) + 0x9e3779b9 + (h << 6) + (h >> 2);
            h ^= std::hash<double>{}(k.temperature) + 0x9e3779b9 + (h << 6) + (h >> 2);
            h ^= std::hash<double>{}(k.humidity) + 0x9e3779b9 + (h << 6) + (h >> 2);
            return h;
        }
    };
}

// ============================================================================
// Struct: avg_cold_temperature_group_by_2_key_struct
// ============================================================================
struct avg_cold_temperature_group_by_2_key_struct {
    std::string sensor_id;


    bool operator==(const avg_cold_temperature_group_by_2_key_struct& other) const {
        return sensor_id == other.sensor_id;
    }
};

namespace std {
    template<>
    struct hash<avg_cold_temperature_group_by_2_key_struct> {
        size_t operator()(const avg_cold_temperature_group_by_2_key_struct& k) const {
            size_t h = 0;
            h ^= std::hash<std::string>{}(k.sensor_id) + 0x9e3779b9 + (h << 6) + (h >> 2);
            return h;
        }
    };
}

// ============================================================================
// Struct: avg_cold_temperature_group_by_2_struct_out
// ============================================================================
struct avg_cold_temperature_group_by_2_struct_out {
    std::string sensor_id;
    int64_t COUNT_ = 0; 
    double SUM_temperature = 0.0; 
    double AVG_temperature = 0.0; 


};


// ============================================================================
// Struct: avg_cold_temperature_select_1_struct_out
// ============================================================================
struct avg_cold_temperature_select_1_struct_out {
    std::string sensor_id;
    double avg_temp;


};


#endif // AVG_COLD_TEMPERATURE_STRUCTS_HPP