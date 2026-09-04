#ifndef TABLE_SOURCE_BUILDER_HPP
#define TABLE_SOURCE_BUILDER_HPP

#include <string>
#include <sstream>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <windflow.hpp>

//funzioni helper per il parsing dei tipi elementari
inline std::string parse_STRING(const std::string& s) { return s; }
inline int32_t     parse_INT(const std::string& s)    { return std::stoi(s); }
inline int64_t     parse_BIGINT(const std::string& s) { return std::stoll(s); }
inline float       parse_FLOAT(const std::string& s)  { return std::stof(s); }
inline double      parse_DOUBLE(const std::string& s) { return std::stod(s); }
inline bool        parse_BOOLEAN(const std::string& s){ return s == "1" || s == "true" || s == "TRUE"; }

//funzioni helper per il parsing del formato temporale
// Converte 'YYYY-MM-DDTHH:MM:SS.mmmZ' in microsecondi lineari continui
inline uint64_t parse_TIMESTAMP_ISO8601(const std::string& s) {
    if (s.size() < 23) return 0;

    // 1. Parsing componenti data
    uint64_t year  = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
    uint64_t month = (s[5] - '0') * 10 + (s[6] - '0');
    uint64_t day   = (s[8] - '0') * 10 + (s[9] - '0');

    // 2. Parsing componenti orarie
    uint64_t hours = (s[11] - '0') * 10 + (s[12] - '0');
    uint64_t mins  = (s[14] - '0') * 10 + (s[15] - '0');
    uint64_t secs  = (s[17] - '0') * 10 + (s[18] - '0');
    uint64_t ms    = (s[20] - '0') * 100 + (s[21] - '0') * 10 + (s[22] - '0');

    // 3. Tabella giorni cumulativi pregressi per mese (anno non bisestile)
    static const uint32_t days_before_month[13] = {
        0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334
    };

    // 4. Calcolo giorni totali assoluti (conteggiando gli anni bisestili)
    uint64_t leap_years = (year - 1) / 4 - (year - 1) / 100 + (year - 1) / 400;
    uint64_t total_days = (year * 365ULL) + leap_years + days_before_month[month] + day;
    
    // Giorno bisestile per l'anno corrente dopo febbraio
    bool is_current_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (month > 2 && is_current_leap) {
        total_days++;
    }

    // 5. Conversione lineare continua in microsecondi
    uint64_t total_secs = total_days * 86400ULL + hours * 3600ULL + mins * 60ULL + secs;
    return (total_secs * 1000ULL + ms) * 1000ULL;
}

//funtore che implementa il ciclo di getline
template <typename TupleT>
class Source_Functor {
    //esegue il parsing della stringa e lo mette in TupleT, se c'è l'intero sarà il timestamp (microsecondi).
    using ParserFn = std::function<void(const std::string&, TupleT&, uint64_t&)>;

    private:
        std::string file_path;
        bool header_skip;

        //funzione che data una riga la parsa, passata come una lambda
        ParserFn parser_lambda;

        //flag per lo shipper
        bool event_time;

        //delay di watermarking
        uint64_t delay;
        
        //flag che sceglie il metodo di watermarking
        bool is_ordered;

        //necessari durante la computazione
        uint64_t max_ts = 0, first_time = 0 , last_wm = 0;
        bool is_first = true;
        bool first_wm = true;

        void process_ordered(uint64_t& current_ts, uint64_t& current_wm) {
            //normalizza secondo la prima tupla
            current_ts = (current_ts >= first_time) ? (current_ts - first_time) : 0;
            current_wm = current_ts;
        }

        void process_out_of_order(uint64_t& current_ts, uint64_t& current_wm) {
            //normalizza secondo il delay
            current_ts = (current_ts + delay >= first_time) 
                            ? (current_ts + delay - first_time) 
                            : 0;

            max_ts = std::max(max_ts, current_ts);
            current_wm = (max_ts >= delay) ? (max_ts - delay) : 0;
        }

    public:
        Source_Functor(const std::string& path, bool head, ParserFn pars, 
            bool ev, bool order, uint64_t del) 
            : file_path(path),
            header_skip(head),
            parser_lambda(pars),
            event_time(ev),
            is_ordered(order),
            delay(del) {}
    
        void operator()(wf::Source_Shipper<TupleT> &shipper) {
            std::ifstream file(file_path);
            if (!file.is_open()) {
                std::cerr << "[ERROR] Impossibile aprire il file: " << file_path << std::endl;
                return;
            }

            std::string line;
            if(header_skip){
                //salto la prima riga se c'è l'header
                std::getline(file, line);
            }

            //ciclo di lettura delle righe del file
            while (std::getline(file, line)) {
                if (line.empty() || line.front() == '\r') continue;

                uint64_t timestamp = 0, watermark = 0;
                TupleT tuple;

                parser_lambda(line, tuple, timestamp);

                if(event_time){
                    //mi salvo il primo per la normalizzazione dei timestamp
                    if(is_first){
                        is_first = false;
                        first_time = timestamp;
                    }
                    
                    //usa la politica corretta per la normalizzazione e il watermarking
                    if (is_ordered) {
                        process_ordered(timestamp, watermark);
                    } else {
                        process_out_of_order(timestamp, watermark);
                    }
  
                    shipper.pushWithTimestamp(tuple, timestamp);
                    if(watermark > last_wm || first_wm){
                        first_wm = false;
                        last_wm = watermark;
                        shipper.setNextWatermark(watermark);
                    }
                }
                else{
                    shipper.push(tuple);
                }
            }
        }
};

template<typename TupleT>
class Table_Source_Builder{
    //esegue il parsing della stringa e lo mette in TupleT, se c'è l'intero sarà il timestamp (microsecondi).
    using ParserFn = std::function<void(const std::string&, TupleT&, uint64_t&)>;

    private:
        //parser function
        ParserFn parser_lambda;

        //filepath da leggere
        std::string filepath;

        //se skippare l'header
        bool header = false;

        //attributi per la politica EVENT_TIME
        bool event_time = false;     
        uint64_t delay = 0;         
        bool ordered = false;    

        std::string op_name = "TableSource_Operator";

    public: 
        Table_Source_Builder(const std::string& path, ParserFn parser)
            :   parser_lambda(parser), filepath(path) {}

        Table_Source_Builder& withName(const std::string& name) {
            this->op_name = name;
            return *this;
        }

        //segna un EVENT TIME in cui il file di input è ordinato per timestamp
        Table_Source_Builder& withOrderedEventTime() {
            this->event_time = true;
            this->ordered = true;
            this->delay = 0;
            return *this;
        }

        //segna un EVENT TIME in cui si accetta del delay nei timestamp disordinati
        Table_Source_Builder& withWatermarkDelay(uint64_t del) {
            this->event_time = true;
            this->ordered = false;
            this->delay = del;
            return *this;
        }

        Table_Source_Builder& withHeader(){
            this->header = true;
            return *this;
        }

        auto build(){
            Source_Functor<TupleT> functor = Source_Functor<TupleT>(
                filepath,
                header,
                parser_lambda,
                event_time,
                ordered,
                delay
            );

            return wf::Source_Builder(functor)
                .withName(op_name)
                .build();
        }
};

#endif