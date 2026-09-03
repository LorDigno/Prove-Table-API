#ifndef TABLE_SOURCE_BUILDER_HPP
#define TABLE_SOURCE_BUILDER_HPP

#include <string>
#include <sstream>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <windflow.hpp>

//funzioni helper per il parsing dei tipi elementari
inline std::string parse_STRING(const std::string& s) { return s; }
inline int32_t     parse_INT(const std::string& s)    { return std::stoi(s); }
inline int64_t     parse_BIGINT(const std::string& s) { return std::stoll(s); }
inline float       parse_FLOAT(const std::string& s)  { return std::stof(s); }
inline double      parse_DOUBLE(const std::string& s) { return std::stod(s); }
inline bool        parse_BOOLEAN(const std::string& s){ return s == "1" || s == "true" || s == "TRUE"; }

//funtore che implementa il ciclo di getline
template <typename TupleT>
class Source_Functor {
    //esegue il parsing della stringa e lo mette in TupleT, se c'è l'intero sarà il timestamp (microsecondi).
    using ParserFn = std::function<void(const std::string&, TupleT&, uint64_t&)>;

    private:
        std::string file_path;

        //funzione che data una riga la parsa, passata come una lambda
        ParserFn parser_lambda;

        //flag per gli shipper
        bool has_timestamp;
        bool has_watermark;

        //delay di watermarking
        uint64_t delay; 

    public:
        Source_Functor(const std::string& path, ParserFn pars, bool timestamp, bool watermark, uint64_t del) 
            : file_path(path),
            parser_lambda(pars),
            has_timestamp(timestamp),
            has_watermark(watermark),
            delay(del) {}
    
        void operator()(wf::Source_Shipper<TupleT> &shipper) {
            std::ifstream file(file_path);
            if (!file.is_open()) {
                std::cerr << "[ERROR] Impossibile aprire il file: " << file_path << std::endl;
                return;
            }

            //ciclo di lettura delle righe del file
            std::string line;
            while (std::getline(file, line)) {
                if (line.empty() || line.front() == '\r') continue;

                uint64_t timestamp = 0;
                TupleT tuple;

                parser_lambda(line, tuple, timestamp);

                if( has_timestamp){
                    shipper.pushWithTimestamp(tuple, timestamp);
                }
                else{
                    shipper.push(tuple);
                } 

                if(has_watermark){
                    uint64_t wm = (timestamp >= delay) ? (timestamp - delay) : 0;
                    shipper.setNextWatermark(wm);
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
        std::string& filepath;

        //flag sulle politiche temporali
        bool has_timestamp = false;     //INGRESS_TIME ed EVENT_TIME
        bool has_watermark = false;     //EVENT_TIME
        uint64_t delay = 0;             //delay per il watermark in EVENT_TIME

        std::string op_name = "TableSource_Operator";

    public: 
        Table_Source_Builder(std::string& path, ParserFn parser)
            :   parser_lambda(parser), filepath(path) {}

        Table_Source_Builder& withName(const std::string& name) {
            this->op_name = name;
            return *this;
        }

        Table_Source_Builder& withTimestamp(){
            this->has_timestamp = true;
            return *this;
        }

        Table_Source_Builder& withWatermarkDelay(uint64_t delay){
            this->has_watermark = true;
            this->delay = delay;
            return *this;
        }

        auto build(){
            Source_Functor<TupleT> functor = Source_Functor<TupleT>(
                filepath,
                parser_lambda,
                has_timestamp,
                has_watermark,
                delay
            );

            return wf::Source_Builder(functor)
                .withName(op_name)
                .build();
        }
};

#endif