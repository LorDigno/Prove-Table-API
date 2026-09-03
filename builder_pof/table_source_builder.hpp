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
        bool header_skip;

        //funzione che data una riga la parsa, passata come una lambda
        ParserFn parser_lambda;

        //flag per lo shipper
        bool event_time;

        //delay di watermarking
        uint64_t delay; 

    public:
        Source_Functor(const std::string& path, bool head, ParserFn pars, bool ev, uint64_t del) 
            : file_path(path),
            header_skip(head),
            parser_lambda(pars),
            event_time(ev),
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

                uint64_t timestamp = 0;
                TupleT tuple;

                parser_lambda(line, tuple, timestamp);

                if(event_time){
                    uint64_t wm = (timestamp >= delay) ? (timestamp - delay) : 0;
                    shipper.pushWithTimestamp(tuple, timestamp);
                    shipper.setNextWatermark(wm);
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
        std::string& filepath;

        //se skippare l'header
        bool header = false;

        //attributi per la politica EVENT_TIME
        bool event_time = false;     
        uint64_t delay = 0;             

        std::string op_name = "TableSource_Operator";

    public: 
        Table_Source_Builder(std::string& path, ParserFn parser)
            :   parser_lambda(parser), filepath(path) {}

        Table_Source_Builder& withName(const std::string& name) {
            this->op_name = name;
            return *this;
        }

        Table_Source_Builder& withWatermarkDelay(uint64_t delay){
            this->event_time = true;
            this->delay = delay;
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
                delay
            );

            return wf::Source_Builder(functor)
                .withName(op_name)
                .build();
        }
};

#endif