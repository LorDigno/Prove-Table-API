#ifndef TABLE_SINK_BUILDER_HPP
#define TABLE_SINK_BUILDER_HPP

#include <string>
#include <fstream>
#include <iostream>
#include <memory>
#include <functional>
#include <optional>
#include <windflow.hpp>

//funtore per scrittura di flusso su file csv
template <typename TupleT>
class Sink_Functor {
    public:
        //riceve la tupla e scrive direttamente sullo stream
        using FormatterFn = std::function<void(const TupleT&, std::ostream&)>;

    private:
        std::string file_path;
        FormatterFn formatter_lambda;
        std::string header;
        std::shared_ptr<std::ofstream> out_file;

    public:
        Sink_Functor(
            const std::string& path,
            FormatterFn formatter,
            const std::string& csv_header = ""
        ) : file_path(path),
            formatter_lambda(formatter),
            header(csv_header),
            out_file(std::make_shared<std::ofstream>(path)) {
            
            if (!out_file->is_open()) {
                std::cerr << "[ERROR] Impossibile creare/aprire il file sink: " << file_path << std::endl;
            } else if (!header.empty()) {
                *out_file << header << "\n";
            }
        }

        void operator()(std::optional<TupleT>& input) {
            //fine dello stream
            if (!input) {
                if (out_file && out_file->is_open()) {
                    out_file->flush();
                    out_file->close();
                }
                std::cout << "[SINK] File " << file_path << " completato e chiuso." << std::endl;
                return;
            }

            //scrittura sul file
            if (out_file && out_file->is_open()) {
                formatter_lambda(*input, *out_file);
                *out_file << "\n";
            }
        }
};

template <typename TupleT>
class Table_Sink_Builder {
    public:
        using FormatterFn = std::function<void(const TupleT&, std::ostream&)>;

    private:
        std::string filepath;
        FormatterFn formatter_lambda;
        std::string header = "";
        std::string op_name = "TableSink_Operator";
        size_t parallelism = 1;

    public:
        Table_Sink_Builder(const std::string& path, FormatterFn formatter)
            : filepath(path), formatter_lambda(formatter) {}

        Table_Sink_Builder& withName(const std::string& name) {
            this->op_name = name;
            return *this;
        }

        Table_Sink_Builder& withHeader(const std::string& csv_header) {
            this->header = csv_header;
            return *this;
        }

        Table_Sink_Builder& withParallelism(size_t par) {
            this->parallelism = par;
            return *this;
        }

        auto build() {
            Sink_Functor<TupleT> functor(filepath, formatter_lambda, header);

            return wf::Sink_Builder(functor)
                .withName(op_name)
                .withParallelism(parallelism)
                .build();
        }
};

#endif // TABLE_SINK_BUILDER_HPP