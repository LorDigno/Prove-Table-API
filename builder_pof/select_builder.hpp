#ifndef SELECT_BUILDER_HPP
#define SELECT_BUILDER_HPP

#include <string>
#include <functional>
#include <windflow.hpp>

//funtore che applica la logica della select
template <typename InputT, typename OutputT>    
class Select_Functor {
private:
    std::function<OutputT(const InputT&)> filt_func;

public:
    //costruttore che associa la lambda
    Select_Functor(std::function<OutputT(const InputT&)> func) 
        : filt_func(func) {}   

    //metodo chiamato dalla map    
    OutputT operator()(const InputT& input) const {
        //la trasformazione è racchiusa nella lambda.
        //il builder non fa altro che impacchettare tutto in modoo che sia coerente con WF.
        return filt_func(input);
    }
};

//vero e proprio builder che gestisce i parametri e istanzia la Map
template <typename InputT, typename OutputT>
class Select_Builder {
private:
    std::function<OutputT(const InputT&)> proj_func;
    std::string op_name = "Select_Operator";
    size_t parallelism = 1;

public:
    //associa la lambda
    explicit Select_Builder(std::function<OutputT(const InputT&)> func)
        : proj_func(func) {}

    //da un nome all'operatore di Map    
    Select_Builder& withName(const std::string& name) {
        this->op_name = name;
        return *this;
    }

    //stabilisce il parallelismo per l'operatore di Map
    Select_Builder& withParallelism(size_t par) {
        this->parallelism = par;
        return *this;
    }

    //istanzia, con tutti i parametri dati, sul Select_Functor e rende la Map nativa 
    auto build() {
        Select_Functor<InputT, OutputT> functor(proj_func);
        return wf::Map_Builder(functor)
            .withName(op_name)
            .withParallelism(parallelism)
            .build();
    }
};

#endif